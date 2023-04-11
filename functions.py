import requests
import base64
import config
import copy
import uuid
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
import json

auth_string = f"{config.SHIPSTATION_API_KEY}:{config.SHIPSTATION_API_SECRET}"
encoded_auth_string = base64.b64encode(auth_string.encode('utf-8')).decode('utf-8')

headers = {
    "Content-Type": "application/json",
    "Authorization": f"Basic {encoded_auth_string}"
}

session = requests.Session()
session.headers.update(headers)

failed = []
def processor(order):
    mlp_data = {}

    has_lawn_plan = any(isLawnPlan(item["sku"]) for item in order["items"])
    if has_lawn_plan:
        print(f"Order {order['orderNumber']} has a lawn plan")
        url_mlp = f"https://user-api-dev-qhw6i22s2q-uc.a.run.app/order?shopify_order_no={order['orderNumber']}"
        response_mlp = session.get(url_mlp)
        data_mlp = response_mlp.json()
        print(data_mlp) 
        plan_details = data_mlp.get("plan_details", [])
        for detail in plan_details:
            product_list = []
            for product in detail['products']:
                product_list.append({
                    'name': product['name'],
                    'count': product['count']
                })
            mlp_data[detail['sku']] = product_list

    process_order(order, mlp_data)

def extract_data_from_resource_url(event):
    payload = json.loads(event["body"])
    resource_url = payload['resource_url']
    print(f"Fetching data from resource url: {resource_url}")
    response = session.get(resource_url)
    data = response.json()
    orders = data['orders']
    return orders

def isLawnPlan(sku):
    return (sku.startswith('SUB') or sku in ['05000', '10000', '15000']) and sku not in ["SUB - LG - D", "SUB - LG - S", "SUB - LG - G"]

def process_item(item, mlp_data):
    print("Processing individual item")
    original_sku = item["sku"]
    if original_sku in config.SKU_REPLACEMENTS:
        if isLawnPlan(original_sku) and original_sku in mlp_data:
            products_info = mlp_data[original_sku]
            item['name'] = config.SKU_REPLACEMENTS[original_sku]
            for product_info in products_info:
                item['name'] += f"\n\u00A0\u00A0\u00A0\u00A0• {product_info['count']} {product_info['name']}"
        else:
            replacement_name = config.SKU_REPLACEMENTS[original_sku]
            item["name"] = replacement_name

def order_split_required(order):
    total_pouches = sum([item['quantity'] * config.sku_to_pouches.get(item['sku'], 0) for item in order['items']])
    return total_pouches > 9

def apply_preset_based_on_pouches(order, mlp_data, use_gnome_preset=False):
    processed_items = []
    total_pouches = 0
    special_items = {'OTP - STK': 0, 'OTP - HES': 0}
    total_special_items = 0

    for item in order['items']:
        process_item(item, mlp_data)
        processed_items.append(item)
        if item['sku'] in special_items:
            special_items[item['sku']] += item['quantity']
            total_special_items += item['quantity']
        else:
            total_pouches += item['quantity'] * config.sku_to_pouches.get(item['sku'], 0)

    order['items'] = processed_items

    preset_dict = config.presets_with_gnome if use_gnome_preset else config.presets

    if special_items['OTP - STK'] == 1 and total_pouches == 0 and total_special_items == 1:
        preset_key = 'STK'
        order['weight'] = {"value": 4, "units": "ounces", "WeightUnits": 1}
    elif special_items['OTP - HES'] > 0 and total_pouches == 0 and total_special_items == special_items['OTP - HES']:
        hes_base_weight = presets['HES']['base_weights'][special_items['OTP - HES']]
        presets['HES']['weight'] = {"value": hes_base_weight, "units": "ounces", "WeightUnits": 1}
        preset_key = 'HES'
    else:
        preset_key = str(total_pouches)

    preset = preset_dict[preset_key]

    updated_order = order.copy()
    if preset_key != 'STK':
        updated_order['weight'] = preset['weight']
    updated_order.update(preset)

    if 'advancedOptions' in order and 'advancedOptions' in preset:
        updated_advanced_options = {**order['advancedOptions'], **preset['advancedOptions']}
    elif 'advancedOptions' in order:
        updated_advanced_options = order['advancedOptions']
    else:
        updated_advanced_options = preset['advancedOptions']

    updated_order['advancedOptions'] = updated_advanced_options

    return updated_order

def set_stk_order_tag(order):
    has_stk_item = any(item['sku'] in ('OTP - STK', 'OTP - LYL') for item in order['items'])
    has_stk_tag = "STK-Order" in order['advancedOptions'].get('customField1', '')

    if has_stk_item and not has_stk_tag:
        if order['advancedOptions'].get('customField1') is None:
            order['advancedOptions']['customField1'] = ""
            order['advancedOptions']['customField1'] += "STK-Order"
        else:
            order['advancedOptions']['customField1'] = "STK-Order, " + order['advancedOptions']['customField1']
    elif not has_stk_item and has_stk_tag:
        tags = order['advancedOptions']['customField1'].split(', ')
        tags.remove("STK-Order")
        order['advancedOptions']['customField1'] = ', '.join(tags)

    return order

def should_add_gnome_to_parent_order(parent_order):
    custom_field1 = parent_order['advancedOptions'].get('customField1', "")
    return isinstance(custom_field1, str) and "First" in custom_field1 and any(isLawnPlan(item["sku"]) for item in parent_order["items"])


def process_order(order, mlp_data):
    need_stk_tag = any(item['sku'] == 'OTP - STK' for item in order['items'])

    need_gnome = should_add_gnome_to_parent_order(order)

    if order_split_required(order):
        # Prepare the child orders and parent order
        original_order, child_orders = prepare_split_data(order, need_stk_tag, mlp_data, need_gnome)

        print(f"Child orders check 2: {child_orders}")
        print(f"Parent order check 2: {original_order}")

        child_responses = []
        for index, child_order in enumerate(child_orders):
            response = session.post('https://ssapi.shipstation.com/orders/createorder', data=json.dumps(child_order))
            child_responses.append(response)


        # Update the parent order in ShipStation
        response2 = session.post('https://ssapi.shipstation.com/orders/createorder', data=json.dumps(original_order))

        if response2.status_code == 200:
            print(f"Parent order #{original_order['orderNumber']} created successfully")
            print(f"Full success response: {response2.__dict__}")
        else:
            failed.append(order['orderNumber'])
            print(f"Unexpected status code for parent order #{original_order['orderNumber']}: {response2.status_code}")
            print(f"Full error response: {response2.__dict__}")

        return f"Successfully processed order #{order['orderNumber']}"

    else:
        if need_gnome:
            gnome_item = config.gnome
            order['items'].append(gnome_item)
            order = apply_preset_based_on_pouches(order, mlp_data, use_gnome_preset=True)
        else:
            order = apply_preset_based_on_pouches(order, mlp_data, use_gnome_preset=False)
        order = set_stk_order_tag(order)
        response = session.post('https://ssapi.shipstation.com/orders/createorder', data=json.dumps(order))

        if response.status_code == 200:
            print(f"Order #{order['orderNumber']} updated successfully with preset")
            print(f"Full success response: {response.__dict__}")
        else:
            failed.append(order['orderNumber'])
            print(f"Unexpected status code for order #{order['orderNumber']}: {response2.status_code}")
            print(f"Full error response: {response.__dict__}")

        return f"Successfully processed order #{order['orderNumber']} without splitting"


def prepare_split_data(order, need_stk_tag, mlp_data, need_gnome):
    original_order = copy.deepcopy(order)  # Create a deep copy of the order object
    child_orders = []

    total_pouches = sum([item['quantity'] * config.sku_to_pouches.get(item['sku'], 0) for item in original_order['items']])
    
    shipment_counter = 1
    
    while total_pouches > 9:
        child_order_items = []
        child_pouches = 0

        for item in original_order['items']:
            pouches_per_item = config.sku_to_pouches.get(item['sku'], 0)
            temp_quantity = 0

            while item['quantity'] > 0 and (child_pouches + pouches_per_item) <= 9:
                item['quantity'] -= 1
                child_pouches += pouches_per_item
                temp_quantity += 1
                total_pouches -= pouches_per_item  # Update total_pouches

            if temp_quantity > 0:
                item_copy = copy.deepcopy(item)
                item_copy['quantity'] = temp_quantity
                child_order_items.append(item_copy)

        child_order = prepare_child_order(original_order, child_order_items, mlp_data)
        child_orders.append(child_order)
        shipment_counter += 1

    total_shipments = shipment_counter

    remaining_pouches_total = sum([item['quantity'] * config.sku_to_pouches.get(item['sku'], 0) for item in original_order['items']])

    if need_gnome:
        gnome = config.gnome
        original_order['items'].append(gnome)
        original_order = apply_preset_based_on_pouches(original_order, mlp_data, use_gnome_preset=True)
    else:
        original_order = apply_preset_based_on_pouches(original_order, mlp_data)

    original_order['items'] = [item for item in original_order['items'] if item['quantity'] > 0]
    original_order['orderNumber'] = f"{original_order['orderNumber']}-1"
    original_order['advancedOptions']['customField2'] = f"Shipment 1 of {total_shipments}"

    for i in range(len(child_orders)):
        child_order = copy.deepcopy(child_orders[i])
        child_order['orderNumber'] = f"{order['orderNumber']}-{i + 2}"
        child_order['advancedOptions']['customField2'] = f"Shipment {i + 2} of {total_shipments}"
        if need_stk_tag:
            child_order = set_stk_order_tag(child_order)
            need_stk_tag = False
        child_orders[i] = child_order

    if need_stk_tag:
        original_order = set_stk_order_tag(original_order)
        need_stk_tag = False

    print(f"Parent order: {original_order}")
    print(f"Child_orders: {child_orders}")
    return original_order, child_orders


def prepare_child_order(parent_order, child_order_items, mlp_data):
    child_order = copy.deepcopy(parent_order)
    if 'orderId' in child_order:
        del child_order['orderId']
    child_order['items'] = child_order_items
    child_order['orderTotal'] = 0.00
    child_order['orderKey'] = str(uuid.uuid4())

    child_order = apply_preset_based_on_pouches(child_order, mlp_data)

    child_order['advancedOptions']['billToParty'] = "my_other_account"
    parent_order['advancedOptions']['billToParty'] = "my_other_account"

    return child_order



def test_order():
    order_data = {"orderNumber":"100724","orderKey":"f3d72157-a212-5e31-20a2-9375205e2566","orderDate":"2023-04-09T04:59:18.0000000","createDate":"2023-04-09T05:00:42.3200000","modifyDate":"2023-04-09T05:00:43.6130000","paymentDate":"2023-04-09T04:59:18.0000000","shipByDate":None,"orderStatus":"awaiting_shipment","customerId":115007741,"customerUsername":"test@testing.com","customerEmail":"test@testing.com","billTo":{"name":"Test Order Do Not Print","company":None,"street1":"","street2":None,"street3":None,"city":"","state":None,"postalCode":"","country":" ","phone":"","residential":None,"addressVerified":None},"shipTo":{"name":"Test Order Do Not Print","company":None,"street1":"1234 Fake Ave","street2":"","street3":None,"city":"Fakesville","state":"CA","postalCode":"90291-6410","country":"US","phone":"","residential":False,"addressVerified":"Address validation warning"},"items":[{"orderItemId":352756346,"lineItemKey":None,"sku":"OTP - LCP","name":"Spring Lawn Fertilizer Kit: Liquid Lawn Food and Iron Bundle","imageUrl":None,"weight":None,"quantity":5,"unitPrice":0.00,"taxAmount":None,"shippingAmount":None,"warehouseLocation":None,"options":[],"productId":23065056,"fulfillmentSku":None,"adjustment":False,"upc":None,"createDate":"2023-04-09T05:00:42.373","modifyDate":"2023-04-09T05:00:42.373"},{"orderItemId":352756347,"lineItemKey":None,"sku":"SUB - LG - S","name":"Lawn Guard - Standard","imageUrl":None,"weight":None,"quantity":1,"unitPrice":0.00,"taxAmount":None,"shippingAmount":None,"warehouseLocation":None,"options":[],"productId":23064771,"fulfillmentSku":None,"adjustment":False,"upc":None,"createDate":"2023-04-09T05:00:42.373","modifyDate":"2023-04-09T05:00:42.373"},{"orderItemId":352756348,"lineItemKey":None,"sku":"OTP - WNF","name":"Spring Lawn Weed & Feed Kit","imageUrl":None,"weight":None,"quantity":3,"unitPrice":0.00,"taxAmount":None,"shippingAmount":None,"warehouseLocation":None,"options":[],"productId":23064761,"fulfillmentSku":None,"adjustment":False,"upc":None,"createDate":"2023-04-09T05:00:42.373","modifyDate":"2023-04-09T05:00:42.373"},{"orderItemId":352756349,"lineItemKey":None,"sku":"SUB - MLPA - S","name":"Magic Lawn Plan Annual - Standard","imageUrl":None,"weight":None,"quantity":4,"unitPrice":0.00,"taxAmount":None,"shippingAmount":None,"warehouseLocation":None,"options":[],"productId":23064928,"fulfillmentSku":None,"adjustment":False,"upc":None,"createDate":"2023-04-09T05:00:42.373","modifyDate":"2023-04-09T05:00:42.373"},{"orderItemId":352756350,"lineItemKey":None,"sku":"SUB - SFLP - S","name":"South Florida Lawn Care Plans - Small Florida Yard","imageUrl":None,"weight":None,"quantity":11,"unitPrice":0.00,"taxAmount":None,"shippingAmount":None,"warehouseLocation":None,"options":[],"productId":23065022,"fulfillmentSku":None,"adjustment":False,"upc":None,"createDate":"2023-04-09T05:00:42.373","modifyDate":"2023-04-09T05:00:42.373"},{"orderItemId":352756351,"lineItemKey":None,"sku":"OTP - STK","name":"Soil Test Kit","imageUrl":None,"weight":None,"quantity":1,"unitPrice":0.00,"taxAmount":None,"shippingAmount":None,"warehouseLocation":None,"options":[],"productId":23066068,"fulfillmentSku":None,"adjustment":False,"upc":None,"createDate":"2023-04-09T05:00:42.373","modifyDate":"2023-04-09T05:00:42.373"},{"orderItemId":352756352,"lineItemKey":None,"sku":"OTP - GG","name":"Green Glow: Concentrated Liquid Nitrogen Fertilizer","imageUrl":None,"weight":None,"quantity":8,"unitPrice":0.00,"taxAmount":None,"shippingAmount":None,"warehouseLocation":None,"options":[],"productId":23065689,"fulfillmentSku":None,"adjustment":False,"upc":None,"createDate":"2023-04-09T05:00:42.373","modifyDate":"2023-04-09T05:00:42.373"},{"orderItemId":352756353,"lineItemKey":None,"sku":"OTP - SB","name":"Soil Balance: Lawn Starter for New and High-Traffic Lawns","imageUrl":None,"weight":None,"quantity":7,"unitPrice":0.00,"taxAmount":None,"shippingAmount":None,"warehouseLocation":None,"options":[],"productId":23065663,"fulfillmentSku":None,"adjustment":False,"upc":None,"createDate":"2023-04-09T05:00:42.373","modifyDate":"2023-04-09T05:00:42.373"}],"orderTotal":0.00,"amountPaid":0.00,"taxAmount":0.00,"shippingAmount":0.00,"customerNotes":None,"internalNotes":None,"gift":False,"giftMessage":None,"paymentMethod":None,"requestedShippingService":None,"carrierCode":None,"serviceCode":None,"packageCode":None,"confirmation":"none","shipDate":None,"holdUntilDate":None,"weight":{"value":0.00,"units":"ounces","WeightUnits":1},"dimensions":None,"insuranceOptions":{"provider":None,"insureShipment":False,"insuredValue":0.0},"internationalOptions":{"contents":None,"customsItems":None,"nonDelivery":None},"advancedOptions":{"warehouseId":242018,"nonMachinable":False,"saturdayDelivery":False,"containsAlcohol":False,"mergedOrSplit":False,"mergedIds":[],"parentId":None,"storeId":310067,"customField1":"Subscription First Order","customField2":None,"customField3":None,"source":None,"billToParty":None,"billToAccount":None,"billToPostalCode":None,"billToCountryCode":None,"billToMyOtherAccount":None},"tagIds":None,"userId":None,"externallyFulfilled":False,"externallyFulfilledBy":None,"externallyFulfilledById":None,"externallyFulfilledByName":None,"labelMessages":None}
    order_data['orderKey'] = str(uuid.uuid4())
    order_data['orderNumber'] = str(random.randrange(1000000, 9999999))
    response = session.post('https://ssapi.shipstation.com/orders/createorder', json=order_data)
    orders = session.get(f"https://ssapi.shipstation.com/orders?orderNumber={order_data['orderNumber']}").json()['orders']
    return orders[0]