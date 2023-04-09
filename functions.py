import requests
import base64
import config

auth_string = f"{config.SHIPSTATION_API_KEY}:{config.SHIPSTATION_API_SECRET}"
encoded_auth_string = base64.b64encode(auth_string.encode('utf-8')).decode('utf-8')

headers = {
    "Content-Type": "application/json",
    "Authorization": f"Basic {encoded_auth_string}"
}

session = requests.Session()
session.headers.update(headers)


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

def apply_preset_based_on_pouches(order, mlp_data):
    processed_items = []
    total_pouches = 0

    for item in order['items']:
        process_item(item, mlp_data)
        processed_items.append(item)
        total_pouches += item['quantity'] * config.sku_to_pouches.get(item['sku'], 0)

    order['items'] = processed_items
    preset = config.presets[str(total_pouches)]
    order['weight'] = preset['weight']
    order.update(preset)
    order['advancedOptions'].update(preset['advancedOptions'])  # Update advancedOptions separately

    return order


def set_stk_order_tag(order, need_stk_tag):
    if need_stk_tag:
        if 'customField1' not in order['advancedOptions']:
            order['advancedOptions']['customField1'] = ""

        if len(order['advancedOptions']['customField1']) == 0:
            order['advancedOptions']['customField1'] = "STK-Order"
        else:
            order['advancedOptions']['customField1'] = "STK-Order, " + order['advancedOptions']['customField1']
    return order


def process_order(order, mlp_data):
    need_stk_tag = any(item['sku'] == 'OTP - STK' for item in order['items'])

    if order_split_required(order):
        # Prepare the child orders and parent order
        original_order, child_orders = prepare_split_data(order, need_stk_tag, mlp_data)

        with ThreadPoolExecutor() as executor:
            # Update all child orders in a single request
            future1 = executor.submit(session.post, 'https://ssapi.shipstation.com/orders/createorders', data=json.dumps(child_orders))

            # Update the parent order in ShipStation
            future2 = executor.submit(session.post, 'https://ssapi.shipstation.com/orders/createorder', data=json.dumps(original_order))

            response1 = future1.result()
            response2 = future2.result()

        if response1.status_code == 200:
            print(f"Successfully created {len(child_orders)} children")
            print(f"Full success response: {response1.__dict__}")
        else:
            print(f"Unexpected status code for child orders: {response1.status_code}")
            print(f"Full error response: {response1.__dict__}")

        if response2.status_code == 200:
            print(f"Parent order created successfully")
            print(f"Full success response: {response2.__dict__}")
        else:
            print(f"Unexpected status code for parent order: {response2.status_code}")
            print(f"Full error response: {response2.__dict__}")

        return f"Successfully processed order #{order['orderNumber']}"

    else:
        order = apply_preset_based_on_pouches(order, mlp_data)
        order = set_stk_order_tag(order, need_stk_tag)
        response = session.post('https://ssapi.shipstation.com/orders/createorder', data=json.dumps(order))

        if response.status_code == 200:
            print(f"Order updated successfully with preset")
            print(f"Full success response: {response.__dict__}")
        else:
            print(f"Unexpected status code for updating order: {response.status_code}")
            print(f"Full error response: {response.__dict__}")

        return f"Successfully processed order #{order['orderNumber']} without splitting"


def prepare_split_data(order, need_stk_tag, mlp_data):
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

        child_order = prepare_child_order(original_order, child_order_items)
        child_orders.append(child_order)
        shipment_counter += 1

    total_shipments = shipment_counter

    remaining_pouches_total = sum([item['quantity'] * config.sku_to_pouches.get(item['sku'], 0) for item in original_order['items']])
    original_order = apply_preset_based_on_pouches(original_order, mlp_data)


    original_order['items'] = [item for item in original_order['items'] if item['quantity'] > 0]
    original_order['orderNumber'] = f"{original_order['orderNumber']}-1"
    original_order['advancedOptions']['customField2'] = f"Shipment 1 of {total_shipments}"

    for i in range(len(child_orders)):
        child_order = copy.deepcopy(child_orders[i])
        child_order['orderNumber'] = f"{order['orderNumber']}-{i + 2}"
        child_order['advancedOptions']['customField2'] = f"Shipment {i + 2} of {total_shipments}"
        if need_stk_tag:
            child_order = set_stk_order_tag(child_order, need_stk_tag)
            need_stk_tag = False
        child_orders[i] = child_order

    if need_stk_tag:
        original_order = set_stk_order_tag(original_order, need_stk_tag)
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
    child_order['paymentDate'] = None

    remaining_pouches_total = sum([item['quantity'] * config.sku_to_pouches.get(item['sku'], 0) for item in child_order_items])
    child_order = apply_preset_based_on_pouches(child_order, mlp_data)

    # Update advanced options to reflect the relationship between the parent and child orders
    child_order['advancedOptions']['mergedOrSplit'] = True
    child_order['advancedOptions']['parentId'] = parent_order['orderId']
    parent_order['advancedOptions']['mergedOrSplit'] = True
    parent_order['advancedOptions'].pop('parentId', None)  # Remove the parentId value for the parent order
    child_order['advancedOptions']['billToParty'] = "my_other_account"
    parent_order['advancedOptions']['billToParty'] = "my_other_account"

    return child_order