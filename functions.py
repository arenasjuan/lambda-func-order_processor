import requests
import base64
import config
import copy
import uuid
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import time

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
        order_number = order['orderNumber']
        print(f"Order {order_number} has a lawn plan")
        if "-" in order['orderNumber']:
            order_number = order['orderNumber'].split("-")[0]
        url_mlp = f"https://user-api-dev-qhw6i22s2q-uc.a.run.app/order?shopify_order_no={order_number}"
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


def should_add_gnome_to_parent_order(parent_order):
    custom_field1 = parent_order['advancedOptions'].get('customField1', "")
    return isinstance(custom_field1, str) and "First" in custom_field1 and any(isLawnPlan(item["sku"]) for item in parent_order["items"])

def append_tag_if_not_exists(tag, custom_field):
    if tag not in custom_field:
        custom_field += (", " if custom_field else "") + tag
    return custom_field

def set_order_tags(order, parent_order=None):
    if not order.get('advancedOptions'):
        order['advancedOptions'] = {}

    if order['advancedOptions'].get('customField1') is None:
        order['advancedOptions']['customField1'] = ""

    lawn_plan_skus = ["MLP", "TLP", "SFLP", "OLFP", "Organic"]
    has_lawn_plan = any(any(substr in item['sku'] or substr in item['name'] for substr in lawn_plan_skus) for item in order['items'])

    if parent_order:
        parent_has_lawn_plan = any(any(substr in item['sku'] or substr in item['name'] for substr in lawn_plan_skus) for item in parent_order['items'])
        tags_to_preserve = ["Amazon"]
        parent_tags = parent_order['advancedOptions'].get('customField1', '') or ''
        
        for tag in tags_to_preserve:
            if tag in parent_tags:
                order['advancedOptions']['customField1'] = append_tag_if_not_exists(tag, order['advancedOptions']['customField1'])

        if "Subscription First Order" in parent_tags and parent_has_lawn_plan and has_lawn_plan:
            order['advancedOptions']['customField1'] = append_tag_if_not_exists("Subscription First Order", order['advancedOptions']['customField1'])

        if "Recurring" in parent_tags and parent_has_lawn_plan and has_lawn_plan:
            order['advancedOptions']['customField1'] = append_tag_if_not_exists("Recurring", order['advancedOptions']['customField1'])

        otp_order_counter = 0
        for item in order['items']:
            if item['sku'].startswith("OTP"):
                otp_order_counter += 1
                continue

            if 'TLP' in item['sku']:
                order['advancedOptions']['customField1'] = append_tag_if_not_exists("Lone-Star", order['advancedOptions']['customField1'])
                continue
            if 'SFLP' in item['sku']:
                order['advancedOptions']['customField1'] = append_tag_if_not_exists("South-Florida", order['advancedOptions']['customField1'])
                continue
            if 'OLFP' in item['sku'] or 'Organic' in item['sku'] or 'Organic Lawn' in item['name']:
                order['advancedOptions']['customField1'] = append_tag_if_not_exists("Organic", order['advancedOptions']['customField1'])

        if otp_order_counter == len(order['items']):
            order['advancedOptions']['customField1'] = append_tag_if_not_exists("OTP-Only", order['advancedOptions']['customField1'])

    else:
        has_stk_item = any(item['sku'] in ('OTP - STK', 'OTP - LYL') for item in order['items'])
        has_stk_tag = "STK-Order" in order['advancedOptions']['customField1']

        if has_stk_item:
            order['advancedOptions']['customField1'] = append_tag_if_not_exists("STK-Order", order['advancedOptions']['customField1'])
        elif not has_stk_item and has_stk_tag:
            tags = order['advancedOptions']['customField1'].split(', ')
            tags.remove("STK-Order")
            order['advancedOptions']['customField1'] = ', '.join(tags)


    return order

def apply_preset_based_on_pouches(order, mlp_data, use_gnome_preset=False):
    preset = {}
    total_pouches = 0
    special_items = {'OTP - HES': 0}
    total_special_items = 0

    def process_and_update(item):
        nonlocal total_pouches, special_items, total_special_items
        process_item(item, mlp_data)

        if item['sku'] in special_items:
            special_items[item['sku']] += item['quantity']
            total_special_items += item['quantity']
        else:
            total_pouches += item['quantity'] * config.sku_to_pouches.get(item['sku'], 0)

        return item

    with ThreadPoolExecutor(max_workers=len(order['items'])) as executor:
        processed_items = list(executor.map(process_and_update, order['items']))

    order['items'] = processed_items

    preset_dict = config.presets_with_gnome if use_gnome_preset else config.presets

    if total_pouches == 0:
        if len(order['items']) == 1:
            if order['items'][0]['sku'] == 'OTP - HES' and 'Sprayer' in order['items'][0]['name']:
                preset = preset_dict['HES']
            else:
                preset_key = str(total_pouches)
                if preset_key in preset_dict:
                    preset = preset_dict[preset_key]
        else:
            preset_key = str(total_pouches)
            if preset_key in preset_dict:
                preset = preset_dict[preset_key]
    else:
        preset_key = str(total_pouches)
        if preset_key in preset_dict:
            preset = preset_dict[preset_key]

    updated_order = order.copy()
    for key, value in preset.items():
        updated_order[key] = value

    if 'advancedOptions' in order and 'advancedOptions' in preset:
        updated_advanced_options = {**order['advancedOptions'], **preset['advancedOptions']}
    elif 'advancedOptions' in order:
        updated_advanced_options = order['advancedOptions']
    else:
        updated_advanced_options = preset['advancedOptions']

    updated_order['advancedOptions'] = updated_advanced_options

    return updated_order


def submit_order(order):
    response = session.post('https://ssapi.shipstation.com/orders/createorder', data=json.dumps(order))
    return order['orderNumber'], response

def process_order(order, mlp_data, parent_has_gnome=False):
    need_stk_tag = any(item['sku'] == 'OTP - STK' for item in order['items'])
    need_gnome = should_add_gnome_to_parent_order(order) if not parent_has_gnome else False

    if order_split_required(order):
        # Prepare the child orders and parent order
        original_order, child_orders = prepare_split_data(order, need_stk_tag, mlp_data, need_gnome)

        # Execute POST requests for child and parent orders in parallel
        orders_to_process = child_orders + [original_order]
        with ThreadPoolExecutor(max_workers=len(orders_to_process)) as executor:
            futures = {executor.submit(submit_order, order): order for order in orders_to_process}
            for future in as_completed(futures):
                order_number, response = future.result()
                if response.status_code == 200:
                    print(f"Order #{order_number} created successfully")
                    print(f"Full success response: {response.__dict__}")
                else:
                    failed.append(order_number)
                    print(f"Unexpected status code for order #{order_number}: {response.status_code}")
                    print(f"Full error response: {response.__dict__}")

    else:
        if need_gnome:
            gnome_item = config.gnome
            order['items'].append(gnome_item)
            order = apply_preset_based_on_pouches(order, mlp_data, use_gnome_preset=True)
        else:
            order = apply_preset_based_on_pouches(order, mlp_data, use_gnome_preset=False)
        order = set_order_tags(order)
        response = session.post('https://ssapi.shipstation.com/orders/createorder', data=json.dumps(order))

        if response.status_code == 200:
            print(f"Order #{order['orderNumber']} updated successfully with preset")
            print(f"Full success response: {response.__dict__}")
        else:
            failed.append(order['orderNumber'])
            print(f"Unexpected status code for order #{order['orderNumber']}: {response.status_code}")
            print(f"Full error response: {response.__dict__}")

        return f"Successfully processed order #{order['orderNumber']} without splitting"


def first_fit_decreasing(items, max_pouches_per_bin=9):
    items = sorted(items, reverse=True)
    bins = []
    pouches = []
    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(find_suitable_bin, item, bins, pouches, max_pouches_per_bin): item for item in items}
        for future in as_completed(futures):
            item = futures[future]
            try:
                i, item = future.result()
                if i is not None:
                    bins[i] += item
                    pouches[i] += 1
                else:
                    bins.append(item)
                    pouches.append(1)
            except Exception as e:
                print(f"Error occurred while processing item {item}: {e}")
    return bins

def find_suitable_bin(item, bins, pouches, max_pouches_per_bin):
    for i, bin in enumerate(bins):
        if pouches[i] < max_pouches_per_bin:
            return i, item
    return None, item


def prepare_split_data(order, need_stk_tag, mlp_data, need_gnome):
    original_order = copy.deepcopy(order)  # Create a deep copy of the order object
    child_orders = []
    
    # Prepare the list of items with their corresponding pouch count
    items_with_pouch_count = []
    for item in original_order['items']:
        pouch_count = config.sku_to_pouches.get(item['sku'], 0)
        items_with_pouch_count.extend([pouch_count] * item['quantity'])

    # Apply the FFD algorithm
    bins = first_fit_decreasing(items_with_pouch_count)

    # Iterate through the bins returned
    for bin in bins:
        child_order_items = []

        for pouch_count in bin:
            for item in original_order['items']:
                item_pouch_count = config.sku_to_pouches.get(item['sku'], 0)
                if item['quantity'] > 0 and item_pouch_count == pouch_count:
                    item_copy = copy.deepcopy(item)
                    item_copy['quantity'] = 1
                    child_order_items.append(item_copy)
                    item['quantity'] -= 1
                    break

        child_order = prepare_child_order(original_order, child_order_items, mlp_data)
        child_orders.append(child_order)

    total_shipments = len(child_orders)

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
    original_order = set_order_tags(original_order)

    for i in range(len(child_orders)):
        child_order = copy.deepcopy(child_orders[i])
        child_order['orderNumber'] = f"{order['orderNumber']}-{i + 2}"
        child_order['advancedOptions']['customField2'] = f"Shipment {i + 2} of {total_shipments}"
        
        child_order = set_order_tags(child_order)
        child_orders[i] = child_order

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

    child_order = set_order_tags(child_order, parent_order)

    child_order['advancedOptions']['billToParty'] = "my_other_account"
    parent_order['advancedOptions']['billToParty'] = "my_other_account"

    return child_order