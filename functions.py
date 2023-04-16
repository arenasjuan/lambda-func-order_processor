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
    if 'customField1' in order['advancedOptions']:
        del order['advancedOptions']['customField1']
        order['advancedOptions']['customField1'] = ""        

    print(f"Tags for {order['orderNumber']}: {order['advancedOptions']['customField1']}")

    lawn_plan_skus = ["MLP", "TLP", "SFLP", "OLFP", "Organic"]
    has_lawn_plan = any(any(plan_sku in item['sku'] for plan_sku in lawn_plan_skus) for item in order['items'])
    print(f"{order['orderNumber']}items: {order['items']}, has a lawn plan: {has_lawn_plan}")

    parent_has_lawn_plan = any(any(plan_sku in item['sku'] for plan_sku in lawn_plan_skus) for item in parent_order['items'])
    parent_tags = parent_order['advancedOptions'].get('customField1', '') or ''
    
    if 'Amazon' in parent_tags:
        order['advancedOptions']['customField1'] = append_tag_if_not_exists('Amazon', order['advancedOptions']['customField1'])
    print(f"Tags 2 for {order['orderNumber']}: {order['advancedOptions']['customField1']}")

    if parent_has_lawn_plan and has_lawn_plan:
        if "Subscription First Order" in parent_tags:
            order['advancedOptions']['customField1'] = append_tag_if_not_exists("Subscription First Order", order['advancedOptions']['customField1'])
        elif "Recurring" in parent_tags:
            order['advancedOptions']['customField1'] = append_tag_if_not_exists("Subscription Recurring", order['advancedOptions']['customField1'])

    otp_order_counter = 0
    for item in order['items']:
        if item['sku'].startswith("OTP"):
            otp_order_counter += 1
            continue

        if 'TLP' in item['sku']:
            order['advancedOptions']['customField1'] = append_tag_if_not_exists("Lone-Star", order['advancedOptions']['customField1'])
            print(f"Tags 3 for {order['orderNumber']}: {order['advancedOptions']['customField1']}")
            continue
        if 'SFLP' in item['sku']:
            order['advancedOptions']['customField1'] = append_tag_if_not_exists("South-Florida", order['advancedOptions']['customField1'])
            print(f"Tags 4 for {order['orderNumber']}: {order['advancedOptions']['customField1']}")
            continue
        if 'OLFP' in item['sku'] or 'Organic' in item['sku'] or 'Organic Lawn' in item['name']:
            order['advancedOptions']['customField1'] = append_tag_if_not_exists("Organic", order['advancedOptions']['customField1'])
            print(f"Tags 5for {order['orderNumber']}: {order['advancedOptions']['customField1']}")

    if otp_order_counter == len(order['items']):
        order['advancedOptions']['customField1'] = append_tag_if_not_exists("OTP-Only", order['advancedOptions']['customField1'])

    
    has_stk_item = any(item['sku'] in ('OTP - STK', 'OTP - LYL') for item in order['items'])
    has_stk_tag = "STK-Order" in order['advancedOptions']['customField1']

    if has_stk_item:
        order['advancedOptions']['customField1'] = append_tag_if_not_exists("STK-Order", order['advancedOptions']['customField1'])
    elif not has_stk_item and has_stk_tag:
        tags = order['advancedOptions']['customField1'].split(', ')
        tags.remove("STK-Order")
        order['advancedOptions']['customField1'] = ', '.join(tags)

    print(f"Tags for {order['orderNumber']} after set_tags: {order['advancedOptions']['customField1']}")
    return order

def apply_preset_based_on_pouches(order, mlp_data, use_gnome_preset=False):
    preset = {}
    total_pouches = 0
    base_weights = {'1': 6, '2': 10, '3': 15}

    def process_and_update(item):
        nonlocal total_pouches
        process_item(item, mlp_data)

        if item['sku'] != 'OTP - HES':
            total_pouches += item['quantity'] * config.sku_to_pouches.get(item['sku'], 0)

        return item

    with ThreadPoolExecutor() as executor:
        processed_items = list(executor.map(process_and_update, order['items']))

    order['items'] = processed_items
    print(f"Processed items for {order['orderNumber']}): {processed_items}")

    preset_dict = config.presets_with_gnome if use_gnome_preset else config.presets

    if total_pouches == 0 and len(order['items']) == 1 and order['items'][0]['sku'] == 'OTP - HES':
        hes_quantity = str(order['items'][0]['quantity'])
        hes_weight = base_weights.get(hes_quantity, None)
        if hes_weight is not None:
            preset = preset_dict['HES'].copy()
            preset['weight']['value'] = hes_weight
        else:
            preset = preset_dict['HES']
    else:
        preset_key = str(total_pouches)
        if preset_key in preset_dict:
            preset = preset_dict[preset_key]

    print(f"Preset key for for {order['orderNumber']}): {preset_key}")

    print(f"Presets: {preset}")

    updated_order = order.copy()
    for key, value in preset.items():
        updated_order[key] = value

    print(f"Order with presets added: {updated_order}")

    if 'advancedOptions' in order and 'advancedOptions' in preset:
        updated_advanced_options = {**order['advancedOptions'], **preset['advancedOptions']}
    elif 'advancedOptions' in order:
        updated_advanced_options = order['advancedOptions']
    else:
        updated_advanced_options = preset['advancedOptions']

    updated_order['advancedOptions'] = updated_advanced_options

    print(f"Final updated order: {updated_order}")

    return updated_order

def submit_order(order):
    response = session.post('https://ssapi.shipstation.com/orders/createorder', data=json.dumps(order))
    return response

def process_order(order, mlp_data, parent_has_gnome=False):
    need_stk_tag = any(item['sku'] == 'OTP - STK' for item in order['items'])
    need_gnome = should_add_gnome_to_parent_order(order) if not parent_has_gnome else False

    if order_split_required(order):
        # Prepare the child orders and parent order
        original_order, child_orders = prepare_split_data(order, need_stk_tag, mlp_data, need_gnome)
        print(f"Check before processing — parent: {original_order}")
        print(f"Check before processing — children: {child_orders}")

        # Execute POST requests for child and parent orders in parallel
        orders_to_process = [original_order] + child_orders
        with ThreadPoolExecutor() as executor:
            responses = list(executor.map(submit_order, orders_to_process))

        # Check responses for successful order creation
        for i, response in enumerate(responses):
            order_processed = orders_to_process[i]['orderNumber']
            if response.status_code == 200:
                print(f"Order #{order_processed} created successfully")
                print(f"Full success response: {response.__dict__}")
            else:
                failed.append(order_processed)
                print(f"Unexpected status code for order #{order_processed}: {response.status_code}")
                print(f"Full error response: {response.__dict__}")

        return f"Successfully processed order #{order['orderNumber']}"

    else:
        if need_gnome:
            gnome_item = config.gnome
            order['items'].append(gnome_item)
            order = apply_preset_based_on_pouches(order, mlp_data, use_gnome_preset=True)
        else:
            order = apply_preset_based_on_pouches(order, mlp_data, use_gnome_preset=False)
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
    otp_lyl_present = any(item[0] == 'OTP - LYL' for item in items)
    if otp_lyl_present:
        items = [(sku, count) for sku, count in items if sku != 'OTP - LYL']

    items = sorted(items, key=lambda x: x[1], reverse=True)
    bins = []

    for item in items:
        remaining_pouches = item[1]
        while remaining_pouches > 0:
            found_bin = False
            for bin in bins:
                # Check if the current bin is the first bin and if OTP - LYL is present in the order.
                is_first_bin = bins.index(bin) == 0
                max_pouches = max_pouches_per_bin - (1 if otp_lyl_present and is_first_bin else 0)

                available_space = max_pouches - sum([count for _, count in bin])
                pouches_to_add = min(available_space, remaining_pouches)
                if pouches_to_add > 0:
                    # Update the existing SKU count in the bin, if present
                    existing_item = next((bin_item for bin_item in bin if bin_item[0] == item[0]), None)
                    if existing_item:
                        index = bin.index(existing_item)
                        bin[index] = (item[0], existing_item[1] + pouches_to_add)
                    else:
                        bin.append((item[0], pouches_to_add))

                    remaining_pouches -= pouches_to_add
                    found_bin = True
                    break

            if not found_bin:
                pouches_to_add = min(max_pouches_per_bin, remaining_pouches)
                bins.append([(item[0], pouches_to_add)])
                remaining_pouches -= pouches_to_add

    if otp_lyl_present:
        # Add OTP - LYL back into the first bin (parent order) with hardcoded pouch count of 1
        bins[0].append(('OTP - LYL', 1))

    return bins




def prepare_split_data(order, need_stk_tag, mlp_data, need_gnome):
    print(f"Check original order: {order}")
    original_order = copy.deepcopy(order)  # Create a deep copy of the order object
    child_orders = []

    items_with_pouch_count = [(item['sku'], config.sku_to_pouches.get(item['sku'], 0)) for item in original_order['items'] for _ in range(item['quantity'])]

    bins = first_fit_decreasing(items_with_pouch_count)

    print(f"Final bins: {bins}")

    stk_item = next((item for item in original_order['items'] if item['sku'] == 'OTP - STK'), None)

    if stk_item:
        bins[0].append(('OTP - STK', 1))

    parent_items = bins[0]
    print(f"Parent items: {parent_items}")

    total_shipments = len(bins)

    original_order_items = []
    for item in original_order['items']:
        if item['sku'] == 'OTP - STK':
            original_order_items.append(item)
            continue

        parent_item = next((x for x in parent_items if x[0] == item['sku']), None)
        if parent_item:
            item_copy = copy.deepcopy(item)
            item_copy['quantity'] = parent_item[1] // config.sku_to_pouches.get(item['sku'], 1)
            original_order_items.append(item_copy)

    original_order['items'] = original_order_items
    original_order = set_order_tags(original_order, order)

    if need_gnome:
        gnome = config.gnome
        original_order['items'].append(gnome)
        original_order = apply_preset_based_on_pouches(original_order, mlp_data, use_gnome_preset=True)
    else:
        original_order = apply_preset_based_on_pouches(original_order, mlp_data)

    with ThreadPoolExecutor(max_workers=len(bins) - 1) as executor:
        args_list = [(bin_index, bin, order, mlp_data, total_shipments) for bin_index, bin in enumerate(bins[1:])]
        child_orders = []
        for result in executor.map(prepare_child_order, args_list):
            child_orders.append(result)

    print(f"Parent items: {original_order_items}")
    original_order['orderNumber'] = f"{original_order['orderNumber']}-1"
    original_order['advancedOptions']['customField2'] = f"Shipment 1 of {total_shipments}"
    original_order['advancedOptions']['billToParty'] = "my_other_account"

    print(f"Parent order: {original_order}")
    print(f"Child_orders: {child_orders}")
    for child in child_orders:
        print(f"Final tags for {child['orderNumber']}: {child['advancedOptions']['customField1']}")
    return original_order, child_orders




def prepare_child_order(args):
    print("Preparing child order with args:", args)
    bin_index, bin, parent_order, mlp_data, total_shipments = args

    child_order = copy.deepcopy(parent_order)
    if 'orderId' in child_order:
        del child_order['orderId']
    child_order['orderNumber'] = f"{parent_order['orderNumber']}-{bin_index+2}"
    child_order['advancedOptions']['customField2'] = f"Shipment {bin_index+2} of {total_shipments}"
    print(f"Bin for {child_order['orderNumber']}: {bin}")

    print(f"Check {child_order['orderNumber']}, parent items = {parent_order['items']}")
    
    child_order_items = []
    for sku, pouch_count in bin:
        item = next((i for i in parent_order['items'] if i['sku'] == sku), None)
        if item is not None:
            item_copy = copy.deepcopy(item)
            item_copy['quantity'] = pouch_count // config.sku_to_pouches.get(item['sku'], 1)
            if item_copy['quantity'] > 0:
                child_order_items.append(item_copy)
        else:
            continue
    child_order['items'] = child_order_items
    child_order['orderTotal'] = 0.00
    child_order['orderKey'] = str(uuid.uuid4())
    child_order['advancedOptions']['billToParty'] = "my_other_account"

    print(f"Child order before applying presets (bin_index: {bin_index}): {child_order}")
    child_order = apply_preset_based_on_pouches(child_order, mlp_data)

    child_order = set_order_tags(child_order, parent_order)

    print(f"Child order {bin_index+1}: {child_order}")
    return child_order
