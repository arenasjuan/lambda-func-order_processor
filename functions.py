import requests
import base64
import config
import copy
import uuid
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import json
import time
from typing import List, Tuple

auth_string = f"{config.SHIPSTATION_API_KEY}:{config.SHIPSTATION_API_SECRET}"
encoded_auth_string = base64.b64encode(auth_string.encode('utf-8')).decode('utf-8')

headers = {
    "Content-Type": "application/json",
    "Authorization": f"Basic {encoded_auth_string}",
    "X-Partner": config.x_partner
}

session = requests.Session()
session.headers.update(headers)


failed = []

def processor(order):
    order_number = order['orderNumber']

    mlp_data = {}
    has_lawn_plan = any(isLawnPlan(item["sku"]) for item in order["items"])
    if has_lawn_plan:
        url_mlp = f"https://user-api-dev-qhw6i22s2q-uc.a.run.app/order?shopify_order_no={order_number}"
        response_mlp = session.get(url_mlp)
        data_mlp = response_mlp.json()
        plan_details = data_mlp.get("plan_details", [])

        for order_item in order['items']:
            if isLawnPlan(order_item['sku']):
                for detail in plan_details:
                    if detail['sku'] == order_item['sku']:
                        product_list = []
                        for product in detail['products']:
                            product_list.append({
                                'name': product['name'],
                                'count': product['count']
                            })
                        mlp_data[detail['sku']] = product_list
                        break

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
        if isLawnPlan(original_sku):
            for sku, products_info in mlp_data.items():
                if sku == original_sku:
                    item['name'] = config.SKU_REPLACEMENTS[original_sku]
                    for product_info in products_info:
                        item['name'] += f"\n\u00A0\u00A0\u00A0\u00A0• {product_info['count']} {product_info['name']}"
                    break
        else:
            replacement_name = config.SKU_REPLACEMENTS[original_sku]
            item["name"] = replacement_name
    return item

def order_split_required(order):
    return total_pouches(order) > 9


def should_add_gnome_to_parent_order(parent_order):
    custom_field1 = parent_order['advancedOptions'].get('customField1', "")
    return isinstance(custom_field1, str) and "First" in custom_field1 and any(isLawnPlan(item["sku"]) for item in parent_order["items"])

def append_tag_if_not_exists(tag, custom_field):
    if tag not in custom_field:
        custom_field += (", " if custom_field else "") + tag
    return custom_field

def set_order_tags(order, parent_order, total_pouches):
    if 'customField1' in order['advancedOptions']:
        del order['advancedOptions']['customField1']
        order['advancedOptions']['customField1'] = ""

    if order.get('tagIds') is not None:
        if 64097 in order['tagIds'] and any(item['sku'] in ('OTP - STK', 'OTP - LYL') for item in order['items']):
            order['tagIds'] = [64097]
        else:
            order['tagIds'] = []
    else:
        order['tagIds'] = []

    lawn_plan_skus = ["MLP", "TLP", "SFLP", "OLFP", "Organic", "SELP", "GSLP"]
    has_lawn_plan = any(any(plan_sku in item['sku'] for plan_sku in lawn_plan_skus) for item in order['items'])

    parent_has_lawn_plan = any(any(plan_sku in item['sku'] for plan_sku in lawn_plan_skus) for item in parent_order['items'])
    parent_tags = parent_order['advancedOptions'].get('customField1', '') or '' 
    
    if 'Amazon' in parent_tags:
        order['tagIds'].append(63002)
        append_tag_if_not_exists('Amazon', order['advancedOptions']['customField1'])

    if parent_has_lawn_plan and has_lawn_plan:
        if "First" in parent_tags:
            order['tagIds'].append(62743)
            append_tag_if_not_exists('Subscription First Order', order['advancedOptions']['customField1'])
        elif "Recurring" or "Prepaid" in parent_tags:
            order['tagIds'].append(62744)
            append_tag_if_not_exists('Subscription Recurring', order['advancedOptions']['customField1'])

    otp_order_counter = 0
    for item in order['items']:

        # Checks for OTPs
        if item['sku'].startswith("OTP"):
            otp_order_counter += 1
            continue
        if 63002 in order['tagIds'] and item['sku'] in config.amazon_otp_skus:
            otp_order_counter += 1
            continue

        if 'TLP' in item['sku']:
            order['tagIds'].append(59793)
            continue
        if 'SFLP' in item['sku']:
            order['tagIds'].append(59794)
            continue
        if 'OLFP' in item['sku'] or 'Organic' in item['sku'] or 'Organic Lawn' in item['name']:
            order['tagIds'].append( 62745)

    if otp_order_counter == len(order['items']):
        order['tagIds'].append(59254)

    if 0 < total_pouches <= 9:
        order['tagIds'].append(config.pouch_tags[total_pouches])

    return order

def apply_preset_based_on_pouches(order, mlp_data, total_pouches, use_gnome_preset=False):
    preset = {}

    with ThreadPoolExecutor(max_workers=len(order['items'])) as executor:
        processed_items = list(executor.map(lambda item: process_item(item, mlp_data), order['items']))

    order['items'] = processed_items

    preset_dict = config.presets_with_gnome if use_gnome_preset else config.presets

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
    return response

def total_pouches(order):
    return sum(item['quantity'] * config.sku_to_pouches.get(item['sku'], 0) for item in order['items'])

def process_order(order, mlp_data, parent_has_gnome=False):
    need_gnome = should_add_gnome_to_parent_order(order) if not parent_has_gnome else False

    if order_split_required(order):
        # Prepare the child orders and parent order
        original_order, child_orders = prepare_split_data(order, mlp_data, need_gnome)

        # Execute POST requests for child and parent orders in parallel
        orders_to_process = [original_order] + child_orders
        with ThreadPoolExecutor(max_workers=len(orders_to_process)) as executor:
            responses = list(executor.map(submit_order, orders_to_process))

        # Check responses for successful order creation
        for i, response in enumerate(responses):
            order_processed = orders_to_process[i]['orderNumber']
            if response.status_code == 200:
                print(f"(Log for #{order_processed}) Order #{order_processed} created successfully", flush=True)
                print(f"(Log for #{order_processed}) Full success response: {response.__dict__}", flush=True)
            else:
                failed.append(order_processed)
                print(f"(Log for #{order_processed}) Unexpected status code for order #{order_processed}: {response.status_code}", flush=True)
                print(f"(Log for #{order_processed}) Full error response: {response.__dict__}", flush=True)
        print(f"(Log for #{order['orderNumber']}) Successfully split and processed order #{order['orderNumber']}", flush=True)
        return

    else:
        parent_pouches = total_pouches(order)
        if need_gnome:
            gnome_item = config.gnome
            order['items'].append(gnome_item)
            order = apply_preset_based_on_pouches(order, mlp_data, parent_pouches, use_gnome_preset=True)
        else:
            order = apply_preset_based_on_pouches(order, mlp_data, parent_pouches)
        copied_order = copy.deepcopy(order)
        order = set_order_tags(order, copied_order, parent_pouches)
        response = submit_order(order)

        if response.status_code == 200:
            print(f"(Log for #{order['orderNumber']}) Successfully processed order #{order['orderNumber']} without splitting", flush=True)
            print(f"(Log for #{order['orderNumber']}) Full success response: {response.__dict__}", flush=True)
        else:
            failed.append(order['orderNumber'])
            print(f"(Log for #{order['orderNumber']}) Unexpected status code for order #{order['orderNumber']}: {response.status_code}", flush=True)
            print(f"(Log for #{order['orderNumber']}) Full error response: {response.__dict__}", flush=True)
        return

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
                is_first_bin = bins.index(bin) == 0
                max_pouches = max_pouches_per_bin - (1 if otp_lyl_present and is_first_bin else 0)
                available_space = max_pouches - sum([config.sku_to_pouches.get(sku, 1) * count for sku, count in bin])

                if available_space >= config.sku_to_pouches.get(item[0], 1):
                    existing_item = next((bin_item for bin_item in bin if bin_item[0] == item[0]), None)
                    if existing_item:
                        index = bin.index(existing_item)
                        bin[index] = (item[0], existing_item[1] + 1)
                    else:
                        bin.append((item[0], 1))
                    remaining_pouches -= 1
                    found_bin = True
                    break

            if not found_bin:
                bins.append([(item[0], 1)])
                remaining_pouches -= 1

    if otp_lyl_present:
        # Add OTP - LYL back into the first bin (parent order) with hardcoded quantity of 1
        bins[0].append(('OTP - LYL', 1))

    return bins


def prepare_split_data(order, mlp_data, need_gnome):
    original_order = copy.deepcopy(order)
    child_orders = []

    items_with_quantity = [
        (item['sku'], item['quantity'])
        for item in original_order['items'] if item['sku']
    ]
    bins = first_fit_decreasing(items_with_quantity)

    stk_item = next(
        (item for item in original_order['items'] if item['sku'] == 'OTP - STK'), None
    )
    if stk_item:
        bins[0].append(('OTP - STK', 1))

    parent_items = bins[0]
    total_shipments = len(bins)

    original_order_items = []

    for item in original_order['items']:
        if not item['sku']:  # Add this condition
            continue
        if item['sku'] == 'OTP - STK':
            original_order_items.append(item)
            continue

        parent_item = next(
            (x for x in parent_items if x[0] == item['sku']), None
        )
        if parent_item:
            item_copy = copy.deepcopy(item)
            item_copy['quantity'] = parent_item[1]
            original_order_items.append(item_copy)

    original_order['items'] = original_order_items

    order_pouches = total_pouches(original_order)
    original_order = set_order_tags(original_order, order, order_pouches)

    if need_gnome:
        gnome = config.gnome
        original_order['items'].append(gnome)
        original_order = apply_preset_based_on_pouches(
            original_order, mlp_data, order_pouches, use_gnome_preset=True
        )
    else:
        original_order = apply_preset_based_on_pouches(
            original_order, mlp_data, order_pouches
        )

    with ThreadPoolExecutor(max_workers=len(bins) - 1) as executor:
        args_list = [
            (bin_index, bin, order, mlp_data, total_shipments)
            for bin_index, bin in enumerate(bins[1:])
        ]
        child_orders = []

        for result in executor.map(prepare_child_order, args_list):
            child_orders.append(result)

    original_order['orderNumber'] = f"{original_order['orderNumber']}-1"
    original_order['advancedOptions']['customField2'] = f"Shipment 1 of {total_shipments}"

    print(f"(Log for #{order['orderNumber']}) Parent order for {order['orderNumber']}: {original_order}", flush=True)
    print(f"(Log for #{order['orderNumber']}) Child_orders for {order['orderNumber']}: {child_orders}", flush=True)

    return original_order, child_orders


def prepare_child_order(args):
    bin_index, bin, parent_order, mlp_data, total_shipments = args
    child_order = copy.deepcopy(parent_order)
    if 'orderId' in child_order:
        del child_order['orderId']
    child_order['tagIds'] = []
    child_order['orderNumber'] = f"{parent_order['orderNumber']}-{bin_index+2}"
    child_order['advancedOptions']['customField1'] = ''
    child_order['advancedOptions']['customField2'] = f"Shipment {bin_index+2} of {total_shipments}"
    
    child_order_items = []
    for sku, item_count in bin:
        item = next((i for i in parent_order['items'] if i['sku'] == sku), None)
        if item is not None:
            item_copy = copy.deepcopy(item)
            item_copy['quantity'] = item_count
            if item_copy['quantity'] > 0:
                child_order_items.append(item_copy)
        else:
            continue
    child_order['items'] = child_order_items
    child_order['orderTotal'] = 0.00
    child_order['orderKey'] = str(uuid.uuid4())
    child_pouches = total_pouches(child_order)

    child_order = apply_preset_based_on_pouches(child_order, mlp_data, child_pouches)

    child_order = set_order_tags(child_order, parent_order, child_pouches)

    return child_order