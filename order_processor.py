import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import functions
import time

def lambda_handler(event, context):

    # Filters out child orders when they reactivate this function after being created in Shipstation
    orders = [order for order in functions.extract_data_from_resource_url(event) if "-" not in order['orderNumber'] and order['orderStatus'] == 'awaiting_shipment']
    if orders:
        print(f"{len(orders)} processable orders: {[order['orderNumber'] for order in orders]}")
    else:
        print(f"Unable to extract order information from resource URL; function execution ending")
        return

    with ThreadPoolExecutor(max_workers=9) as executor:
        futures = [executor.submit(functions.processor, order) for order in orders]
        for future in as_completed(futures):
            future.result()

    if len(functions.failed) == 0:
        print("All orders processed successfully!")
    else:
        print(f"Failed to process {len(functions.failed)} orders: {functions.failed}")
        if len(functions.rate_limited) > 0:
            print(f"Rate-limited on {len(functions.rate_limited)} orders: {order['orderNumber'] for order in functions.rate_limited}")
            print(f"Retrying rate-limited orders in 1 minute...")
            time.sleep(60)
            print(f"Now retrying rate-limited orders")

            rate_limited_orders = functions.rate_limited.copy()

            functions.failed = []
            functions.rate_limited = []
            with ThreadPoolExecutor(max_workers=9) as executor:
                futures = [executor.submit(functions.processor, order) for order in rate_limited_orders]
                for future in as_completed(futures):
                    future.result()

            if len(functions.failed) == 0:
                print("All orders processed successfully!")
            else:
                print(f"Retry failed on {len(functions.failed)} orders: {functions.failed}")


    return {
        'statusCode': 200,
        'body': json.dumps('Lambda function executed successfully')
    }
