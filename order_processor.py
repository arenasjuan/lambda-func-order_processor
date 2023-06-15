import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import functions

def lambda_handler(event, context):

    # Filters out child orders when they reactivate this function after being created in Shipstation
    orders = [order for order in functions.extract_data_from_resource_url(event) if "-" not in order['orderNumber']]
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
        print(f"Failed to process orders: {functions.failed}")

    return {
        'statusCode': 200,
        'body': json.dumps('Lambda function executed successfully')
    }
