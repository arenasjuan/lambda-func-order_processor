import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import functions

def lambda_handler(event, context):
    orders = functions.extract_data_from_resource_url(event)
    print(f"Number of orders: {len(orders)}")

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(process_order, order, get_mlp_data(order['orderNumber']), parent_has_gnome=False) for order in orders]
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
