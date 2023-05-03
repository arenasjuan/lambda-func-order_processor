import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import functions

def lambda_handler(event, context):
    orders = [order for order in functions.extract_data_from_resource_url(event) if "-" not in str(order['orderNumber'])]
    print(f"Number of processable orders: {len(orders)}")

    with ThreadPoolExecutor(max_workers=3) as executor:
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
