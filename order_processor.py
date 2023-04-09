import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import functions

def lambda_handler(event, context):
    orders = functions.extract_data_from_resource_url(event)
    print(f"Number of orders: {len(orders)}")

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(functions.processor, order) for order in orders]
        for future in as_completed(futures):
            future.result()

    return {
        'statusCode': 200,
        'body': json.dumps('Lambda function executed successfully')
    }
