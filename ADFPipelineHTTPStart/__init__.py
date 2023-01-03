""" This function an HTTP starter function for Durable Functions, which calls an 
    orchestrator function to 'fan out' work to function handlers.
"""
 
import logging

import azure.functions as func
import azure.durable_functions as df


async def main(req: func.HttpRequest, starter: str) -> func.HttpResponse:
    client = df.DurableOrchestrationClient(starter)
    function_name = req.route_params['functionName']
    event_data = req.get_body()

    instance_id = await client.start_new(function_name, None, event_data)

    logging_info = f"ADFFunctionOrchestrator started with Instance ID : {instance_id} - Event Payload: {event_data}"
    logging.info(logging_info)
    print(logging_info)
    return client.create_check_status_response(req, instance_id)