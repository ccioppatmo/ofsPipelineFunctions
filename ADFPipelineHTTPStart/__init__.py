""" This function an HTTP starter function for Durable Functions, which calls an 
    orchestrator function that manages the 'fanning out' of work to sub-orchestrator that 
    asynchronously starts tasks to be completed by activity handlers.
"""
 
import logging

import azure.functions as func
import azure.durable_functions as df
from ..shared_code.MyClasses import SerializableClass
import json

async def main(req: func.HttpRequest, starter: str) -> func.HttpResponse:
    client = df.DurableOrchestrationClient(starter)
    request_body: SerializableClass = req.get_input()
    request_payload = request_body.get_payload()
    logging.log(f'request_body type: {type(request_body)}')
    logging.log(f'request_body: {request_body}')
    logging.log(f'request_payload type: {type(request_payload)}')
    logging.log(f'request_payload: {request_payload}')
    function_name = req.route_params['functionName']
    #orchestrator_payload = req.get_body()
    logging.log = (f'About to start ADFFunctionOrchestrator - Event Payload: {json.dumps(request_payload)}')
    instance_id = await client.start_new(function_name, None, None)
    logging.log = (f'ADFFunctionOrchestrator started with Instance ID : {instance_id}')

    return client.create_check_status_response(req, instance_id)