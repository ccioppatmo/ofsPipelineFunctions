""" This function an HTTP starter function for Durable Functions, which calls an 
    orchestrator function that manages the 'fanning out' of work to sub-orchestrator that 
    asynchronously starts tasks to be completed by activity handlers.
"""
 
import logging

import azure.functions as func
import azure.durable_functions as df

async def main(req: func.HttpRequest, starter: str) -> func.HttpResponse:
    client = df.DurableOrchestrationClient(starter)
    function_name = req.route_params['functionName']
    #orchestrator_payload = req.get_body()
    logging.log = (f'About to start ADFFunctionOrchestrator - Event Payload: {req}')
    instance_id = await client.start_new(function_name, None, req)
    logging.log = (f'ADFFunctionOrchestrator started with Instance ID : {instance_id}')

    return client.create_check_status_response(req, instance_id)