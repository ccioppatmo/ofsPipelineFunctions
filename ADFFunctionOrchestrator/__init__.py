"""
    This main "orchestrator" function is not intended to be invoked directly. Instead it will be triggered by an HTTP Start function.
    This function invokes sub-orchestrators to manage the asynchronous 'fanning out' of tasks to be completed by activity handlers.
"""

import logging
import json

import azure.functions as func
import azure.durable_functions as df
from ..shared_code.MyClasses import SerializableClass

activity_pipeline_name = ""
activity_pipeline_workload_purpose = ""
results = {}
results_list = []
activity_task_list = []
payload_list_items_found = 0
payload_list_items_processed = 0
orchestrator_list_items_found = 0
orchestrator_list_items_processed = 0
sub_orchestrator_list_items_found = 0
sub_orchestrator_list_items_processed = 0
activity_task_list_items_found = 0
activity_task_list_items_processed = 0
activity_functions_invoked = 0
total_payload_list_items_found = 0
total_payload_list_items_processed = 0
total_orchestrator_list_items_found = 0
total_orchestrator_list_items_processed = 0
total_sub_orchestrator_list_items_found = 0
total_sub_orchestrator_list_items_processed = 0
total_activity_task_list_items_found = 0
total_activity_functions_invoked = 0
def orchestrator_function(context: df.DurableOrchestrationContext):
    orchestrator_input: SerializableClass = context.get_input()
    if (orchestrator_input):
        logging.log(f'orchestrator_input type: {type(orchestrator_input)}')
        logging.log(f'orchestrator_input: {orchestrator_input}')
        orchestrator_payload = orchestrator_input.get_payload()
        logging.log(f'orchestrator_payload type: {type(orchestrator_payload)}')
        logging.log(f'orchestrator_payload: {orchestrator_payload}')
        orchestrator_json = json.loads(orchestrator_payload)
        logging.log(f'orchestrator_json type: {type(orchestrator_json)}')
        logging.log(f'orchestrator_json: {orchestrator_json}')
        payload_list_items_found = 0
        payload_list_items_processed = 0
        if (isinstance(orchestrator_json, list)):
            payload_list_items_found += len(orchestrator_json)
            total_payload_list_items_found += len(orchestrator_json)
            for orchestrator_obj in orchestrator_json:
                payload_list_items_processed += 1
                total_payload_list_items_processed += 1
                orchestrator_list = orchestrator_obj.get('orchestrator',None)
                logging.log(f'orchestrator_list type: {type(orchestrator_list)}')
                logging.log(f'orchestrator_list: {orchestrator_list}')
                orchestrator_list_items_found = 0
                orchestrator_list_items_processed = 0
                if (isinstance(orchestrator_list, list)):
                    orchestrator_list_items_found += len(orchestrator_list)
                    total_orchestrator_list_items_found += len(orchestrator_list)
                    for sub_orchestrator_obj in orchestrator_list:
                        orchestrator_list_items_processed += 1
                        total_orchestrator_list_items_processed += 1
                        sub_orchestrator_list = sub_orchestrator_obj.get('sub_orchestrator',None)
                        logging.log(f'sub_orchestrator_list type: {type(sub_orchestrator_list)}')
                        logging.log(f'sub_orchestrator_list: {sub_orchestrator_list}')
                        sub_orchestrator_list_items_found = 0
                        sub_orchestrator_list_items_processed = 0
                        if (isinstance(sub_orchestrator_list, list)):
                            sub_orchestrator_list_items_found += len(sub_orchestrator_list)
                            total_sub_orchestrator_list_items_found += len(sub_orchestrator_list)
                            activity_task_list_to_process = []
                            activity_payload = {}
                            for activity_task_list_obj in sub_orchestrator_list:
                                sub_orchestrator_list_items_processed += 1
                                total_sub_orchestrator_list_items_processed += 1
                                activity_payload["activity_pipeline_name"] = activity_task_list_obj.get('activity_pipeline_name',None)
                                activity_payload["activity_pipeline_workload_purpose"] = activity_task_list_obj.get('activity_pipeline_workload_purpose',None)
                                activity_payload["activity_task_list"] = activity_task_list_obj.get('activity_task_list',None)
                                activity_task_list_items_found = 0
                                activity_functions_invoked = 0
                                if (isinstance(activity_task_list, list)):
                                    activity_task_list_items_found += len(activity_task_list)
                                    total_activity_task_list_items_found += len(activity_task_list)
                                    logging.log(f'Adding task {activity_payload} - {len(activity_task_list)} tasks to activity_task_list_to_process list')
                                    activity_task_list_to_process.append(context.call_sub_orchestrator("ADFFunctionSubOrchestrator", activity_payload))
                                else:
                                    results = f'No activity_task_list found for activity_task_list_obj: {activity_task_list_obj}'
                                    yield results     
                            if (len(activity_task_list_to_process) > 0):
                                logging.log(f'Processing {len(activity_task_list_to_process)} tasks asynchronously...')
                                results = yield context.task_all(activity_task_list_to_process)
                                results_list.append(results)
                                activity_functions_invoked += 1
                                total_activity_functions_invoked += 1
                            else:
                                results = f'No tasks found for activity_task_list: {activity_task_list}'
                                yield results 
                        else:
                            results = f'No activity_task_list provided for activity_task_list_obj: {activity_task_list_obj}'
                            yield results 
                    if (orchestrator_list_items_processed == 0):
                        results = f'No sub_orchestrator_obj found orchestrator_list: {orchestrator_list}'
                        yield results 
                else:
                    results = f'No orchestrator_list provided for orchestrator_obj: {orchestrator_obj}'
                    yield results 
            if (payload_list_items_found == 0):
                results = f'No orchestrator_obj found in sub_orchestrator_list: {orchestrator_json}'
                yield results 
            elif (payload_list_items_processed == 0):
                results = f'No orchestrator_obj processed in orchestrator_json: {orchestrator_json}'
                yield results
        else:
            results = f'No orchestrator_json list found: {orchestrator_json}'
            yield results   
    else:
        results = f'No orchestrator_input found: {orchestrator_input}'
        yield results     
   
    logging.log(f'Total total_payload_list_items_found: {total_payload_list_items_found}')
    logging.log(f'Total total_payload_list_items_processed: {total_payload_list_items_processed}')
    logging.log(f'Total total_orchestrator_list_items_found: {total_orchestrator_list_items_found}')
    logging.log(f'Total total_orchestrator_list_items_processed: {total_orchestrator_list_items_processed}')
    logging.log(f'Total total_sub_orchestrator_list_items_found: {total_sub_orchestrator_list_items_found}')
    logging.log(f'Total total_sub_orchestrator_list_items_processed: {total_sub_orchestrator_list_items_processed}')
    logging.log(f'Total total_activity_task_list_items_found: {total_activity_task_list_items_found}')
    logging.log(f'Total activity_functions_invoked: {total_activity_functions_invoked} Results: {results_list}')  
    yield results   

main = df.Orchestrator.create(orchestrator_function)
