"""
    This "sub-orchestrator" function is not intended to be invoked directly. 
    Instead it will be invoked by the main orchestrator to manage the asynchronous 'fanning out' 
    of tasks to be completed by activity handlers.
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
sub_orchestrator_list_items_found = 0
sub_orchestrator_list_items_processed = 0
activity_task_list_items_found = 0
activity_task_list_items_processed = 0
activity_functions_invoked = 0
total_payload_list_items_found = 0
total_payload_list_items_processed = 0
total_sub_orchestrator_list_items_found = 0
total_sub_orchestrator_list_items_processed = 0
total_activity_task_list_items_found = 0
total_activity_task_list_items_processed = 0
total_activity_functions_invoked = 0
def orchestrator_function(context: df.DurableOrchestrationContext):
    #sub_orchestrator_input: SerializableClass = context.get_input()
    sub_orchestrator_input = context._input
    sub_orchestrator_input = json.loads(sub_orchestrator_input)
    if (sub_orchestrator_input):
        logging.log(f'sub_orchestrator_input type: {type(sub_orchestrator_input)}')
        logging.log(f'sub_orchestrator_input: {json.loads(sub_orchestrator_input)}')
        sub_orchestrator_payload = sub_orchestrator_input.get_payload()
        logging.log(f'sub_orchestrator_payload type: {type(sub_orchestrator_payload)}')
        logging.log(f'sub_orchestrator_payload: {json.loads(sub_orchestrator_payload)}')
        sub_orchestrator_json = json.loads(sub_orchestrator_payload)
        logging.log(f'sub_orchestrator_json type: {type(sub_orchestrator_json)}')
        logging.log(f'sub_orchestrator_json: {sub_orchestrator_json}')
        activity_pipeline_name = sub_orchestrator_json.get('activity_pipeline_name',None)
        activity_pipeline_workload_purpose = sub_orchestrator_json.get('activity_pipeline_workload_purpose',None)
        activity_task_list = sub_orchestrator_json.get('activity_task_list',None)
        payload_list_items_found = 0
        payload_list_items_processed = 0
        if (isinstance(activity_task_list, list)):
            payload_list_items_found += len(activity_task_list)
            total_payload_list_items_found += len(activity_task_list)
            for activity_task_list_obj in activity_task_list:
                payload_list_items_processed += 1
                total_payload_list_items_processed += 1
                activity_task_list = activity_task_list_obj.get('activity_task_list',None)
                activity_task_list_items_found = 0
                activity_task_list_items_processed = 0
                activity_functions_invoked = 0
                if (isinstance(activity_task_list, list)):
                    activity_task_list_items_found += len(activity_task_list)
                    total_activity_task_list_items_found += len(activity_task_list)
                    for activity_task_list_obj in activity_task_list:
                        activity_task_list_items_processed += 1
                        total_activity_task_list_items_processed += 1
                        activity_payload = {
                            "activity_pipeline_name": activity_pipeline_name,
                            "activity_pipeline_workload_purpose": activity_pipeline_workload_purpose,
                            "activity_task_list_obj": activity_task_list_obj
                        }
                        task_function_name = activity_task_list_obj.get('task_function_name',None)
                        logging.log(f'Adding task {activity_task_list_items_processed} of {activity_task_list_items_found} to activity processing list: function_name: {task_function_name} task: {activity_task_list_obj}')
                        activity_task_list.append(context.call_activity("ADFFunction_qtmofssqlpg", activity_payload))
                    if (len(activity_task_list) > 0):
                        logging.log(f'Processing {len(activity_task_list)} tasks asynchronously...')
                        results = yield context.task_all(activity_task_list)
                        results_list.append(results)
                        activity_functions_invoked += 1
                        total_activity_functions_invoked += 1
                    else:
                        results = f'No tasks found for activity_task_list: {activity_task_list}'
                        yield json.dumps(results) 
                else:
                    results = f'No activity_task_list provided for activity_task_list_obj: {activity_task_list_obj}'
                    yield json.dumps(results) 
            if (payload_list_items_found == 0):
                results = f'No payload_list_items_found in sub_orchestrator_list: {activity_task_list}'
                yield json.dumps(results)    
            elif (payload_list_items_processed == 0):
                results = f'No activity_obj processed in sub_orchestrator_list: {activity_task_list}'
                yield json.dumps(results) 
            else:
                logging.log(f'activity_functions_invoked: {activity_functions_invoked} for activity_pipeline_name: {activity_pipeline_name} with purpose: {activity_pipeline_workload_purpose} Results: {results_list}')     
                yield json.dumps(results)  
        else:
            results = f'No activity_task_list provided for sub_orchestrator_list: {sub_orchestrator_input}'
            yield json.dumps(results) 
    else:
        results = f'No sub_orchestrator_input found: {sub_orchestrator_input}'
        yield json.dumps(results)     
   
    logging.log(f'Total total_payload_list_items_found: {total_payload_list_items_found}')
    logging.log(f'Total total_payload_list_items_processed: {total_payload_list_items_processed}')
    logging.log(f'Total total_sub_orchestrator_list_items_found: {total_sub_orchestrator_list_items_found}')
    logging.log(f'Total total_sub_orchestrator_list_items_processed: {total_sub_orchestrator_list_items_processed}')
    logging.log(f'Total total_activity_task_list_items_found: {total_activity_task_list_items_found}')
    logging.log(f'Total total_activity_task_list_items_processed: {total_activity_task_list_items_processed}')
    logging.log(f'Total activity_functions_invoked: {total_activity_functions_invoked} Results: {results_list}')  
    yield json.dumps(results) 
    return

main = df.Orchestrator.create(orchestrator_function)
