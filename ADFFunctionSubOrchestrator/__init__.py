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

def log_message(sev, msg):
    if (sev == "INFO"):
        log_message("DEBUG", msg)
    elif (sev == "WARNING"):
        logging.warning(msg)
    elif (sev == "ERROR"):
        logging.error(msg) 
    elif (sev == "CRITICAL"):
        logging.critical(msg) 
    elif (sev == "LOG"):
        logging.log(msg)            
    elif (sev == "EXCEPTION"):
        logging.exception(msg) 
    return
    
def orchestrator_function(context: df.DurableOrchestrationContext):
    #sub_orchestrator_input: SerializableClass = context.get_input()
    succeeded = True
    status_code = 200
    status_messages = []
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
    try:
        sub_orchestrator_input = context._input
        sub_orchestrator_input = json.loads(sub_orchestrator_input)
        print(f'type(sub_orchestrator_input): {type(sub_orchestrator_input)}')
        print(f'sub_orchestrator_input: {sub_orchestrator_input}')
    except:
        succeeded = False
        status_messages.append(f'input data missing or invalid - aborting')
    if ((succeeded) and (sub_orchestrator_input)):
        """
        log_message("DEBUG", f'sub_orchestrator_input: {json.loads(sub_orchestrator_input)}')
        sub_orchestrator_payload = sub_orchestrator_input.get_payload()
        print(f'type(sub_orchestrator_payload): {type(sub_orchestrator_payload)}')
        print(f'sub_orchestrator_payload: {sub_orchestrator_payload}')
        log_message("DEBUG", f'sub_orchestrator_payload: {json.loads(sub_orchestrator_payload)}')
        """
        sub_orchestrator_json = json.loads(sub_orchestrator_input)
        print(f'type(sub_orchestrator_json): {type(sub_orchestrator_json)}')
        print(f'sub_orchestrator_json: {sub_orchestrator_json}')
        log_message("DEBUG", f'sub_orchestrator_json: {sub_orchestrator_json}')
        activity_pipeline_name = sub_orchestrator_json.get('activity_pipeline_name',None)
        print(f'activity_pipeline_name: {activity_pipeline_name}')
        activity_pipeline_workload_purpose = sub_orchestrator_json.get('activity_pipeline_workload_purpose',None)
        print(f'activity_pipeline_workload_purpose): {activity_pipeline_workload_purpose}')
        activity_task_list = sub_orchestrator_json.get('activity_task_list',None)
        print(f'type(activity_task_list): {type(activity_task_list)}')
        print(f'activity_task_list: {activity_task_list}')
        if (isinstance(activity_task_list, list)):
            print(f'About to process activity_task_list: {activity_task_list}')
            activity_task_list_items_found += len(activity_task_list)
            total_activity_task_list_items_found += len(activity_task_list)
            activity_task_list_items_processed = 0
            activity_functions_invoked = 0
            for activity_task_list_obj in activity_task_list:
                print(f'Processing activity_task_list_obj: {activity_task_list_obj}')
                print(f'type(activity_task_list_obj): {type(activity_task_list_obj)}')
                activity_task_list_items_processed += 1
                total_activity_task_list_items_processed += 1
                activity_payload = {
                    "activity_pipeline_name": activity_pipeline_name,
                    "activity_pipeline_workload_purpose": activity_pipeline_workload_purpose,
                    "activity_task_list_obj": activity_task_list_obj
                }
                task_function_name = activity_task_list_obj.get('task_function_name',None)
                log_message("DEBUG", f'Adding task {activity_task_list_items_processed} of {activity_task_list_items_found} to activity processing list: PostgreSQL function_name to process: {task_function_name} task: {activity_task_list_obj}')
                activity_task_list.append(context.call_activity("ADFFunction_qtmofssqlpg", activity_payload))
            if (activity_task_list_items_processed > 0):
                log_message("DEBUG", f'Processing {len(activity_task_list)} tasks asynchronously...')
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
    else:
        results = f'No sub_orchestrator_input found: {sub_orchestrator_input}'
        yield json.dumps(results)     
   
    log_message("DEBUG", f'Total total_payload_list_items_found: {total_payload_list_items_found}')
    log_message("DEBUG", f'Total total_payload_list_items_processed: {total_payload_list_items_processed}')
    log_message("DEBUG", f'Total total_sub_orchestrator_list_items_found: {total_sub_orchestrator_list_items_found}')
    log_message("DEBUG", f'Total total_sub_orchestrator_list_items_processed: {total_sub_orchestrator_list_items_processed}')
    log_message("DEBUG", f'Total total_activity_task_list_items_found: {total_activity_task_list_items_found}')
    log_message("DEBUG", f'Total total_activity_task_list_items_processed: {total_activity_task_list_items_processed}')
    log_message("DEBUG", f'Total activity_functions_invoked: {total_activity_functions_invoked} Results: {results_list}')  
  
    return json.dumps(results) 

main = df.Orchestrator.create(orchestrator_function)
