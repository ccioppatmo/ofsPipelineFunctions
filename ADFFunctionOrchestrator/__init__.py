"""
    This main "orchestrator" function is not intended to be invoked directly. Instead it will be triggered by an HTTP Start function.
    This function invokes sub-orchestrators to manage the asynchronous 'fanning out' of tasks to be completed by activity handlers.
"""

import logging
import json

import azure.functions as func
import azure.durable_functions as df
from ..shared_code.MyClasses import SerializableClass

results = {}
results_list = []
sub_orchestrator_activities_submitted = 0
def orchestrator_function(context: df.DurableOrchestrationContext):
    orchestrator_input: SerializableClass = context.get_input()
    if (orchestrator_input):
        print(f'orchestrator_input type: {type(orchestrator_input)}')
        print(f'orchestrator_input: {orchestrator_input}')
        orchestrator_payload = orchestrator_payload.get_payload()
        print(f'orchestrator_payload type: {type(orchestrator_payload)}')
        print(f'orchestrator_payload: {orchestrator_payload}')
        orchestrator_json = json.loads(context._input)
        print(f'orchestrator_json type: {type(orchestrator_json)}')
        print(f'orchestrator_json: {orchestrator_json}')
        if (orchestrator_json):
            for orchestrator_obj in orchestrator_json:
                orchestrator_list = orchestrator_obj['orchestrator']
                print(f'orchestrator_list type: {type(orchestrator_list)}')
                print(f'orchestrator_list: {orchestrator_list}')
                if (orchestrator_list):
                    for sub_orchestrator_obj in orchestrator_list:
                        sub_orchestrator_list = sub_orchestrator_obj['sub_orchestrator']
                        print(f'sub_orchestrator_list type: {type(sub_orchestrator_list)}')
                        print(f'sub_orchestrator_list: {sub_orchestrator_list}')
                        sub_orchestrator_activities_list = []
                        if (sub_orchestrator_list):
                            for activity_obj in sub_orchestrator_list:
                                activity_list = activity_obj['activity']
                                activity_handler_name = activity_obj['activity_handler_name']
                                print(f'activity_list type: {type(activity_list)}')
                                print(f'activity_list: {activity_list}')
                                print(f'activity_handler_name type: {activity_handler_name}')
                                sub_orchestrator_activities_list.append(context.call_activity(activity_handler_name, activity_list))
                            if (len(activity_list) > 0):
                                print(f'sub_orchestrator_activities_list: {sub_orchestrator_activities_list}')
                                results = yield context.task_all(sub_orchestrator_activities_list)
                                results_list.append(results)
                                sub_orchestrator_activities_submitted += 1
                            else:
                                results = f'No activities provided for sub_orchestrator_list: {sub_orchestrator_list}'
                                yield results 
                        else:
                            results = f'No sub_activities provided for sub_orchestrator_list: {sub_orchestrator_list}'
                            yield results 
                else:
                    results = f'No sub_activities provided for orchestrator_list: {orchestrator_list}'
                    yield results 
        else:
            results = f'No orchestrator_json provided for orchestrator_obj: {orchestrator_obj}'
            yield results
    print(f'sub_orchestrator_activities_submitted: {sub_orchestrator_activities_submitted} Results: {results_list}')     
    return results
main = df.Orchestrator.create(orchestrator_function)
