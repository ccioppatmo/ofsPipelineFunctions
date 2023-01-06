# This function is not intended to be invoked directly. Instead it will be
# triggered by an HTTP starter function.

import logging
import json

import azure.functions as func
import azure.durable_functions as df
from ..shared_code.MyClasses import SerializableClass

results = {}
def orchestrator_function(context: df.DurableOrchestrationContext):
    event_obj: SerializableClass = context.get_input()
    if (event_obj):
        print(f'event_obj type: {type(event_obj)}')
        print(f'event_obj: {event_obj}')
        event_payload = event_obj.get_payload()
        print(f'event_payload type: {type(event_payload)}')
        print(f'event_payload: {event_payload}')
        event_json = json.loads(context._input)
        print(f'event_json type: {type(event_json)}')
        print(f'event_json: {event_json}')
        task_handler = event_json['task_handler']['handler_name']
        print(f'task_handler: {task_handler}')
        tasks = event_json['task_handler']['tasks']
        print(f'tasks: {tasks}')
        for t in tasks:
            tasks = []
            tasks.append(context.call_activity(task_handler, SerializableClass(5)))
        print(f'tasks: {tasks}')
        results = yield context.task_all(tasks)
    else:
        results = None
        yield results
    print(f'results: {results}')     
    return results
main = df.Orchestrator.create(orchestrator_function)
