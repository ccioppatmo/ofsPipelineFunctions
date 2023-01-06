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

results = {}
def orchestrator_function(context: df.DurableOrchestrationContext):
    event_obj: SerializableClass = context.get_input()
    print(str(event_obj))
    if (event_obj):
        event = event_obj.get_payload()
        print(f'event type: {type(event)}')
        print(f'event: {event}')
        event_data = json.loads(event)
        print(f'event_data type: {type(event_data)}')
        print(f'event_data: {event_data}')
        task_handler = event_data['task_handler']['handler_name']
        print(f'task_handler: {task_handler}')
        tasks = event_data['task_handler']['tasks']
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
