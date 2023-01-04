# This function is not intended to be invoked directly. Instead it will be
# triggered by an HTTP starter function.

import logging
import json

import azure.functions as func
import azure.durable_functions as df
from ..shared_code.MyClasses import SerializableClass

results = {}
def orchestrator_function(context: df.DurableOrchestrationContext):
    event: SerializableClass = context.get_input()

    if (event):
        event: str = event.get_payload()
        event_data = json.loads(event)
        task_handler = event_data['task_handler']['handler_name']
        tasks = event_data['task_handler']['tasks']
        for t in tasks:
            tasks = []
            tasks.append(context.call_activity(task_handler, SerializableClass(5)))
        results = yield context.task_all(tasks)
    else:
        results = None
        yield results
    return results
main = df.Orchestrator.create(orchestrator_function)
