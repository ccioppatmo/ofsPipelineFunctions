# This function is not intended to be invoked directly. Instead it will be
# triggered by an HTTP starter function.

import logging
import json

import azure.functions as func
import azure.durable_functions as df


def orchestrator_function(context: df.DurableOrchestrationContext, event):
    event_data=json.loads(event_data)
    task_handler = event_data['task_handler']['handler_name']
    tasks = event_data['task_handler']['tasks']
    for t in tasks:
        tasks = []
        tasks.append(context.call_activity(task_handler, t))
    yield context.task_all(tasks)

main = df.Orchestrator.create(orchestrator_function)
