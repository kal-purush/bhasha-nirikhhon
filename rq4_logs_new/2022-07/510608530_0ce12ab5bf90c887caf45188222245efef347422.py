import logging
import json
import azure.functions as func

def main(req: func.HttpRequest, doc:func.DocumentList) -> func.HttpResponse:
    
    logging.info('Python HTTP trigger function processed a request.')
 
    tasks_json = []

    for task in doc:
        tasksjson = {
           "name": task['name'],
           "description": task['description'],
           "deadline": task['deadline'],
           "isCompleted": task['isCompleted'],
        }
        tasks_json.append(tasksjson)

    return func.HttpResponse(
            json.dumps(tasks_json),
            status_code=200,
            mimetype="application/json"            
    )