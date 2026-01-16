from watson_developer_cloud import AssistantV1
import json
import os

class PythonAssistantConnector():
    
    def __init__(self, iam_apikey, url):
        self.assistant = AssistantV1(
            version='2018-09-20',
            iam_apikey=iam_apikey,
            url=url
        )
        
    def set_workspace_id(self, workspace_id):
        self.workspace_id = workspace_id
        
    def list_workspaces(self):
        response = self.assistant.list_workspaces().get_result()
        return response
    
    def list_intents(self):
        response = self.assistant.list_intents(
            workspace_id = self.workspace_id
        ).get_result()
        
        return response
    
    def create_workspace(self, name, description):
        response = self.assistant.create_workspace(
            name = name,
            description=description
        ).get_result()
        
        return response

    def list_entities(self):
        response = self.assistant.list_entities(
            workspace_id = self.workspace_id
        ).get_result()
        
        return response
    
    def create_intent(self, intent):
        response = self.assistant.create_intent(
            workspace_id = self.workspace_id,
            intent = intent,
            examples = [
                {'text': intent}
            ]
        ).get_result()
        
        return response

    def create_entity(self, entity):
        response = self.assistant.create_entity(
            workspace_id = self.workspace_id,
            entity = entity
        ).get_result()
        
        return response
    
    def list_examples(self, intent):
        response = self.assistant.list_examples(
            workspace_id = self.workspace_id,
            intent = intent
        ).get_result()
        
        return response

    def list_values(self, entity):
        response = self.assistant.list_values(
            workspace_id = self.workspace_id,
            entity = entity
        ).get_result()
        
        return response

    def create_value(self, entity, value):
        response = self.assistant.create_value(
            workspace_id = self.workspace_id,
            entity = entity,
            value = value
        ).get_result()
        
        return response
    
    def message(self, message):
        response = self.assistant.message(
            workspace_id= self.workspace_id,
            input={
                'text': message
            }
        ).get_result()
        
        return response

    def remove_workspace(self, workspace_id = ""):
        response = self.assistant.delete_workspace(
            workspace_id = self.workspace_id
        ).get_result()
        
        return response

        