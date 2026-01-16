from flask_restful import Resource
from flask import jsonify, request
import marshmallow_dataclass
from marshmallow import Schema, fields, post_load, ValidationError
from db.db_manager import Debugger, Message, Metric, DataProcessor
from service.metric_schema import MetricSchema

class AverageMetric(Resource):
    '''
        getting or deleting data specific to sensor id
            http://127.0.0.1:5000/sensors/id/metrics
                post - create new metric data
                get - get the average of metric data
                put - buk update of metrics data
                delete - delete the metrics data
    '''
    metric_schema = MetricSchema()
    metrics_schema = MetricSchema(many=True)
    

    def __init__(self, session):
        self.session = session
        self.processor = DataProcessor()

    def get(self, id):
        result = self.processor.get_avg_by_id(self.session, Metric, id=id)
        print(result)
        return result
      
    def put(self, id):
        return {Message.MSG_FAILED: Message.MSG_NOT_REQUIRED}, 200

    def delete(self, id):
        return {Message.MSG_FAILED: Message.MSG_NOT_REQUIRED}, 200

    def post(self, id):
        return {Message.MSG_FAILED: Message.MSG_NOT_REQUIRED}, 200