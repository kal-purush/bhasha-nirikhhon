from dynamodb_wrapper.meta import ModelMetadata


class Client(object):
    def __init__(self, connection):
        self.connection = connection

    def create_schema(self, model, provisioned_throughput=None, wait=False):
        meta = ModelMetadata(model)

        if not provisioned_throughput:
            provisioned_throughput = {
                'ReadCapacityUnits': 25,
                'WriteCapacityUnits': 25
            }

        meta.create_table(self.connection, provisioned_throughput, wait)