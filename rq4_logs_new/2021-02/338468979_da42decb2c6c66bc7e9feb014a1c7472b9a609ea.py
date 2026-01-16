from aws_cdk import (
    core,
    aws_lambda,
    aws_dynamodb
)


class HitCounter(core.Construct):

    @property
    def handler(self):
        return self._handler

    @property
    def table(self):
        return self._table

    def __init__(self, scope: core.Construct, id: str, downstream: aws_lambda.IFunction, **kwargs):
        super().__init__(scope, id, **kwargs)

        self._table = aws_dynamodb.Table(
            self,
            'Hits',
            partition_key={'name': 'path', 'type': aws_dynamodb.AttributeType.STRING},
            removal_policy=core.RemovalPolicy.DESTROY
        )

        self._handler = aws_lambda.Function(
            self,
            'HitCounterHandler',
            runtime=aws_lambda.Runtime.PYTHON_3_7,
            handler='hitcount.handler',
            code=aws_lambda.Code.asset('lambda'),
            environment={
                'DOWNSTREAM_FUNCTION_NAME': downstream.function_name,
                'HITS_TABLE_NAME': self._table.table_name,
            }
        )

        self._table.grant_read_write_data(self.handler)
        downstream.grant_invoke(self.handler)


'''
What’s going on here?
We declared a new construct class called HitCounter.
As usual, constructor arguments are scope, id and kwargs, and we propagate them to the cdk.Construct base class.
The HitCounter class also takes one explicit keyword parameter downstream of type lambda.IFunction.
 This is where we are going to “plug in” the Lambda function we created in the previous chapter 
 so it can be hit-counted.
'''

'''
What did we do here?
This code is hopefully quite easy to understand:

We defined a DynamoDB table with path as the partition key (every DynamoDB table must have a single partition key).
We defined a Lambda function which is bound to the lambda/hitcount.handler code.
We wired the Lambda’s environment variables to the function_name and table_name of our resources.
'''