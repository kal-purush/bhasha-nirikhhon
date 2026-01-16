from aws_lambda_powertools.utilities.typing.lambda_context import LambdaContext
from aws_lambda_powertools.utilities.data_classes.api_gateway_proxy_event import (
    APIGatewayProxyEventV2,
)

from models.products import Products
from utils.exception_decorator import error_handler
from utils.response import respond_error
from utils import constant, helpers


@error_handler
def main(event: APIGatewayProxyEventV2, context: LambdaContext):
    path = event.get("path")

    if path == "/events":
        return event_handler(event, context)

    else:
        return respond_error(
            status_code=constant.ERROR_BAD_REQUEST,
            message="Invalid path",
            data=None,
            success=False,
            errors=None
        )


def event_handler(event: APIGatewayProxyEventV2, context: LambdaContext):
    input_data = helpers.load_json(event=event)

    # checking the condition for the operation in json payload

    operation = input_data.get("operation")
    product = input_data.get("product")
    product_id = product.get("product_id")
    if operation == "decrease":
        pass
    elif operation == "increase":
        pass
    else:
        return respond_error(
            status_code=constant.ERROR_BAD_REQUEST,
            message="Invalid operation",
            data=None,
            success=False
        )



