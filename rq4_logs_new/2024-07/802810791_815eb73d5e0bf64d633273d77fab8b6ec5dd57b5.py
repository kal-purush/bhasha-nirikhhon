import uuid

from aws_lambda_powertools.utilities.typing.lambda_context import LambdaContext

from utils.helpers import boto3_s3_client, generate_8digit_uuid
from utils import variables, constant, helpers
from utils.exception_decorator import error_handler
from utils.response import respond_error, respond_success
from schema.s3 import S3Delete


@error_handler
def main(event: LambdaContext, context: LambdaContext):
    path = event.get("path")

    if path == "/generate/put":
        return presigned_put_url()
    elif path == "/generate/delete":
        return presigned_delete_url(event=event)
    else:
        return respond_error(
            status_code=constant.ERROR_BAD_REQUEST,
            message="Invalid path",
            data=None,
            success=False,
            errors=None
        )


def presigned_put_url():
    s3_client = boto3_s3_client()
    unique_id = generate_8digit_uuid()
    object_name = f"uploads/product/image/{unique_id}.jpg"
    response = s3_client.generate_presigned_url(
        ClientMethod='put_object',
        Params={
            'Bucket': variables.S3Bucket,
            'Key': object_name
        },
        ExpiresIn=variables.S3Expiration
    )
    data = {
        "image_url": f"https://{variables.S3Bucket}.s3.ap-south-1.amazonaws.com/{object_name}",
        "s3_presigned_url": response
    }
    return respond_success(
        data=data,
        success=True,
        message="Fetched url",
        status_code=200,
        warning=None
    )


def presigned_delete_url(event):
    body = helpers.load_json(event)

    # validate body
    data = S3Delete(**body)

    s3_client = boto3_s3_client()

    response = s3_client.generate_presigned_url(
        ClientMethod='delete_object',
        Params={
            'Bucket': variables.S3Bucket,
            'Key': data.name
        },
        ExpiresIn=variables.S3Expiration
    )

    data = {
        "delete_url": response
    }

    return respond_success(
        data=data,
        success=True,
        message="Delete URL generated successfully",
        status_code=200,
        warning=None
    )