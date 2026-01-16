import base64
from concurrent import futures
import json
import uuid
import os
import tempfile

from base.lambda_handler_base import APILambdaHandlerBase
from base.api_exceptions import BadRequestException, UnauthorizedException
from run_comparison import compare_outlines

BUCKET_NAME = "aseaman-public-bucket"
JASPER_PREFIX = "aseaman/images/jasper"
TABLE_NAME = 'whisky'
LOCAL_TABLE_NAME = 'whisky_local'

max_workers = 5

class DrawJasperLambdaHandler(APILambdaHandlerBase):
    require_auth = True

    def _init(self):
        self.id = uuid.uuid4().hex
        self.table_name = LOCAL_TABLE_NAME if self.is_local else TABLE_NAME

    def _init_aws(self):
        self.ddb_client = self.aws_session.client('dynamodb', region_name='us-east-1')
        self.s3_client = self.aws_session.client('s3', region_name='us-east-1')

    def _parse_payload(self, payload):
        # return
        if not payload.get('img'):
            raise BadRequestException('img parameter required')
        self.imgBytes = base64.b64decode(payload['img'].split(',')[1])

    def _download(self, key):
        tmpdir = tempfile.gettempdir()
        filename = key.split('/')[-1]
        local_filename = os.path.join(tmpdir, filename)
        with open(local_filename, 'wb') as data:
            self.s3_client.download_fileobj(BUCKET_NAME, key, data)
        return local_filename

    def _download_all_masks(self):
        mask_response = self.s3_client.list_objects_v2(
            Bucket=BUCKET_NAME,
            MaxKeys=50,
            Prefix='{}/data/masks/'.format(JASPER_PREFIX),
        )

        mask_keys = [file['Key'] for file in mask_response['Contents'] if file['Key'].endswith('.png')]

        with futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_key = {executor.submit(self._download, key): key.split('/')[-1].split('.')[0] for key in mask_keys}

            for future in futures.as_completed(future_to_key):
                key = future_to_key[future]
                exception = future.exception()
                if not exception:
                    yield key, future.result()
                else:
                    yield key, exception

    def _save_image_to_s3(self):
        filename = '{}.png'.format(self.id)
        tmpdir = tempfile.gettempdir()
        local_filename = os.path.join(tmpdir, filename)
        with open(local_filename, "wb") as fh:
            fh.write(self.imgBytes)

        self.s3_client.put_object(
            Body=self.imgBytes,
            Bucket=BUCKET_NAME,
            Key='aseaman/images/jasper/{}/input_drawing.png'.format(self.id)
        )

        return local_filename

    def _run(self):
        # user_drawing_filename = '/tmp/input_drawing.png'
        user_drawing_filename = self._save_image_to_s3()
        results = []
        for (key, filepath) in self._download_all_masks():
            res, nzc = compare_outlines(user_drawing_filename, filepath)
            results.append((nzc, key))

        result = {
            'results': sorted(results),
        }

        return {
            "isBase64Encoded": False,
            "statusCode": 200,
            "headers": {
                "Access-Control-Allow-Origin": "*"
            },
            "multiValueHeaders": {},
            "body": json.dumps(result)
        }


def lambda_handler(event, context):
    return DrawJasperLambdaHandler(event, context).handle()