import boto3
from lifestylesite.settings import AWS_REGION, AWS_S3_BUCKET, AWS_BUCKET_ROOT_URI
from datetime import datetime
import random
from rest_framework.views import APIView
from rest_framework.response import Response


# generate a presigned upload url to s3
def generatepresigneduploadforimageurl():
    numberkey = random.randint(10000000000,1000000000000000)
    timekey = datetime.now().strftime('%m%d%Y%H%M%S%f')
    key = str(numberkey) + timekey
    fields = {
        'acl': 'public-read',
        'content-type': 'image/*'
    }
    conditions = [{'acl': 'public-read'},
                  {'content-type':'image/*'}]
    s3 = boto3.client('s3')
    post = s3.generate_presigned_post(
        Bucket=AWS_S3_BUCKET,
        Key= key,
        Fields=fields,
        Conditions = conditions
    )
    return ({
        'url': post['url'],
        'fields': post['fields'],
        'uriroot': AWS_BUCKET_ROOT_URI

    })


class GeneratePresignedUrl(APIView):

    def get(self, request, *args, **kwargs):
        type = kwargs.get('type')
        if type == 'image':
            response = generatepresigneduploadforimageurl()
            return Response(response)