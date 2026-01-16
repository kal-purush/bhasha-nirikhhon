from pprint import pprint
import botocore
import boto3

con_accountA = boto3.session.Session(profile_name='profile_name')
svc_con_accountA = con_accountA.client(service_name='s3', region_name='region')

con_accountB = boto3.session.Session(profile_name='profile_name')
svc_con_accountB = con_accountB.client(service_name='s3', region_name='region')

object_key = svc_con_accountB.get_paginator('list_objects_v2')
for object in object_key.paginate(Bucket='s3_bucket', Prefix='dirs_in_s3_bucket'):
    all_objects = object['Contents']
    for n in all_objects:
        keys = n['Key']

    try:
        response = svc_con_accountB.get_object_acl(Bucket='s3_bucket', Key=keys)
        print("This is response")
        pprint(response)
    except botocore.exceptions.ClientError as error:
        if error.response['Error']['Code'] == 'AccessDenied':
            #Setting object_acl for object owned by Account A (owner account) & accessed by Account B (grantee account)
            object_acl = svc_con_accountA.put_object_acl(Bucket='s3_bucket', AccessControlPolicy={
            'Grants': [
                {
                    'Grantee': {
                        'DisplayName': 'aws_grantee_account_id',
                        'ID': 'CanonicalId',
                        'Type': 'CanonicalUser'
                    },
                    'Permission': 'FULL_CONTROL'
                },
            ],
            'Owner': {
                'DisplayName': 'aws_owner_account_id',
                'ID': 'CanonicalId'
            }
            }, Key=keys)
            print("After setting ACL:")
            pprint(object_acl)
        else:
            print("Unexpected error: %s" % error)