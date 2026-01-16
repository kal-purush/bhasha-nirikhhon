import json
import os
from typing import TYPE_CHECKING, Any

import boto3
from aws_lambda_powertools.utilities import parameters
from aws_lambda_powertools.utilities.typing import LambdaContext
from psycopg2 import Error, connect, sql

# ----- Environment Variables -----
rds_fdw_host = os.environ["RDS_FDW_HOST"]
rds_fdw_db = os.environ["RDS_FDW_DB"]
rds_resource_id = os.environ["RDS_FDW_RESOURCE_ID"]

rds_fdw_root_secret: Any = parameters.get_secret(os.environ["RDS_FDW_ROOT"])

rds_fdw_root_secret_key_value = json.loads(rds_fdw_root_secret)
rds_fdw_root_user = rds_fdw_root_secret_key_value["username"]
rds_fdw_root_pw = rds_fdw_root_secret_key_value["password"]


if TYPE_CHECKING:
    from mypy_boto3_iam import IAMClient
    from mypy_boto3_sts import STSClient

else:
    IAMClient = STSClient = dict

boto3_iam_client: IAMClient = boto3.client("iam")
boto3_sts_client: STSClient = boto3.client("sts")


def create_rds_user_from_iam(username: str) -> None:
    conn = connect(
        host=rds_fdw_host,
        database=rds_fdw_db,
        user=rds_fdw_root_user,
        password=rds_fdw_root_pw,
    )

    sql_create_user = sql.SQL("CREATE ROLE {username} WITH LOGIN").format(
        username=sql.Identifier(username),
    )
    sql_user_create_schema = sql.SQL("CREATE SCHEMA {username}").format(
        username=sql.Identifier(username),
    )
    sql_user_grant_schema_usage = sql.SQL("GRANT USAGE ON SCHEMA {username} TO {username}").format(
        username=sql.Identifier(username),
    )
    sql_user_grant_schema_privileges = sql.SQL(
        "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA {username} TO {username}"
    ).format(
        username=sql.Identifier(username),
    )
    sql_user_grant_schema_execute = sql.SQL("GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA {username} TO {username}").format(
        username=sql.Identifier(username),
    )
    sql_grant_iam_role = sql.SQL("GRANT rds_iam TO {username}").format(
        username=sql.Identifier(username),
    )

    try:
        with conn.cursor() as cur:
            try:
                cur.execute(sql_create_user)
                cur.execute(sql_user_create_schema)
                cur.execute(sql_user_grant_schema_usage)
                cur.execute(sql_user_grant_schema_privileges)
                cur.execute(sql_user_grant_schema_execute)
                cur.execute(sql_grant_iam_role)

            except Error:
                conn.rollback()
                raise

            conn.commit()

    finally:
        conn.close()


def ensure_iam_user_exists(username: str, iam_policy_arn: str) -> None:
    try:
        boto3_iam_client.get_user(UserName=username)
    except boto3_iam_client.exceptions.NoSuchEntityException:
        boto3_iam_client.create_user(
            UserName=username,
        )
    boto3_iam_client.attach_user_policy(UserName=username, PolicyArn=iam_policy_arn)
    boto3_iam_client.tag_user(UserName=username, Tags=[{"Key": "BDE_Analytics_User", "Value": "True"}])


def generate_iam_user_policy(username: str) -> str:
    # Resource arn needs to be specific to a particular user to prevent individuals from connecting as another user.
    # https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.IAMDBAuth.IAMPolicy.html
    aws_account_id = boto3_sts_client.get_caller_identity()["Account"]
    resource_arn = f"arn:aws:rds-db:ap-southeast-2:{aws_account_id}:dbuser:{rds_resource_id}/{username}"

    iam_user_policy_document = {
        "Version": "2012-10-17",
        "Statement": [{"Action": "rds-db:connect", "Resource": resource_arn, "Effect": "Allow"}],
    }

    response = boto3_iam_client.create_policy(
        PolicyName=f"bde-analytics-iam-policy-{username}",
        Path="/bde-analytics-policies/",
        PolicyDocument=json.dumps(iam_user_policy_document),
        Description="IAM policy allowing user access to bde analytics.",
    )

    return response["Policy"]["Arn"]


def handler(event: dict[str, str], _context: LambdaContext) -> None:
    iam_policy_arn = generate_iam_user_policy(username=event["username"])
    ensure_iam_user_exists(username=event["username"], iam_policy_arn=iam_policy_arn)

    create_rds_user_from_iam(username=event["username"])