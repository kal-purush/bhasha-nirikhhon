import os
import aioboto3
from utils.lambda_decorators import ssm_parameters, async_handler
import time
from timeit import default_timer as timer
from dns_ingestor.scheduler import Scheduler
from dns_ingestor.scan_plan_writer import PlannedScanDbWriter
from dns_ingestor.record_resolver import RecordResolver
from dns_ingestor.ingestor import DnsZoneIngestor
from collections import namedtuple
from asyncio import gather, run

region = os.environ["REGION"]
stage = os.environ["STAGE"]
app_name = os.environ["APP_NAME"]

ssm_client = aioboto3.client("ssm", region_name=region)
sts_client = aioboto3.client("sts", region_name=region)
dynamo_resource = aioboto3.resource("dynamodb", region_name=region)


# ssm params
ssm_prefix = f"/{app_name}/{stage}"
ROUTE53_ROLE = f"{ssm_prefix}/scheduler/route53/role/arn"
SCAN_PLAN_TABLE = f"{ssm_prefix}/scheduler/dynamodb/id"
PLANNING_PERIOD_SECONDS = f"{ssm_prefix}/scheduler/config/period"
PLANNING_BUCKETS = f"{ssm_prefix}/scheduler/config/buckets"
LOG_UNHANDLED = f"{ssm_prefix}/scheduler/config/log_unhandled"


async def _get_route53_client(role):
    route53_role = await sts_client.assume_role(RoleArn=role, RoleSessionName="ScanPlanSession")
    assumed_creds = route53_role["Credentials"]
    return aioboto3.client(
        "route53",
        aws_access_key_id=assumed_creds['AccessKeyId'],
        aws_secret_access_key=assumed_creds['SecretAccessKey'],
        aws_session_token=assumed_creds['SessionToken']
    )


@ssm_parameters(
    ssm_client,
    ROUTE53_ROLE,
    SCAN_PLAN_TABLE,
    PLANNING_PERIOD_SECONDS,
    PLANNING_BUCKETS,
    LOG_UNHANDLED
)
@async_handler
async def ingest_dns(event, _):
    params = event['ssm_params']
    start = timer()
    ingest_time = time.time()
    log_unhandled = params[LOG_UNHANDLED].lower() == "true"

    # used to obtain the dns zone records
    route53_client = await _get_route53_client(params[ROUTE53_ROLE])

    # gathering futures for the per record writes to the db
    # we await these before exiting
    db_writes = []

    try:
        # first create the ingestor and get it to load the list of zones
        ingestor = DnsZoneIngestor(route53_client)
        await ingestor.ingest_zones()

        # the list of known zones is needed by the record resolver
        # which resolves a record into a list of IP addresses
        record_resolver = RecordResolver(ingestor.known_zones, log_unhandled)

        # splits the day into several buckets, each scan is placed in one of those
        schedule = Scheduler(
            ingest_time,
            int(params[PLANNING_PERIOD_SECONDS]),
            int(params[PLANNING_BUCKETS])
        )

        # writes the planned scans to the dynamodb
        scan_plan_writer = PlannedScanDbWriter(
            dynamo_resource.Table(params[SCAN_PLAN_TABLE]),
            ingest_time,
            schedule
        )

        # A bit of glue code that acts as the record consumer for the ingestor
        # resolves the IPs from records and then submits them to the plan writer
        async def scan_planner(record):
            hosts_to_scan = await record_resolver.resolve(record)

            # gathering dispatches the writes which can be awaited later
            writes = [scan_plan_writer.write(host) for host in hosts_to_scan]
            gathered_writes = gather(*writes)
            db_writes.append(gathered_writes)

        await ingestor.ingest_records(scan_planner)
    finally:
        # ensure route53 connection is shut down and all writes are written
        await gather(
            route53_client.close(),
            *db_writes
        )

    end = timer()
    print(f"Ingested all zones in {end-start}s, resolved {record_resolver.ips_resolved} IPs (may contain duplicates)")


# For developer test use only
if __name__ == "__main__":
    async def _clean_clients():
        return await gather(
            ssm_client.close(),
            sts_client.close(),
            dynamo_resource.close()
        )
    try:
        ingest_dns({}, namedtuple("context", ["loop"]))
    finally:
        run(_clean_clients())