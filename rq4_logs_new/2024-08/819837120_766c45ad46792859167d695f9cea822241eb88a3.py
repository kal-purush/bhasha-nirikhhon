import logging
import os
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

cloudwatch_client = boto3.client('cloudwatch')
lambda_client = boto3.client('lambda')

def create_metric_alarm(alarm_name, alarm_description, alarm_actions, eval_periods, threshold, comparison_op, metrics):
  try:
    alarm = cloudwatch_client.put_metric_alarm(
      AlarmName=alarm_name,
      AlarmDescription=alarm_description,
      AlarmActions=alarm_actions,
      Threshold=threshold,
      DatapointsToAlarm=1,
      EvaluationPeriods=eval_periods,
      ComparisonOperator=comparison_op,
      Metrics=metrics
    )
    logger.info('Added alarm %s to track metric', alarm_name)
    return alarm
  except ClientError:
    logger.exception("Couldn't add alarm %s to metric", alarm_name)
    raise

def delete_metric_alarms(alarm_names):
  try:
    cloudwatch_client.delete_alarms(AlarmNames=alarm_names)
    logger.info('Deleted alarms %s', alarm_names)
  except ClientError:
    logger.exception("Couldn't delete alarms for metric %s", alarm_names)
    raise

def get_event_detail(event, key):
  try:
    value = event['detail'][key]
    logger.info('%s: %s', key, value)
    return value
  except KeyError as e:
    logger.error('Key %s not found in event detail: %s', key, e)
    raise

def get_resource_info(event, index, split_index):
  try:
    value = event['resources'][index].split('/')[split_index]
    logger.info('Resource info at index %d, split index %d: %s', index, split_index, value)
    return value
  except (IndexError, KeyError) as e:
    logger.error('Invalid index or key for resource info extraction: %s', e)
    raise

def add_lambda_permission(function_name, region, account_id, alarm_name, task_id):
  try:
    alarm_arn = f'arn:aws:cloudwatch:{region}:{account_id}:alarm:{alarm_name}'
    res = lambda_client.add_permission(
      FunctionName=function_name,
      Action='lambda:InvokeFunction',
      StatementId=f'alarm_lambda_{task_id}',
      Principal='lambda.alarms.cloudwatch.amazonaws.com',
      SourceArn=alarm_arn
    )
    logger.info('Added lambda permission: %s', res)
  except ClientError as e:
    logger.exception("Couldn't add permission for alarm %s: %s", alarm_name, e)
    raise

def remove_lambda_permission(function_name, task_id):
  try:
    res = lambda_client.remove_permission(
      FunctionName=function_name,
      StatementId=f'alarm_lambda_{task_id}'
    )
    logger.info('Removed lambda permission: %s', res)
  except ClientError as e:
    logger.exception("Couldn't remove permission for alarm %s: %s", task_id, e)
    raise

def create_task_alarm(event):
  task_id = get_resource_info(event, 0, -1)
  cluster_name = get_resource_info(event, 0, -2)
  service_name = get_event_detail(event, 'group').split(':')[-1]

  alarm_name = f'{cluster_name}-{service_name}-{task_id}-alarm'
  logger.info('Alarm name: %s', alarm_name)

  function_name = os.getenv('ALARM_FUNCTION_NAME')
  region = os.getenv('REGION')
  account_id = os.getenv('ACCOUNT_ID')
  eval_periods = int(os.getenv('EVAL_PERIODS'))
  threshold = int(os.getenv('THRESHOLD'))
  cpu_threshold = os.getenv('CPU_THRESHOLD')
  memory_threshold = os.getenv('MEMORY_THRESHOLD')

  alarm_actions = [f'arn:aws:lambda:{region}:{account_id}:function:{function_name}']
  alarm_description = 'Alerm when ECS Task CPU and memory exceed the threshold'
  comparison_operator = 'GreaterThanOrEqualToThreshold'
  metrics = [
    {
      'Id': 'reboot_alarm',
      'Label': 'reboot_alarm',
      'ReturnData': True,
      'Expression': f'(cpu_usage > {cpu_threshold}) && (memory_usage > {memory_threshold})'
    },
    {
      'Id': 'cpu_usage',
      'Label': 'cpu_usage',
      'ReturnData': False,
      'Expression': '(cpu_utilized * 100) / cpu_reserved'
    },
    {
      'Id': 'memory_usage',
      'Label': 'memory_usage',
      'ReturnData': False,
      'Expression': '(memory_utilized * 100) / memory_reserved'
    },
    {
      'Id': 'cpu_utilized',
      'Label': 'cpu_utilized',
      'ReturnData': False,
      'MetricStat': {
        'Metric': {
          'Namespace': cluster_name,
          'MetricName': 'CpuUtilized',
          'Dimensions': [{'Name': 'TaskId', 'Value': task_id}]
        },
        'Stat': 'Average',
        'Period': 60
      }
    },
    {
      'Id': 'cpu_reserved',
      'Label': 'cpu_reserved',
      'ReturnData': False,
      'MetricStat': {
        'Metric': {
          'Namespace': cluster_name,
          'MetricName': 'CpuReserved',
          'Dimensions': [{'Name': 'TaskId', 'Value': task_id}]
        },
        'Stat': 'Average',
        'Period': 60
      }
    },
    {
      'Id': 'memory_utilized',
      'Label': 'memory_utilized',
      'ReturnData': False,
      'MetricStat': {
        'Metric': {
          'Namespace': cluster_name,
          'MetricName': 'MemoryUtilized',
          'Dimensions': [{'Name': 'TaskId', 'Value': task_id}]
        },
        'Stat': 'Average',
        'Period': 60
      }
    },
    {
      'Id': 'memory_reserved',
      'Label': 'memory_reserved',
      'ReturnData': False,
      'MetricStat': {
        'Metric': {
          'Namespace': cluster_name,
          'MetricName': 'MemoryReserved',
          'Dimensions': [{'Name': 'TaskId', 'Value': task_id}]
        },
        'Stat': 'Average',
        'Period': 60
      }
    }
  ]

  create_metric_alarm(alarm_name, alarm_description, alarm_actions, eval_periods, threshold, comparison_operator, metrics)
  add_lambda_permission(function_name, region, account_id, alarm_name, task_id)

def delete_task_alarm(event):
  task_id = get_resource_info(event, 0, -1)
  cluster_name = get_resource_info(event, 0, -2)
  service_name = get_event_detail(event, 'group').split(':')[-1]

  alarm_name = f'{cluster_name}-{service_name}-{task_id}-alarm'
  delete_metric_alarms([alarm_name])
  function_name = os.getenv('ALARM_FUNCTION_NAME')
  remove_lambda_permission(function_name, task_id)

def lambda_handler(event, context):
  logger.info('Event: %s', event)
  status = get_event_detail(event, 'lastStatus')
  if status == 'PROVISIONING':
    create_task_alarm(event)
  elif status == 'DEPROVISIONING':
    delete_task_alarm(event)
  logger.info("=== END ===")