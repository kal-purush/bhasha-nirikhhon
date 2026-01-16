import ast
import json
import logging
import os
import yaml

import taskcluster as taskcluster_client
from mozci import TaskClusterManager
from mozci.utils.log_util import setup_logging

setup_logging(logging.INFO)
credentials = None

try:
    with open('credentials.json', 'r') as file:
        credentials = ast.literal_eval(file.read())
    tc = TaskClusterManager(credentials=credentials)
except IOError:
    tc = TaskClusterManager(web_auth=True)


with open(os.path.join('artifacts', 'action.yml')) as file:
    text = file.read()
    text = text.replace("{{decision_task_id}}", "MjLBUBpmS-Si-zwZocynVg")
    text = text.replace("{{task_labels}}", "TaskLabel==AL0295rhRE-g644nG-gXXw,TaskLabel==APFZRPfbQuGPVcqyUt_zLA")
    task = yaml.load(text)
text = json.dumps(task, indent=4, sort_keys=True)
# https://groups.google.com/d/msg/mozilla.tools.taskcluster/nkzBlX7BgrU/-IrWXPXCAQAJ
# This prevents us from scheduling a task which would show up on Treeherder

tc.schedule_task(task=json.loads(text))