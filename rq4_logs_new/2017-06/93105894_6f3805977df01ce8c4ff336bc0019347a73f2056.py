"""
Lambda wrapper for WPSeku
"""

import logging
from subprocess import call


logger = logging.getLogger()
logger.setLevel(logging.INFO)


def handle(event, context):
    """
    Lambda handler
    """
    target = json.loads(event["target"])
    logger.info("%s - %s", event, context)
    #subprocess.call(args, *, stdin=None, stdout=None, stderr=None, shell=False)
    call(["python", "wpseku.py", "-t ", target])

    return event