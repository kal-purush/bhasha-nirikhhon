import datetime
import logging
import os
from uuid import uuid4
import data_base.db as database
import data_base.partitions as partitions
import components.security.user_session as user_session
from components.model import communication
from components.model.templates import template_notification, template_notification_requester


def add_user(user=None):
    """This component adds the record in the Cloudant (assessment) for CBTA assessment """

    if user is None:
        return [500, None]
    logging.info(msg="add_user- Start: ")

    try:

        # 1. Generate assessment ID (unique)
        user_id = str(uuid4())

        # 2. Add the assessment ID in the object
        user['user_id'] = user_id

        # 3. Add control data
        user['created_date'] = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M"))
#       assessment['created_by'] = user_session.get_user_session()['email']

        # 4. Add the object in the database
#        result = database.db_create(doc=assessment, partition=None)
        result = database.db_create(doc=user, partition="users")

        # 5. Return result
        if result['status'] is True:
            return [200, None]

    except Exception as e:
        logging.info(msg="Add_user exception: " + str(e))
    return [500, None]

def get_user_info(email=None):
    """Check if user is registered in db"""
    try:
        logging.info(msg="get_user_info - Start")

        #1. Get user email
       # user_email =

        if email:
            # 1. Select from database the records filtering by the user email
            selector = {
                "email": {"",
                }
            }

            # 3. Return the result
            return "true"

        # Missing input
        return "false"
    except Exception as e:
        logging.info(msg="get_user_info exception: " + str(e))
        return [500, None]