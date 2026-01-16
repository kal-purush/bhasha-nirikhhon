import json

import requests

from services.enum import *
from services.utility import *


def load_scheduled_class_booking_info_list(member_id):
    url = HOSTNAME + BookingServicesURLs.LoadScheduledClassBookingInfoList
    body_data = {
        "Member_id": member_id,
        "School_id": 36,
        "SelectedUtcDate": "2017-04-07 16:00:00"
    }
    response = requests.post(url=url, json=body_data, headers=headers, verify=False)
    if response.status_code == StatusCode.Success:
        response_data = json.loads(response.content)

        print(response_data['StudentClassBookingInfoList'])
load_scheduled_class_booking_info_list(11270970)
