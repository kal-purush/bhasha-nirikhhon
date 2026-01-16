import requests
import json
from services.enum import *
from services.utility import *


def load_class_booking_detail_info(student_id, scheduled_class_id):
    url = HOSTNAME + BookingServicesURLs.LoadClassBookingDetailInfo
    body_data = {
        "Member_id": student_id,
        "ScheduledClass_id": scheduled_class_id
    }

    response = requests.post(url=url, json=body_data, headers=headers, verify=False)
    assert response.status_code == StatusCode.Success, response.text
    json_response = json.loads(response.text)

    if json_response['Success'] == JSONReSponse.Success:
        print(json_response['ScheduledClassBookingDetailInfo'])
    else:
        print(json_response['ErrorMessage'])

load_class_booking_detail_info(11282834, '3031303')