# encoding=utf-8

import photos
import random


class Subject():
    visit_notify = None
    password_reseted = True
    birthday = "null"
    subject_id = "111"
    title = ""
    subject_type = 0
    job_number = 123
    name = "敖翔"
    come_from = ""
    purpose = 0
    id = 6997
    company_id = 1
    department = "Cloud Service"
    email = "aoxiang@megvii.com"
    gender = 0
    phone = "18621736951"
    pinyin = "aoxiang"
    interviewee = ""
    avatar = "https://o7rv4xhdy.qnssl.com/@/static/upload/avatar/2015-12-02/aab9d380766e5d6b5d103f4e81e4f0ab7f63d556.jpg"
    photos = photos
    start_time = 0
    end_time = 0
    entry_date = "null"
    remark = ""
    description = ""

    @classmethod
    def get_json(self):
	    info = []
	    for i in range(100):
		    info.append( {
			    "visit_notify": self.visit_notify,
			    "password_reseted": random.choice([True, False]),
			    "birthday": self.birthday,
			    "title": self.subject_id,
			    "subject_type": random.randint(0,1),
			    "job_number": random.randint(1,1000),
			    "name": ''.join(random.sample('abcdefghik', 10)),
			    "come_from": self.come_from,
			    "purpose": self.purpose,
			    "id": self.id,
			    "company_id": self.company_id,
			    "department": self.department,
			    "email": self.email,
			    "gender": self.gender,
			    "phone": self.phone,
			    "pinyin": self.pinyin,
			    "interviewee": self.interviewee,
			    "avatar": self.avatar,
			    "photos": [
				    {
					    "version": photos.photos.version,
					    "id": photos.photos.id,
					    "quality": photos.photos.quality,
					    "subject_id": photos.photos.subject_id,
					    "company_id": photos.photos.company_id,
					    "url": photos.photos.url
				    }
			    ],
			    "start_time": 0,
			    "end_time": 0,
			    "entry_date": "null",
			    "remark": "",
			    "description": ""
		    })
	    return info


