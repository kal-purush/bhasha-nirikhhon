#!/usr/bin/env python
try:
    from .transfer_ujs_njs import MigrateDatabases
    from execution_engine2.db.models.models import Status, valid_status
except:
    from transfer_ujs_njs import MigrateDatabases
    from models import *
from bson import ObjectId

from pprint import pprint

njs_jobs_db = MigrateDatabases().njs_jobs
ujs_jobs_db = MigrateDatabases().ujs_jobs

from collections import defaultdict

count = 0
missing_ujs = []
c = defaultdict(int)
for job in njs_jobs_db.find():
    job_id = job["ujs_job_id"]
    c[job_id] += 1
    count += 1
    ujs_job = ujs_jobs_db.find_one({"_id": ObjectId(job_id)})
    if not ujs_job:
        print(f"Couldn't find {job_id}, ")
        missing_ujs.append(job_id)


print("Max occurences", max(c.values()))

print("Number of njs jobs", count)
print("Number of ujs jobs found", count - len(missing_ujs))
print("Number of missing ujs jobs", len(missing_ujs))