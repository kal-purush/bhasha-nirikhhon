import asyncio

from celery_app.celery_config import celery_app
from db.with_db_session import with_db_session
from src.services.service_factory import service_factory


@celery_app.task(name="attendance_process_job")
def attendance_process_job():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(process_attendances())
    print("invoke attendance_process_job")


@with_db_session
async def process_attendances(db_session):
    attendance_process_service = service_factory().attendance_process_service(
        db_session=db_session
    )
    print("Async task starter!")
    await attendance_process_service.process_attendances()
    print("Async task completed!")