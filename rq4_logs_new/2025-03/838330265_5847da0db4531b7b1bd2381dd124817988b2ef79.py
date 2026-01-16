# checker.py
import asyncio
from datetime import datetime, timedelta
from sqlalchemy import and_
from db.database import FlatRecord, get_db, MonitoringTask
from core.rabbitmq_client import RabbitMQClient
from core.config import settings

async def check_and_send_flats():
    db = next(get_db())
    rabbit_client = await RabbitMQClient().connect()
    
    # Find users who haven't received updates in DEFAULT_LAST_MINUTES_GETTING
    time_threshold = datetime.now() - timedelta(minutes=settings.DEFAULT_LAST_MINUTES_GETTING)
    tasks = db.query(MonitoringTask).filter(
        MonitoringTask.last_updated < time_threshold
    ).all()
    
    for task in tasks:
        # Find flats for this user's URL that they haven't seen yet
        flats_query = db.query(FlatRecord).filter(
            FlatRecord.flat_url.like(f"%{task.url}%")
        )
        
        # If they have a last_got_flat timestamp, only send flats newer than that
        if task.last_got_flat:
            flats_to_send = flats_query.filter(
                FlatRecord.first_seen > task.last_got_flat
            ).all()
        else:
            # Otherwise, send flats from the last DEFAULT_LAST_MINUTES_GETTING
            flats_to_send = flats_query.filter(
                FlatRecord.first_seen > time_threshold
            ).all()
        
        if flats_to_send:
            # Send flats to RabbitMQ for the Telegram service to handle
            await rabbit_client.send_message(
                exchange="flats",
                body={
                    "type": "SEND_FLATS",
                    "chat_id": task.chat_id,
                    "flats": [
                        {
                            "title": flat.title,
                            "price": flat.price,
                            "location": flat.location,
                            "created_at": flat.created_at.strftime("%H:%M"),
                            "image_url": flat.image_url,
                            "flat_url": flat.flat_url,
                            "description": flat.description
                        }
                        for flat in flats_to_send
                    ]
                }
            )
            
            # Update the last_updated and last_got_flat timestamps
            task.last_updated = datetime.now()
            task.last_got_flat = datetime.now()
            db.commit()
    
    db.close()
    await rabbit_client.close_connection()