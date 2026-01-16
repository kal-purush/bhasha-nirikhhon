from math import dist
import models
from services.check_in_accounts import CheckInAccountService
from utils.check_in_crawlers import CheckInCrawler

from motor.motor_asyncio import AsyncIOMotorClient
from config import DatabaseConfig
import asyncio
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

client = AsyncIOMotorClient(DatabaseConfig.MONGODB_URL)
db = client[DatabaseConfig.DATABASE_NAME]
async def connect_to_mongo():
    logger.info("Attempting to connect to MongoDB...")
    try:
        await db.command("serverStatus")
        logger.info("Successfully connected to MongoDB!")
    except Exception as e:
        logger.error(f"Error connecting to MongoDB: {e}")
        await close_mongo_connection()

async def close_mongo_connection():
    client.close()
    logger.info("MongoDB connection closed.")

async def main():
    logger.info("Script started!")
    await connect_to_mongo()
    current_time = datetime.now()

    hours = current_time.hour
    minutes = current_time.minute

    formatted_time = f"{hours:02d}:{minutes:02d}"
    collection = db.check_in_accounts
    logger.info("Fetching check-in accounts...")
    check_in_accounts = await collection.find({"check_out_time":formatted_time}).to_list(length=1000)
    logger.info(f"Found {len(check_in_accounts)} check-in accounts.")
    no_check_in_holidays = [
        datetime(2024, 1, 1).date(),
        datetime(2024, 2, 8).date(),
        datetime(2024, 2, 9).date(),
        datetime(2024, 2, 12).date(),
        datetime(2024, 2, 13).date(),
        datetime(2024, 2, 14).date(),
        datetime(2024, 2, 28).date(),
        datetime(2024, 4, 4).date(),
        datetime(2024, 4, 5).date(),
        datetime(2024, 6, 10).date(),
        datetime(2024, 9, 17).date(),
        datetime(2024, 10, 10).date(),
    ]
    holiday_check_in_days=[
        datetime(2024, 2, 17).date(),
        
    ]
    current_date = datetime.now().date()
    if current_date.weekday() <= 4 or current_date in holiday_check_in_days:
        check_in(check_in_accounts)
    elif current_date in no_check_in_holidays:
        check_in(check_in_accounts)

    # check_in_crawler = CheckInCrawler()
    
    
    # for idx, check_in_account in enumerate(check_in_accounts):
    #     logger.info(f"Processing account {idx+1}...")
    #     check_in_crawler.check_in(check_in_account)
    
    await close_mongo_connection()
    logger.info("Script finished successfully!")

def check_in(check_in_accounts:dist):
    check_in_crawler = CheckInCrawler()
    for idx, check_in_account in enumerate(check_in_accounts):
        logger.info(f"Processing account {idx+1}...")
        check_in_crawler.check_in(check_in_account)

if __name__ == "__main__":
    asyncio.run(main())