import motor.motor_asyncio

from config import DATABASE

client = motor.motor_asyncio.AsyncIOMotorClient(DATABASE)
db = client.salary_data_aggregator_db
collection = db.sample_collection
