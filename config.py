from dotenv import load_dotenv
import os

load_dotenv()

DATABASE = os.environ.get("DB")
TOKEN = os.environ.get("TOKEN")
