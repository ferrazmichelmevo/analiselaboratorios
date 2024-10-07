import os
from dotenv import load_dotenv

load_dotenv()

config = {
    "AWS_ACCESS_KEY_ID": os.getenv('AWS_ACCESS_KEY_ID'),
    'AWS_REGION': os.getenv('AWS_REGION'),
    'AWS_SECRET_ACCESS_KEY': os.getenv('AWS_SECRET_ACCESS_KEY')
}