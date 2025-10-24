from os import environ

API_ID = int(environ.get("API_ID", "27353035"))
API_HASH = environ.get("API_HASH", "cf2a75861140ceb746c7796e07cbde9e")
BOT_TOKEN = environ.get("BOT_TOKEN", "5843226951:AAEi-4rmnqU3gQ86Rv0hcuBFLUpdUGRhRcE")

# Make Bot Admin In Log Channel With Full Rights
LOG_CHANNEL = int(environ.get("LOG_CHANNEL", "-1002073865889"))
ADMINS = int(environ.get("ADMINS", "1327021082"))

# TinyDB will store data in a local JSON file.
DB_FILE = environ.get("DB_FILE", "tinydb_data.json")

