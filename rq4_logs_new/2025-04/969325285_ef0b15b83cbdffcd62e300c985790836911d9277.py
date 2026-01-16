import os  # Import the 'os' module to interact with the environment variables
from pymongo import MongoClient  # Import the MongoDB client to connect to MongoDB
from dotenv import load_dotenv  # Import the 'load_dotenv' function to load variables from the .env file

# Load the environment variables from the .env file
load_dotenv()

# Get the MongoDB URI from environment variables
MONGO_URI = os.getenv("MONGODB_URI")

# If the environment variable isn't found, raise an error
if not MONGO_URI:
    raise ValueError("MONGODB_URI not set in environment variables.")

# Create the MongoDB connection using the URI
conn = MongoClient(MONGO_URI)