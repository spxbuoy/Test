import pymongo
import time
import logging
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError

# Configure MongoDB connection with proper options
CONNECTION_STRING = "mongodb+srv://amkushu999:kNK30QJ3WpscZ6H5@amkush.eqmhymy.mongodb.net/?retryWrites=true&w=majority"

# Connection pool settings
MONGO_OPTIONS = {
    "maxPoolSize": 10,              # Maximum number of connections in the pool
    "minPoolSize": 1,               # Minimum number of connections in the pool
    "maxIdleTimeMS": 30000,         # Max time a connection can be idle (30 seconds)
    "connectTimeoutMS": 5000,       # Connection timeout (5 seconds)
    "socketTimeoutMS": 30000,       # Socket timeout (30 seconds)
    "serverSelectionTimeoutMS": 5000, # Server selection timeout (5 seconds)
    "retryWrites": True,            # Retry failed write operations
    "retryReads": True,             # Retry failed read operations
    "waitQueueTimeoutMS": 10000     # How long to wait for a connection from the pool
}

# Maximum number of connection retries
MAX_RETRIES = 3

def get_mongodb_client():
    """
    Get a MongoDB client with connection pooling and automatic reconnection.
    Uses global client if available, or creates a new one.
    """
    global mongo_client
    
    # If we already have a client, return it
    if 'mongo_client' in globals():
        return mongo_client
        
    # Otherwise create a new client
    for attempt in range(MAX_RETRIES):
        try:
            client = pymongo.MongoClient(CONNECTION_STRING, **MONGO_OPTIONS)
            
            # Test the connection
            client.admin.command('ping')
            
            print("MongoDB connection established successfully ✅")
            # Store the client for reuse
            mongo_client = client
            return client
            
        except (ConnectionFailure, ServerSelectionTimeoutError) as e:
            if attempt < MAX_RETRIES - 1:
                print(f"MongoDB connection attempt {attempt + 1} failed. Retrying in 1 second...")
                time.sleep(1)
            else:
                print(f"MongoDB connection failed after {MAX_RETRIES} attempts: {str(e)} ❌")
                # Return None or raise the exception based on your error handling strategy
                raise

# Initialize the client
client = get_mongodb_client()

# Initialize collections with error handling
try:
    COLLECTIONS = client["CONFIG_DATABASE"] 
    BLACKLISTED_SKS = COLLECTIONS["BLACKLISTED_SKS"] 
    TOKEN_DB = COLLECTIONS["TOKEN_DB"] 
    SKS_DB = COLLECTIONS["SKS_DB"]
    CHANNELS_DB = COLLECTIONS["CHANNELS_DB"]
    
    # Create indexes for better performance
    # Index for CHANNELS_DB to optimize the lookup for left_channel=False
    CHANNELS_DB.create_index([("left_channel", pymongo.ASCENDING)])
    CHANNELS_DB.create_index([("join_time", pymongo.ASCENDING)])
    
    # Create a TTL index to automatically remove records of channels that have been left
    # This will remove entries 30 days after they're marked as left
    try:
        CHANNELS_DB.create_index(
            [("left_time", pymongo.ASCENDING)],
            expireAfterSeconds=30 * 24 * 60 * 60,  # 30 days
            background=True
        )
    except Exception as e:
        # TTL index creation is non-critical, we can log and continue
        logging.warning(f"Failed to create TTL index: {str(e)}")
        
except Exception as e:
    print(f"Failed to initialize database collections: {str(e)}")
    raise

# Helper function for database operations with automatic retry
def db_operation_with_retry(operation, max_retries=3):
    """
    Execute a database operation with automatic retry on connection errors.
    
    Args:
        operation: A function that performs the database operation
        max_retries: Maximum number of retry attempts
    
    Returns:
        The result of the operation
    """
    for attempt in range(max_retries):
        try:
            return operation()
        except (ConnectionFailure, ServerSelectionTimeoutError) as e:
            if attempt < max_retries - 1:
                print(f"Database operation failed, retry {attempt + 1}/{max_retries}: {str(e)}")
                time.sleep(0.5 * (attempt + 1))  # Exponential backoff
            else:
                raise
