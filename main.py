from pyrogram.client import Client
import json
import asyncio
from FUNC.server_stats import *
from FUNC.scraperfunc import channel_cleanup_background

plugins = dict(root="BOT")

with open("FILES/config.json", "r", encoding="utf-8") as f:
    DATA      = json.load(f)
    API_ID    = DATA["API_ID"]
    API_HASH  = DATA["API_HASH"]
    BOT_TOKEN = DATA["BOT_TOKEN"]

user = Client( 
            "Scrapper", 
             api_id   = API_ID, 
             api_hash = API_HASH
              )

# Generate a unique session name based on the token to avoid session conflicts when token changes
import hashlib
session_name = f"bot_{hashlib.md5(BOT_TOKEN.encode()).hexdigest()[:8]}"

bot = Client(
    session_name,  # Dynamic session name based on token
    api_id    = API_ID, 
    api_hash  = API_HASH, 
    bot_token = BOT_TOKEN, 
    plugins   = plugins 
)



async def start_background_tasks():
    """Start background tasks for channel cleanup"""
    asyncio.create_task(channel_cleanup_background())
    print("Background channel cleanup task started")

def clean_old_sessions():
    """Remove old session files to prevent conflicts"""
    import os
    import glob
    
    # Find all .session files in the current directory
    session_files = glob.glob("*.session")
    current_session = f"{session_name}.session"
    
    # Keep the current session file, remove others with 'bot_' prefix
    for file in session_files:
        if file.startswith("bot_") and file != current_session:
            try:
                os.remove(file)
                print(f"Removed old session file: {file}")
            except Exception as e:
                print(f"Failed to remove {file}: {e}")
                
    # Also remove MY_BOT.session if it exists (from previous versions)
    if os.path.exists("MY_BOT.session"):
        try:
            os.remove("MY_BOT.session")
            print("Removed legacy session file: MY_BOT.session")
        except Exception as e:
            print(f"Failed to remove MY_BOT.session: {e}")

if __name__ == "__main__":
    # Clean up old session files
    clean_old_sessions()
    
    # send_server_alert()
    print("Done Bot Active ✅")
    print(f"Using session name: {session_name}")
    print("NOW START YOUR BOT")
    
    # Start background tasks
    loop = asyncio.get_event_loop()
    loop.create_task(start_background_tasks())
    
    # Run the bot
    bot.run()
