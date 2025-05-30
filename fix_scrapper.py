#!/usr/bin/env python3
import re

# Read the file
with open("BOT/tools/scrapper.py", "r") as f:
    content = f.read()

# Define the replacement pattern
old_pattern = r'''        try:
            await user\.start\(\)
        except:
            pass'''

new_replacement = '''        # Get a properly initialized user client
        user = await get_scraper_client()
        if not user:
            await message.reply_text("<b>Error ❌\\n\\nCould not initialize scraper client. Please contact admin.</b>", message.id)
            return
            
        # Start the client with proper error handling
        try:
            if not hasattr(user, 'is_connected') or not user.is_connected:
                await user.start()
                print("Started user client for scraping")
        except Exception as e:
            await error_log(f"Failed to start scraper client: {str(e)}")
            await message.reply_text(f"<b>Error ❌\\n\\nFailed to start scraper: {str(e)}</b>", message.id)
            return'''

# Replace all occurrences
modified_content = re.sub(old_pattern, new_replacement, content)

# Write back to the file
with open("BOT/tools/scrapper.py", "w") as f:
    f.write(modified_content)

print("Fixed all user client initializations in scrapper.py")
