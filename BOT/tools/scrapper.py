import json
import asyncio
from pyrogram.client import Client
from pyrogram import filters
from FUNC.defs import *
from FUNC.usersdb_func import *
from TOOLS.check_all_func import *
from FUNC.scraperfunc import *


with open("FILES/config.json", "r",encoding="utf-8") as f:
    DATA          = json.load(f)
    API_ID        = DATA["API_ID"]
    API_HASH      = DATA["API_HASH"]
    SESSION_STRING = DATA.get("SESSION_STRING")

# Define a better method to get user client
async def get_scraper_client():
    """Get a properly initialized user client for scraping"""
    from FUNC.defs import error_log
    
    # Create a fresh client without using session string to avoid unpacking errors
    try:
        print("Creating scraper client without session string")
        client = Client(
            "Scrapper",
            api_id=API_ID,
            api_hash=API_HASH,
            no_updates=True  # Reduce overhead for scraping client
        )
        return client
    except Exception as e:
        await error_log(f"Failed to create basic scraper client: {str(e)}")
        
        # Try a different approach as last resort
        try:
            print("Trying alternate client creation method")
            import tempfile
            # Create a new random session file to avoid conflicts
            temp_session = tempfile.NamedTemporaryFile(prefix="scraper_", suffix=".session", delete=False).name
            
            client = Client(
                temp_session,
                api_id=API_ID,
                api_hash=API_HASH
            )
            return client
        except Exception as e2:
            await error_log(f"All client creation methods failed: {str(e2)}")
            return None

# Initialize user client (will be replaced with async version in each command)
user = None  # Will be initialized when needed


@Client.on_message(filters.command("scr", [".", "/"]))
async def scrapper_cc(Client, message):
    try:
        checkall = await check_all_thing(Client , message)
        if checkall[0] == False:
            return

        role = checkall[1]
        try:
            channel_link = message.text.split(" ")[1]
            limit        = int(message.text.split(" ")[2])
        except:
            resp = f"""<b>
Wrong Format ❌

Usage:
For Public Group Scraping
<code>/scr username 50</code>

For Private Group Scraping
<code>/scr https://t.me/+3C_fYRjP2y85YTk050 50</code>
        </b>"""
            await message.reply_text(resp, message.id)
            return

        if role == "FREE" and int(limit) > 5000:
            resp = """<b>
Limit Reached ⚠️

Message: Your Can Scrape 5000 CC at a Time . Buy Plan to Increase Your Limit .

Type /buy For Paid Plan
</b>"""
            await message.reply_text(resp, message.id)
            return

        if role == "PREMIUM" and int(limit) > 10000:
            resp = f"""<b>
Limit Reached ⚠️

Message: Your Can Scrape 10000 CC at a Time .

Type /buy For Paid Plan
</b>"""
            await message.reply_text(resp, message.id)
            return
            
        # Get a properly initialized user client
        user = await get_scraper_client()
        if not user:
            await message.reply_text("<b>Error ❌\n\nCould not initialize scraper client. Please contact admin.</b>", message.id)
            return
            
        # Start the client with proper error handling
        try:
            if not hasattr(user, 'is_connected') or not user.is_connected:
                await user.start()
                print("Started user client for CC scraping")
        except Exception as e:
            await error_log(f"Failed to start scraper client: {str(e)}")
            await message.reply_text(f"<b>Error ❌\n\nFailed to start scraper: {str(e)}</b>", message.id)
            return

        # Process the channel link
        if "https" in channel_link:
            print(f"Processing invite link: {channel_link}")
            check_link = await check_invite_link(user, channel_link, requested_by=message.from_user.id)
            if check_link == False:
                resp = f"""<b>
Wrong Invite Link ❌

Message: Your Provided Link is Wrong . Please Check Your Link and Try Again .

</b>"""
                await message.reply_text(resp, message.id)
                return

            channel_id    = check_link[1]
            channel_title = check_link[2]
            newly_joined  = check_link[3] if len(check_link) > 3 else False
            
            # Perform scraping
            await cc_private_scrape(message, user, Client, channel_id, channel_title, limit, role)
            
            # Try to leave the channel if we just joined it for scraping
            if newly_joined:
                try:
                    from FUNC.scraperfunc import leave_channel
                    await leave_channel(user, channel_id)
                except Exception as e:
                    await error_log(f"Failed to leave channel after scraping: {str(e)}")

        else:
            resp = f"""<b>
Gate: CC Scraper ♻️

Message: Scraping {limit} CC From @{channel_link} . Please Wait . 

Status: Scraping...
        </b> """
            delete = await message.reply_text(resp, message.id)
            await cc_public_scrape(message, user, Client, channel_link, limit, delete, role)

    except Exception as e:
        import traceback
        await error_log(traceback.format_exc())
        await message.reply_text(f"<b>Error ❌\n\n{str(e)}</b>", message.id)


@Client.on_message(filters.command("scrsk", [".", "/"]))
async def scrapper_sk(Client, message):
    try:
        checkall = await check_all_thing(Client , message)
        if checkall[0] == False:
            return

        role = checkall[1]
        try:
            channel_link = message.text.split(" ")[1]
            limit        = int(message.text.split(" ")[2])
        except:
            resp = f"""<b>
Wrong Format ❌

Usage:
For Public Group Scraping
<code>/scrsk username 50</code>

For Private Group Scraping
<code>/scrsk https://t.me/+3C_fYRjP2y85YTk050 50</code>
        </b>"""
            await message.reply_text(resp, message.id)
            return

        if role == "FREE" and int(limit) > 5000:
            resp = """<b>
Limit Reached ⚠️

Message: Your Can Scrape 5000 SK at a Time . Buy Plan to Increase Your Limit .

Type /buy For Paid Plan
</b>"""
            await message.reply_text(resp, message.id)
            return

        if role == "PREMIUM" and int(limit) > 10000:
            resp = """<b>
Limit Reached ⚠️

Message: Your Can Scrape 10000 SK at a Time . Buy Plan to Increase Your Limit .

Type /buy For Paid Plan
</b>"""
            await message.reply_text(resp, message.id)
            return

        # Get a properly initialized user client
        user = await get_scraper_client()
        if not user:
            await message.reply_text("<b>Error ❌\n\nCould not initialize scraper client. Please contact admin.</b>", message.id)
            return
            
        # Start the client with proper error handling
        try:
            if not hasattr(user, 'is_connected') or not user.is_connected:
                await user.start()
                print("Started user client for SK scraping")
        except Exception as e:
            await error_log(f"Failed to start scraper client: {str(e)}")
            await message.reply_text(f"<b>Error ❌\n\nFailed to start scraper: {str(e)}</b>", message.id)
            return

        # Process the channel link
        if "https" in channel_link:
            print(f"Processing invite link: {channel_link}")
            check_link = await check_invite_link(user, channel_link, requested_by=message.from_user.id)
            if check_link == False:
                resp = f"""<b>
Wrong Invite Link ❌

Message: Your Provided Link is Wrong . Please Check Your Link and Try Again .

</b>"""
                await message.reply_text(resp, message.id)
                return

            channel_id    = check_link[1]
            channel_title = check_link[2]
            newly_joined  = check_link[3] if len(check_link) > 3 else False
            
            # Perform scraping
            await sk_private_scrape(message, user, Client, channel_id, channel_title, limit, role)
            
            # Try to leave the channel if we just joined it for scraping
            if newly_joined:
                try:
                    from FUNC.scraperfunc import leave_channel
                    await leave_channel(user, channel_id)
                except Exception as e:
                    await error_log(f"Failed to leave channel after scraping: {str(e)}")
        else:
            resp = f"""<b>
Gate: SK Scraper ♻️

Message: Scraping {limit} SK From @{channel_link} . Please Wait . 

Status: Scraping...
        </b> """
            delete = await message.reply_text(resp, message.id)
            await sk_public_scrape(message, user, Client, channel_link, limit, delete, role)

    except Exception as e:
        import traceback
        await error_log(traceback.format_exc())
        await message.reply_text(f"<b>Error ❌\n\n{str(e)}</b>", message.id)


@Client.on_message(filters.command("scrbin", [".", "/"]))
async def scrapper_bin(Client, message):
    try:
        checkall = await check_all_thing(Client , message)
        if checkall[0] == False:
            return

        role = checkall[1]
        try:
            scrape_bin   = message.text.split(" ")[1]
            channel_link = message.text.split(" ")[2]
            limit        = int(message.text.split(" ")[3])
        except:
            resp = f"""<b>
Wrong Format ❌

Usage:
For Public Group Scraping
<code>/scrbin bin username 50</code>

For Private Group Scraping
<code>/scrbin bin https://t.me/+3C_fYRjP2y85YTk050 50</code>
        </b>"""
            await message.reply_text(resp, message.id)
            return

        if role == "FREE" and int(limit) > 5000:
            resp = """<b>
Limit Reached ⚠️

Message: Your Can Scrape 5000 CC at a Time . Buy Plan to Increase Your Limit .

Type /buy For Paid Plan
</b>"""
            await message.reply_text(resp, message.id)
            return

        if role == "PREMIUM" and int(limit) > 10000:
            resp = """<b>
Limit Reached ⚠️

Message: Your Can Scrape 10000 CC at a Time . Buy Plan to Increase Your Limit .

Type /buy For Paid Plan
</b>"""
            await message.reply_text(resp, message.id)
            return

        # Get a properly initialized user client
        user = await get_scraper_client()
        if not user:
            await message.reply_text("<b>Error ❌\n\nCould not initialize scraper client. Please contact admin.</b>", message.id)
            return
            
        # Start the client with proper error handling
        try:
            if not hasattr(user, 'is_connected') or not user.is_connected:
                await user.start()
                print("Started user client for BIN scraping")
        except Exception as e:
            await error_log(f"Failed to start scraper client: {str(e)}")
            await message.reply_text(f"<b>Error ❌\n\nFailed to start scraper: {str(e)}</b>", message.id)
            return

        # Process the channel link
        if "https" in channel_link:
            print(f"Processing invite link: {channel_link}")
            check_link = await check_invite_link(user, channel_link, requested_by=message.from_user.id)
            if check_link == False:
                resp = f"""<b>
Wrong Invite Link ❌

Message: Your Provided Link is Wrong . Please Check Your Link and Try Again .

</b>"""
                await message.reply_text(resp, message.id)
                return

            channel_id    = check_link[1]
            channel_title = check_link[2]
            newly_joined  = check_link[3] if len(check_link) > 3 else False
            
            # Perform scraping
            await bin_private_scrape(
                message,
                user,
                Client,
                scrape_bin,
                channel_id,
                channel_title,
                limit,
                role,
            )
            
            # Try to leave the channel if we just joined it for scraping
            if newly_joined:
                try:
                    from FUNC.scraperfunc import leave_channel
                    await leave_channel(user, channel_id)
                except Exception as e:
                    await error_log(f"Failed to leave channel after scraping: {str(e)}")
        else:
            resp = f"""<b>
Gate: CC Scraper ♻️

Message: Scraping {limit} CC From @{channel_link} . Please Wait . 

Status: Scraping...
        </b> """
            delete = await message.reply_text(resp, message.id)
            await bin_public_scrape(
                message,
                user,
                Client,
                scrape_bin,
                channel_link,
                limit,
                delete,
                role,
            )

    except Exception as e:
        import traceback
        await error_log(traceback.format_exc())
        await message.reply_text(f"<b>Error ❌\n\n{str(e)}</b>", message.id)

# Import admin commands
from BOT.tools.admin_commands import *
