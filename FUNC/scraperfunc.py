import time
import os
from FUNC.defs import *
from datetime import timedelta


async def cc_public_scrape(message, user, bot, channel_link, limit, delete, role):
    try:
        start = time.perf_counter()
        ccs = []
        amt = 0
        duplicate = 0
        async for msg in user.get_chat_history(channel_link, limit):
            if msg.text:
                content = msg.text
            elif msg.caption:
                content = msg.caption
            else:
                continue

            try:
                for x in content.split("\n"):
                    getcc = await getcards(x)
                    if getcc:
                        if getcc in ccs:
                            duplicate += 1
                        elif amt < limit:
                            ccs.append(getcc)
                            amt += 1
                        else:
                            break

            except:
                getcc = await getcards(content)
                if getcc:
                    if getcc in ccs:
                        duplicate += 1
                    elif amt < limit:
                        ccs.append(getcc)
                        amt += 1
                    else:
                        break
            # try:
            #     for x in content.split("\n"):
            #         getcc = await getcards(x)
            #         if getcc:
            #             if getcc in ccs:
            #                 dublicate += 1
            #             else:
            #                 ccs.append(getcc)
            #                 amt += 1

            # except:
            #     getcc = await getcards(content)
            #     if getcc:
            #         if getcc in ccs:
            #             dublicate += 1
            #         else:
            #             ccs.append(getcc)
            #             amt += 1

        if amt == 0:
            await bot.delete_messages(message.chat.id, delete.id)
            resp = f"""<b>
No CC Found ❌

Message: We Didn't Find Any CC In @{channel_link}.

</b>"""
            await message.reply_text(resp, message.id)
        else:
            file_name = f"downloads/{amt}x_CC_Scraped_For_{message.from_user.id}_By_@amkuush.txt"
            with open(file_name, "a", encoding="utf-8") as f:
                for x in ccs:
                    cc, mes, ano, cvv = x[0], x[1], x[2], x[3]
                    fullcc = f"{cc}|{mes}|{ano}|{cvv}"
                    f.write(f"{fullcc}\n")

            await bot.delete_messages(message.chat.id, delete.id)
            chat_info = await user.get_chat(channel_link)
            title = chat_info.title
            taken = str(timedelta(seconds=time.perf_counter() - start))
            hours, minutes, seconds = map(float, taken.split(":"))
            hour = int(hours)
            min = int(minutes)
            sec = int(seconds)

            caption = f"""<b>

CC Scraped ✅

● Source: {title}
● Targeted Amount: {limit}
● CC Found: {amt}
● Duplicate Removed: {duplicate}
● Scraped By: <a href="tg://user?id={message.from_user.id}"> {message.from_user.first_name}</a> ♻️ [ {role} ]
● Time Taken: {min} Minutes {sec} Seconds
● Bot by - <a href=\"tg://user?id=7447317982\">tevixl</a>
"""
            await message.reply_document(document=file_name, caption=caption, reply_to_message_id=message.id)
            os.remove(file_name)

    except Exception as e:
        try:
            await bot.delete_messages(message.chat.id, delete.id)
            await message.reply_text(f"<b>Error ❌\n\n{str(e)}</b>", message.id)
        except:
            pass


async def check_invite_link(user, channel_link, requested_by=None):
    import traceback
    
    # Make sure the user client is properly initialized
    if not user or not hasattr(user, 'get_chat'):
        await error_log("User client not properly initialized in check_invite_link")
        return False
        
    try:
        # Log the attempt
        print(f"Attempting to get chat info for: {channel_link}")
        
        # First try: check if we can access the channel directly
        chat_info = await user.get_chat(channel_link)
        channel_id = chat_info.id
        title = chat_info.title
        
        # Log success
        print(f"Successfully accessed channel: {title} ({channel_id}) without joining")
        
        # We didn't need to join the channel (already a member or public)
        return True, channel_id, title, False
        
    except Exception as e1:
        # First attempt failed, log the error
        await error_log(f"First attempt to access channel failed: {str(e1)}\n{traceback.format_exc()}")
        
        try:
            # Second try: attempt to join the channel
            print(f"Attempting to join channel: {channel_link}")
            join = await user.join_chat(channel_link)
            title = join.title
            channel_id = join.id
            
            # Log success
            print(f"Successfully joined channel: {title} ({channel_id})")
            
            # Record that we joined this channel
            from FUNC.defs import record_channel_join
            await record_channel_join(channel_id, title, channel_link, requested_by)
            
            # Return success with newly_joined flag set to True
            return True, channel_id, title, True
            
        except Exception as e2:
            # Both attempts failed, log detailed error
            error_msg = f"Failed to access or join channel: {str(e2)}\n{traceback.format_exc()}"
            await error_log(error_msg)
            
            # Check if this is an invite link format issue
            if "invite" in str(e2).lower() or "URL" in str(e2) or "link" in str(e2).lower():
                print(f"Invalid invite link format: {channel_link}")
            
            return False


async def cc_private_scrape(message, user, bot, channel_id, channel_title, limit, role):
    try:
        start = time.perf_counter()
        ccs = []
        amt = 0
        dublicate = 0
        resp = f"""<b>
Gate: CC Scraper ♻️

Message: Scraping {limit} CC From {channel_title}. Please Wait.

Status: Scraping...
</b> """
        delete = await message.reply_text(resp, message.id)
        async for msg in user.get_chat_history(channel_id, limit):
            if msg.text:
                content = msg.text
            elif msg.caption:
                content = msg.caption
            else:
                continue

            try:
                for x in content.split("\n"):
                    getcc = await getcards(x)
                    if getcc:
                        if getcc in ccs:
                            dublicate += 1
                        else:
                            ccs.append(getcc)
                            amt += 1

            except:
                getcc = await getcards(content)
                if getcc:
                    if getcc in ccs:
                        dublicate += 1
                    else:
                        ccs.append(getcc)
                        amt += 1

        if amt == 0:
            await bot.delete_messages(message.chat.id, delete.id)
            resp = f"""<b>
No CC Found ❌

Message: We Didn't Find Any CC In {channel_title}.

</b>"""
            await message.reply_text(resp, message.id)

        else:
            file_name = f"downloads/{amt}x_CC_Scraped_For_{message.from_user.id}_By_@amkuush.txt"
            with open(file_name, "a", encoding="UTF-8") as f:
                for x in ccs:
                    cc, mes, ano, cvv = x[0], x[1], x[2], x[3]
                    fullcc = f"{cc}|{mes}|{ano}|{cvv}"
                    f.write(f"{fullcc}\n")
            await bot.delete_messages(message.chat.id, delete.id)
            taken = str(timedelta(seconds=time.perf_counter() - start))
            hours, minutes, seconds = map(float, taken.split(":"))
            min = int(minutes)
            sec = int(seconds)
            caption = f"""<b>
CC Scraped ✅

● Source: {channel_title}
● Targeted Amount: {limit}
● CC Found: {amt}
● Duplicate Removed: {dublicate}
● Scraped By: <a href="tg://user?id={message.from_user.id}"> {message.from_user.first_name}</a> ♻️ [ {role} ]
● Time Taken: {min} Minutes {sec} Seconds
● Bot by - <a href=\"tg://user?id=7447317982\">tevixl</a>
</b>
"""
            await message.reply_document(document=file_name, caption=caption, reply_to_message_id=message.id)
            os.remove(file_name)

    except Exception as e:
        try:
            await bot.delete_messages(message.chat.id, delete.id)
            await message.reply_text(f"<b>Error ❌\n\n{str(e)}</b>", message.id)
        except:
            pass


async def bin_public_scrape(message, user, bot, scrape_bin, channel_link, limit, delete, role):
    try:
        start = time.perf_counter()
        ccs = []
        amt = 0
        dublicate = 0
        async for msg in user.get_chat_history(channel_link, limit):
            msg = str(msg.text)
            try:
                for x in msg.split("\n"):
                    getcc = await getcards(x)
                    if getcc:
                        if getcc in ccs:
                            dublicate += 1
                        else:
                            if scrape_bin in getcc[0][:6]:
                                ccs.append(getcc)
                                amt += 1

            except:
                getcc = await getcards(msg)
                if getcc:
                    if getcc in ccs:
                        dublicate += 1
                    else:
                        ccs.append(getcc)
                        amt += 1
        if amt == 0:
            await bot.delete_messages(message.chat.id, delete.id)
            resp = f"""<b>
No CC Found ❌

Message: We Didnt Find Any CC In @{channel_link} .

</b>"""
            await message.reply_text(resp, message.id)

        else:
            file_name = f"downloads/{amt}x_CC_Scraped_For_{message.from_user.id}_By_@PharaohCHKϟ.txt"
            with open(file_name, "a", encoding="UTF-8") as f:
                for x in ccs:
                    cc, mes, ano, cvv = x[0], x[1], x[2], x[3]
                    fullcc = f"{cc}|{mes}|{ano}|{cvv}"
                    f.write(f"{fullcc}\n")

            await bot.delete_messages(message.chat.id, delete.id)
            chat_info = await user.get_chat(channel_link)
            title = chat_info.title
            taken = str(timedelta(seconds=time.perf_counter() - start))
            hours, minutes, seconds = map(float, taken.split(":"))
            hour = int(hours)
            min = int(minutes)
            sec = int(seconds)
            caption = f"""<b>
CC Scraped ✅

● Source: {title}
● Targeted Amount: {limit}
● Targeted Bin: {scrape_bin}
● CC Found: {amt}
● Duplicate Removed: {dublicate}
● Scraped By: <a href="tg://user?id={message.from_user.id}"> {message.from_user.first_name}</a> ♻️ [ {role} ]
● Time Taken: {min} Minutes {sec} Seconds
● Bot by - <a href=\"tg://user?id=7447317982\">tevixl</a>
"""
            await message.reply_document(document=file_name, caption=caption, reply_to_message_id=message.id)
            os.remove(file_name)

    except Exception as e:
        try:
            await bot.delete_messages(message.chat.id, delete.id)
            await message.reply_text(f"<b>Error ❌\n\n{str(e)}</b>", message.id)
        except:
            pass


async def bin_private_scrape(message, user, bot, scrape_bin, channel_id, channel_title, limit, role):
    try:
        start = time.perf_counter()
        ccs = []
        amt = 0
        dublicate = 0
        resp = f"""<b>
Gate: CC Scraper ♻️

Message: Scraping {limit} CC From {channel_title} . Please Wait . 

Status: Scraping...
            </b> """
        delete = await message.reply_text(resp, message.id)
        async for msg in user.get_chat_history(channel_id, limit):
            msg = str(msg.text)
            try:
                for x in msg.split("\n"):
                    getcc = await getcards(x)
                    if getcc:
                        if getcc in ccs:
                            dublicate += 1
                        else:
                            if scrape_bin in getcc[0][:6]:
                                ccs.append(getcc)
                                amt += 1

            except:
                getcc = await getcards(msg)
                if getcc:
                    if getcc in ccs:
                        dublicate += 1
                    else:
                        ccs.append(getcc)
                        amt += 1
        if amt == 0:
            await bot.delete_messages(message.chat.id, delete.id)
            resp = f"""<b>
No CC Found ❌

Message: We Didnt Find Any CC In {channel_title} .

</b>"""
            await message.reply_text(resp, message.id)

        else:
            file_name = f"downloads/{amt}x_CC_Scraped_For_{message.from_user.id}_By_@amkuush.txt"
            with open(file_name, "a", encoding="UTF-8") as f:
                for x in ccs:
                    cc, mes, ano, cvv = x[0], x[1], x[2], x[3]
                    fullcc = f"{cc}|{mes}|{ano}|{cvv}"
                    f.write(f"{fullcc}\n")
            await bot.delete_messages(message.chat.id, delete.id)
            taken = str(timedelta(seconds=time.perf_counter() - start))
            hours, minutes, seconds = map(float, taken.split(":"))
            min = int(minutes)
            sec = int(seconds)
            caption = f"""<b>
CC Scraped ✅

● Source: {channel_title}
● Targeted Amount: {limit}
● Targeted Bin: {scrape_bin}
● CC Found: {amt}
● Duplicate Removed: {dublicate}
● Scraped By: <a href="tg://user?id={message.from_user.id}"> {message.from_user.first_name}</a> ♻️ [ {role} ]
● Time Taken: {min} Minutes {sec} Seconds
● Bot by - <a href=\"tg://user?id=7447317982\">tevixl</a>
"""
            await message.reply_document(document=file_name, caption=caption, reply_to_message_id=message.id)
            os.remove(file_name)

    except Exception as e:
        try:
            await bot.delete_messages(message.chat.id, delete.id)
            await message.reply_text(f"<b>Error ❌\n\n{str(e)}</b>", message.id)
        except:
            pass


async def sk_public_scrape(message, user, bot, channel_link, limit, delete, role):
    try:
        start = time.perf_counter()
        ccs = []
        amt = 0
        dublicate = 0
        async for msg in user.get_chat_history(channel_link, limit):
            msg = str(msg.text)
            if "sk_live" in msg:
                sk = msg.split("sk_live")[1].split(" ")[0]
                if "xxxxx" in sk:
                    pass
                else:
                    if "\n" in sk:
                        sk = sk.split("\n")[0]
                    if "✅" in sk:
                        sk = sk.splice("✅")[1]
                    sk = "sk_live" + sk
                    if sk in ccs:
                        dublicate += 1
                    else:
                        amt += 1
                        ccs.append(sk)

        if amt == 0:
            await bot.delete_messages(message.chat.id, delete.id)
            resp = f"""<b>
No SK Found ❌

Message: We Didnt Find Any SK In @{channel_link} .

</b>"""
            await message.reply_text(resp, message.id)

        else:
            file_name = f"downloads/{amt}x_SK_Scraped_For_{message.from_user.id}_By_@amkuush.txt"
            with open(file_name, "a", encoding="UTF-8") as f:
                for x in ccs:
                    f.write(f"{x}\n")

            await bot.delete_messages(message.chat.id, delete.id)
            chat_info = await user.get_chat(channel_link)
            title = chat_info.title
            taken = str(timedelta(seconds=time.perf_counter() - start))
            hours, minutes, seconds = map(float, taken.split(":"))
            hour = int(hours)
            min = int(minutes)
            sec = int(seconds)
            caption = f"""<b>
SK Scraped ✅

● Source: {title}
● Targeted Amount: {limit}
● SK Found: {amt}
● Scraped By: <a href="tg://user?id={message.from_user.id}"> {message.from_user.first_name}</a> ♻️ [ {role} ]
● Time Taken: {min} Minutes {sec} Seconds
● Bot by - <a href=\"tg://user?id=7447317982\">tevixl</a>
"""
            await message.reply_document(document=file_name, caption=caption, reply_to_message_id=message.id)
            os.remove(file_name)

    except Exception as e:
        try:
            await bot.delete_messages(message.chat.id, delete.id)
            await message.reply_text(f"<b>Error ❌\n\n{str(e)}</b>", message.id)
        except:
            pass


async def sk_private_scrape(message, user, bot, channel_id, channel_title, limit, role):
    try:
        start = time.perf_counter()
        ccs = []
        amt = 0
        dublicate = 0
        resp = f"""<b>
Gate: SK Scraper ♻️

Message: Scraping {limit} SK From {channel_title} . Please Wait . 

Status: Scraping...
            </b> """
        delete = await message.reply_text(resp, message.id)
        async for msg in user.get_chat_history(channel_id, limit):
            msg = str(msg.text)
            if "sk_live" in msg:
                sk = msg.split("sk_live")[1].split(" ")[0]
                if "xxxxx" in sk:
                    pass
                else:
                    if "\n" in sk:
                        sk = sk.split("\n")[0]
                    if "✅" in sk:
                        sk = sk.splice("✅")[1]
                    sk = "sk_live" + sk
                    if sk in ccs:
                        dublicate += 1
                    else:
                        amt += 1
                        ccs.append(sk)

        if amt == 0:
            await bot.delete_messages(message.chat.id, delete.id)
            resp = f"""<b>
No SK Found ❌

Message: We Didnt Find Any SK In {channel_title} .

</b>"""
            await message.reply_text(resp, message.id)

        else:
            file_name = f"downloads/{amt}x_SK_Scraped_For_{message.from_user.id}_By_@amkuush.txt"
            with open(file_name, "a", encoding="UTF-8") as f:
                for x in ccs:
                    f.write(f"{x}\n")
            await bot.delete_messages(message.chat.id, delete.id)
            taken = str(timedelta(seconds=time.perf_counter() - start))
            hours, minutes, seconds = map(float, taken.split(":"))
            min = int(minutes)
            sec = int(seconds)
            caption = f"""<b>
SK Scraped ✅

● Source: {channel_title}
● Targeted Amount: {limit}
● CC Found: {amt}
● Duplicate Removed: {dublicate}
● Scraped By: <a href="tg://user?id={message.from_user.id}"> {message.from_user.first_name}</a> ♻️ [ {role} ]
● Time Taken: {min} Minutes {sec} Seconds
● Bot by - <a href=\"tg://user?id=7447317982\">tevixl</a>
"""
            await message.reply_document(document=file_name, caption=caption, reply_to_message_id=message.id)
            os.remove(file_name)

    except Exception as e:
        try:
            await bot.delete_messages(message.chat.id, delete.id)
            await message.reply_text(f"<b>Error ❌\n\n{str(e)}</b>", message.id)
        except:
            pass
async def calculate_backoff_time(attempts):
    """
    Calculate exponential backoff time with jitter for retry operations.
    
    Args:
        attempts (int): Number of previous attempts
    
    Returns:
        float: Number of hours to wait before next attempt
    """
    import random
    # Base exponential backoff: 2^attempts hours with maximum of 24 hours
    # Adding jitter of ±25% to avoid thundering herd problem
    base_hours = min(2 ** attempts, 24)
    jitter = base_hours * 0.25 * (random.random() * 2 - 1)  # Random value between -25% and +25%
    return max(0.5, base_hours + jitter)  # Minimum 30 minutes

async def leave_channel(client, channel_id):
    """
    Attempt to leave a channel and update its status in the database.
    Uses exponential backoff for retries.
    
    Args:
        client (pyrogram.Client): The Telegram client to use
        channel_id (int/str): ID of the channel to leave
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Perform the leave operation
        await client.leave_chat(channel_id)
        
        from FUNC.defs import update_channel_leave_status
        await update_channel_leave_status(channel_id, success=True)
        
        # Log success
        from CONFIG_DB import CHANNELS_DB
        channel_info = CHANNELS_DB.find_one({"channel_id": channel_id})
        if channel_info:
            channel_title = channel_info.get("channel_title", "Unknown")
            print(f"Successfully left channel: {channel_title} ({channel_id})")
        
        return True
    except Exception as e:
        from FUNC.defs import error_log, update_channel_leave_status
        
        # Determine if this is a permanent error or should be retried
        error_msg = str(e).lower()
        permanent_failure = any(keyword in error_msg for keyword in 
                               ["not found", "not exist", "invalid", "already left"])
        
        if permanent_failure:
            # If it's a permanent error (like channel doesn't exist), mark as 'left'
            await update_channel_leave_status(channel_id, success=True)
            await error_log(f"Channel {channel_id} marked as left due to permanent error: {error_msg}")
            return True
        else:
            # For temporary errors, update the failure count for backoff
            await update_channel_leave_status(channel_id, success=False)
            await error_log(f"Failed to leave channel {channel_id}: {error_msg}")
            return False


async def check_system_load():
    """
    Check if the system is under heavy load and should postpone background tasks.
    
    Returns:
        bool: True if system load is high, False otherwise
    """
    try:
        import os
        import psutil
        
        # Check CPU usage
        cpu_percent = psutil.cpu_percent(interval=0.5)
        # Check memory usage
        memory_percent = psutil.virtual_memory().percent
        
        # If either CPU or memory usage is high, consider the system under load
        return cpu_percent > 70 or memory_percent > 80
    except ImportError:
        # If psutil is not available, assume system is not under heavy load
        return False
    except Exception:
        # If we can't check system load, assume it's safe to proceed
        return False

async def is_user_active():
    """
    Check if there is recent user activity that should take precedence over background tasks.
    
    Returns:
        bool: True if there is recent user activity, False otherwise
    """
    try:
        from datetime import datetime, timedelta
        from CONFIG_DB import COLLECTIONS
        
        # We'll use a simple heuristic: check if there have been commands in the last 5 minutes
        five_minutes_ago = datetime.now() - timedelta(minutes=5)
        
        # This assumes you have some collection that tracks command usage
        # If you don't have such tracking, this will always return False
        if hasattr(COLLECTIONS, "COMMAND_LOGS"):
            recent_commands = COLLECTIONS.COMMAND_LOGS.count_documents({
                "timestamp": {"$gt": five_minutes_ago}
            })
            return recent_commands > 0
    except Exception:
        pass
    
    # Default: assume no recent activity
    return False

async def channel_cleanup_background():
    """
    Background task that periodically attempts to leave channels
    that we previously joined but failed to leave.
    
    Features:
    - Runs during off-peak hours when possible
    - Yields to user commands
    - Uses adaptive timing based on system load
    - Implements proper exponential backoff with jitter
    - Handles client connections efficiently
    """
    import asyncio
    from CONFIG_DB import CHANNELS_DB
    from FUNC.defs import error_log, get_client_with_session
    from datetime import datetime, timedelta
    
    # Configuration
    BASE_CLEANUP_INTERVAL = 6 * 3600  # 6 hours baseline
    MAX_BATCH_SIZE = 10  # Process at most 10 channels per run
    
    # Initial delay to ensure bot is fully started
    await asyncio.sleep(300)
    
    while True:
        try:
            # Check if we should run now (system not under heavy load, preferably non-peak hours)
            system_under_load = await check_system_load()
            user_active = await is_user_active()
            
            # Determine if this is an off-peak hour (2 AM - 6 AM)
            current_hour = datetime.now().hour
            off_peak_hour = 2 <= current_hour <= 6
            
            if system_under_load or user_active:
                # System is busy or user is active, use a shorter sleep and try again
                await asyncio.sleep(10 * 60)  # Wait 10 minutes
                continue
                
            # Find channels we haven't left yet - prioritize older joins
            channels_to_leave = list(CHANNELS_DB.find(
                {"left_channel": False},
                {"_id": 0, "channel_id": 1, "channel_title": 1, "leave_attempts": 1, "last_leave_attempt": 1, "join_time": 1}
            ).sort([("join_time", 1)]).limit(MAX_BATCH_SIZE))
            
            processed_channels = 0
            
            if channels_to_leave:
                print(f"Background cleanup: found {len(channels_to_leave)} channels to process")
                
                # Only initialize client if we have channels to process
                client = await get_client_with_session()
                if client:
                    try:
                        await client.start()
                        
                        for channel in channels_to_leave:
                            # Calculate backoff using our improved function
                            backoff_hours = await calculate_backoff_time(channel.get("leave_attempts", 0))
                            
                            # Check if we should attempt to leave again based on backoff
                            last_attempt = channel.get("last_leave_attempt")
                            if last_attempt:
                                hours_since_attempt = (datetime.now() - last_attempt).total_seconds() / 3600
                                if hours_since_attempt < backoff_hours:
                                    # Skip this channel, too soon to retry
                                    continue
                            
                            # Check again if user is active before each channel operation
                            if await is_user_active():
                                # User became active, stop processing and try again later
                                break
                            
                            # Try to leave the channel
                            channel_id = channel["channel_id"]
                            result = await leave_channel(client, channel_id)
                            processed_channels += 1
                            
                            if result:
                                print(f"Background cleanup: successfully left channel {channel.get('channel_title', channel_id)}")
                            
                            # Small delay between operations to prevent API rate limits
                            await asyncio.sleep(2)
                    finally:
                        # Always ensure client is stopped
                        try:
                            await client.stop()
                        except:
                            pass
            
            # Determine next sleep interval - adjust based on time of day and results
            next_interval = BASE_CLEANUP_INTERVAL
            
            if off_peak_hour:
                # During off-peak hours, run more frequently if there are channels to process
                next_interval = 30 * 60 if processed_channels > 0 else 2 * 3600
            else:
                # During peak hours, run less frequently
                next_interval = 8 * 3600
            
            # Log status
            if processed_channels > 0:
                await error_log(f"Background cleanup: processed {processed_channels} channels. Next run in {next_interval/3600:.1f} hours")
            
            # Wait until next cleanup cycle
            await asyncio.sleep(next_interval)
            
        except Exception as e:
            await error_log(f"Channel cleanup background task error: {str(e)}")
            # If there's an error, wait before retrying
            await asyncio.sleep(1800)  # 30 minutes