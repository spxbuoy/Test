# Admin-only commands for session management
from pyrogram.client import Client
from pyrogram import filters
import json
import asyncio
from CONFIG_DB import CHANNELS_DB

# Load config data
with open("FILES/config.json", "r", encoding="utf-8") as f:
    DATA = json.load(f)

@Client.on_message(filters.command("check_session", [".", "/"]) & filters.user(int(DATA["OWNER_ID"])))
async def check_session_cmd(client, message):
    """Admin command to check if the session is valid and working"""
    try:
        from FUNC.defs import validate_session, get_client_with_session
        import time
        
        resp = f"""<b>
Session Check ⏳

Message: Checking session status. Please wait...
</b>"""
        status_msg = await message.reply_text(resp, quote=True)
        
        # Create a new client instance for testing
        test_client = await get_client_with_session()
        if not test_client:
            await client.edit_message_text(
                message.chat.id, 
                status_msg.id, 
                f"<b>Session Error ❌\n\nCould not initialize client with session string. Check config.</b>"
            )
            return
            
        # Test the session
        try:
            await test_client.start()
            valid, details = await validate_session(test_client)
            
            if valid:
                # Get some additional info
                me = await test_client.get_me()
                
                await client.edit_message_text(
                    message.chat.id,
                    status_msg.id,
                    f"""<b>
Session Status: ✅ ACTIVE

• Account: {me.first_name} (@{me.username or 'None'})
• User ID: {me.id}
• Phone: {me.phone_number if hasattr(me, 'phone_number') else 'Not accessible'}
• Status: {details}

No issues detected with current session.
</b>"""
                )
            else:
                await client.edit_message_text(
                    message.chat.id,
                    status_msg.id,
                    f"""<b>
Session Status: ⚠️ ISSUE DETECTED

• Error: {details}

Please update the session string using /replace_session command.
</b>"""
                )
        except Exception as e:
            await client.edit_message_text(
                message.chat.id,
                status_msg.id,
                f"<b>Session Error ❌\n\n{str(e)}</b>"
            )
        finally:
            # Ensure client is stopped
            if test_client and hasattr(test_client, 'is_connected') and test_client.is_connected:
                await test_client.stop()
                
    except Exception as e:
        await message.reply_text(f"<b>Error: {str(e)}</b>", quote=True)


@Client.on_message(filters.command("replace_session", [".", "/"]) & filters.user(int(DATA["OWNER_ID"])))
async def replace_session_cmd(client, message):
    """Admin command to replace the session string"""
    try:
        # Check if we have a session string in the command
        if len(message.text.split()) < 2:
            await message.reply_text(
                "<b>Usage: /replace_session YOUR_NEW_SESSION_STRING</b>",
                quote=True
            )
            return
            
        # Get the new session string
        new_session = message.text.split(None, 1)[1].strip()
        
        # Test the new session string
        resp = f"""<b>
Session Update ⏳

Message: Validating new session string. Please wait...
</b>"""
        status_msg = await message.reply_text(resp, quote=True)
        
        # Try to create a client with the new session
        import json
        import asyncio
        from pyrogram.client import Client as PyroClient
        
        with open("FILES/config.json", "r", encoding="utf-8") as f:
            config = json.load(f)
            API_ID = config.get("API_ID")
            API_HASH = config.get("API_HASH")
        
        temp_client = PyroClient(
            "temp_session",
            api_id=API_ID,
            api_hash=API_HASH,
            session_string=new_session,
            in_memory=True
        )
        
        try:
            await temp_client.start()
            me = await temp_client.get_me()
            
            # Update the config file
            config["SESSION_STRING"] = new_session
            
            with open("FILES/config.json", "w", encoding="utf-8") as f:
                json.dump(config, f, indent=4)
                
            await client.edit_message_text(
                message.chat.id,
                status_msg.id,
                f"""<b>
Session Updated ✅

• New session for: {me.first_name} (@{me.username or 'None'})
• User ID: {me.id}
• Status: Active and working

The bot will use the new session for future scraping operations.
</b>"""
            )
        except Exception as e:
            await client.edit_message_text(
                message.chat.id,
                status_msg.id,
                f"<b>Invalid Session ❌\n\nThe provided session string is invalid: {str(e)}</b>"
            )
        finally:
            # Ensure client is stopped
            if 'temp_client' in locals() and hasattr(temp_client, 'is_connected') and temp_client.is_connected:
                await temp_client.stop()
                
    except Exception as e:
        await message.reply_text(f"<b>Error: {str(e)}</b>", quote=True)


@Client.on_message(filters.command("channel_stats", [".", "/"]) & filters.user(int(DATA["OWNER_ID"])))
async def channel_stats_cmd(client, message):
    """Admin command to check channel leaving statistics"""
    try:
        from CONFIG_DB import CHANNELS_DB
        import time
        import asyncio
        
        # Check command arguments
        command_parts = message.text.split()
        
        if len(command_parts) == 1:
            # Just show general statistics
            total_channels = CHANNELS_DB.count_documents({})
            not_left = CHANNELS_DB.count_documents({"left_channel": False})
            
            # Calculate recent joins
            from datetime import datetime, timedelta
            twenty_four_hours_ago = datetime.now() - timedelta(days=1)
            recent_joins = CHANNELS_DB.count_documents({"join_time": {"$gt": twenty_four_hours_ago}})
            
            resp = f"""<b>
Channel Management Statistics 📊

• Total Tracked Channels: {total_channels}
• Channels Not Yet Left: {not_left}
• Recent Joins (24h): {recent_joins}

Commands:
/channel_stats list - Show channels not left
/channel_stats clear - Clear all left channel records
/channel_stats force_leave - Attempt to leave all channels now
</b>"""
            await message.reply_text(resp, quote=True)
            
        elif command_parts[1] == "list":
            # List channels we haven't left
            channels = list(CHANNELS_DB.find(
                {"left_channel": False},
                {"_id": 0, "channel_id": 1, "channel_title": 1, "join_time": 1, "leave_attempts": 1}
            ).sort([("join_time", -1)]).limit(10))
            
            if not channels:
                await message.reply_text("<b>No channels found that need to be left.</b>", quote=True)
                return
                
            text = "<b>Channels Not Left:</b>\n\n"
            for idx, channel in enumerate(channels, 1):
                join_time = channel.get("join_time", "Unknown")
                join_time_str = join_time.strftime("%Y-%m-%d %H:%M") if hasattr(join_time, "strftime") else str(join_time)
                
                text += f"{idx}. {channel.get('channel_title', 'Unknown')}\n"
                text += f"   ID: {channel.get('channel_id', 'Unknown')}\n"
                text += f"   Joined: {join_time_str}\n"
                text += f"   Leave attempts: {channel.get('leave_attempts', 0)}\n\n"
            
            await message.reply_text(text, quote=True)
            
        elif command_parts[1] == "clear":
            # Clear left channel records
            result = CHANNELS_DB.delete_many({"left_channel": True})
            await message.reply_text(f"<b>Cleared {result.deleted_count} channel records.</b>", quote=True)
            
        elif command_parts[1] == "force_leave":
            # Force leave all channels using batch processing
            # Get the number of channels to process from command if provided
            limit = 20  # Default limit
            if len(command_parts) > 2 and command_parts[2].isdigit():
                limit = int(command_parts[2])
                limit = min(limit, 50)  # Cap at 50 for safety
            
            channels = list(CHANNELS_DB.find(
                {"left_channel": False},
                {"_id": 0, "channel_id": 1, "channel_title": 1, "join_time": 1}
            ).sort([("join_time", 1)]).limit(limit))
            
            if not channels:
                await message.reply_text("<b>No channels found that need to be left.</b>", quote=True)
                return
                
            status_msg = await message.reply_text(
                f"<b>Attempting to leave {len(channels)} channels in batches. Please wait...</b>", 
                quote=True
            )
            
            from FUNC.defs import get_client_with_session
            from FUNC.batch_operations import batch_leave_channels
            
            test_client = await get_client_with_session()
            if not test_client:
                await client.edit_message_text(
                    message.chat.id,
                    status_msg.id,
                    "<b>Failed to initialize client with session string.</b>"
                )
                return
                
            try:
                await test_client.start()
                
                # Extract just the channel IDs for batch processing
                channel_ids = [channel.get("channel_id") for channel in channels]
                
                # Process channels in batches
                await client.edit_message_text(
                    message.chat.id,
                    status_msg.id,
                    f"<b>Processing {len(channel_ids)} channels in batches...</b>"
                )
                
                successful, failed = await batch_leave_channels(test_client, channel_ids)
                
                # Prepare detailed result message
                success_count = len([s for s in successful if s[1]])  # Count truly successful operations
                failed_count = len(channel_ids) - success_count
                
                result_message = f"<b>Channel Leave Results:\n• Successfully left: {success_count}\n• Failed: {failed_count}</b>"
                
                # Add details about failed channels if any
                if failed_count > 0:
                    result_message += "\n\n<b>Failed channels:</b>"
                    for i, (channel_id, error) in enumerate(failed[:5]):  # Show up to 5 failures
                        # Try to get channel title
                        channel_info = next((c for c in channels if c.get("channel_id") == channel_id), None)
                        title = channel_info.get("channel_title", "Unknown") if channel_info else "Unknown"
                        result_message += f"\n{i+1}. {title} ({channel_id})"
                    
                    if failed_count > 5:
                        result_message += f"\n... and {failed_count - 5} more"
                
                await client.edit_message_text(
                    message.chat.id,
                    status_msg.id,
                    result_message
                )
            
            except Exception as e:
                await client.edit_message_text(
                    message.chat.id,
                    status_msg.id,
                    f"<b>Error during batch operation: {str(e)}</b>"
                )
            finally:
                if test_client and hasattr(test_client, 'is_connected') and test_client.is_connected:
                    await test_client.stop()
        else:
            await message.reply_text("<b>Unknown command. Use /channel_stats for help.</b>", quote=True)
            
    except Exception as e:
        await message.reply_text(f"<b>Error: {str(e)}</b>", quote=True)


# Commands are registered automatically via the decorators