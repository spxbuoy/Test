import json
from datetime import datetime
from pyrogram import Client, filters
from FUNC.usersdb_func import *
from FUNC.defs import *
from mongodb import *

# Create API_KEYS collection if it doesn't exist in CONFIG_DATABASE
if "API_KEYS" not in client["CONFIG_DATABASE"].list_collection_names():
    client["CONFIG_DATABASE"].create_collection("API_KEYS")
    
# Initialize API_KEYS collection
API_KEYS = client["CONFIG_DATABASE"]["API_KEYS"]

# Initialize with default values if empty
if API_KEYS.count_documents({}) == 0:
    default_apis = [
        {
            "api_name": "randomuser",
            "api_key": "",
            "enabled": True,
            "usage_count": 0,
            "remaining_quota": float('inf'),
            "last_reset": "",
            "success_rate": 1.0
        },
        {
            "api_name": "mockaroo",
            "api_key": "",
            "enabled": False,
            "usage_count": 0,
            "remaining_quota": 1000,  # Default free tier limit
            "last_reset": "",
            "success_rate": 0
        },
        {
            "api_name": "randomapi",
            "api_key": "",
            "enabled": False,
            "usage_count": 0,
            "remaining_quota": 1000,  # Default free tier limit
            "last_reset": "",
            "success_rate": 0
        }
    ]
    API_KEYS.insert_many(default_apis)

# Create SCHEMA_MAPPINGS collection if it doesn't exist
if "SCHEMA_MAPPINGS" not in client["CONFIG_DATABASE"].list_collection_names():
    client["CONFIG_DATABASE"].create_collection("SCHEMA_MAPPINGS")

# Initialize SCHEMA_MAPPINGS collection
SCHEMA_MAPPINGS = client["CONFIG_DATABASE"]["SCHEMA_MAPPINGS"]

async def get_api_key(api_name):
    """Get the API key for a specific service."""
    api_record = API_KEYS.find_one({"api_name": api_name})
    if api_record and api_record.get("enabled", False):
        return api_record.get("api_key", "")
    return None

async def increment_api_usage(api_name):
    """Increment the usage count for an API."""
    API_KEYS.update_one(
        {"api_name": api_name},
        {"$inc": {"usage_count": 1, "remaining_quota": -1}}
    )

async def update_api_success_rate(api_name, success):
    """Update the success rate for an API."""
    api_record = API_KEYS.find_one({"api_name": api_name})
    if api_record:
        current_rate = api_record.get("success_rate", 0)
        current_count = api_record.get("usage_count", 0)
        
        # Calculate new success rate with more weight on recent results
        if current_count > 0:
            new_rate = (current_rate * 0.9) + (1 if success else 0) * 0.1
        else:
            new_rate = 1 if success else 0
            
        API_KEYS.update_one(
            {"api_name": api_name},
            {"$set": {"success_rate": new_rate}}
        )

async def get_schema_id(api_name, country_code):
    """Get schema ID for a specific country and API."""
    mapping = SCHEMA_MAPPINGS.find_one({
        "api_name": api_name,
        "country_code": country_code
    })
    
    if mapping:
        return mapping.get("schema_id", "")
    
    # Return default schema if no specific mapping exists
    default_mapping = SCHEMA_MAPPINGS.find_one({
        "api_name": api_name,
        "country_code": "default"
    })
    
    if default_mapping:
        return default_mapping.get("schema_id", "")
        
    return ""

@Client.on_message(filters.command("setapi", [".", "/"]) & filters.user([7325746010]))
async def cmd_setapi(client, message):
    """Admin command to set an API key."""
    try:
        parts = message.text.split(" ", 2)
        if len(parts) < 3:
            await message.reply_text(
                "<b>Usage: /setapi [api_name] [api_key]</b>\n"
                "<b>Example: /setapi mockaroo YOUR_API_KEY_HERE</b>\n"
                "<b>Available APIs: mockaroo, randomapi</b>"
            )
            return
            
        api_name = parts[1].lower()
        api_key = parts[2]
        
        # Check if this is a valid API
        if api_name not in ["mockaroo", "randomapi", "randomuser"]:
            await message.reply_text(
                "<b>⚠️ Invalid API name!</b>\n"
                "<b>Available APIs: mockaroo, randomapi, randomuser</b>"
            )
            return
            
        # Update the API key
        result = API_KEYS.update_one(
            {"api_name": api_name},
            {"$set": {
                "api_key": api_key,
                "enabled": True,
                "remaining_quota": 1000 if api_name != "randomuser" else float('inf'),
                "last_reset": "",
                "success_rate": 0
            }}
        )
        
        if result.modified_count > 0:
            await message.reply_text(
                f"<b>✅ Successfully set API key for {api_name}!</b>\n"
                f"<b>API is now enabled.</b>"
            )
        else:
            await message.reply_text(
                f"<b>⚠️ Failed to update API key for {api_name}.</b>"
            )
            
    except Exception as e:
        import traceback
        await error_log(traceback.format_exc())
        await message.reply_text(f"<b>❌ Error: {str(e)}</b>")

@Client.on_message(filters.command("viewapis", [".", "/"]) & filters.user([7325746010]))
async def cmd_viewapis(client, message):
    """Admin command to view configured APIs."""
    try:
        api_records = list(API_KEYS.find({}, {"_id": 0}))
        
        if not api_records:
            await message.reply_text("<b>No APIs configured yet.</b>")
            return
            
        response = "<b>📊 API Configuration Status:</b>\n\n"
        
        for api in api_records:
            status = "✅ Enabled" if api.get("enabled", False) else "❌ Disabled"
            key_status = "🔑 Set" if api.get("api_key") else "🔒 Not Set"
            usage = api.get("usage_count", 0)
            quota = api.get("remaining_quota", "∞")
            success_rate = round(api.get("success_rate", 0) * 100, 2)
            
            response += f"<b>🔹 {api['api_name'].upper()}:</b>\n"
            response += f"   <b>Status:</b> {status}\n"
            response += f"   <b>API Key:</b> {key_status}\n"
            response += f"   <b>Usage Count:</b> {usage}\n"
            response += f"   <b>Remaining Quota:</b> {quota}\n"
            response += f"   <b>Success Rate:</b> {success_rate}%\n\n"
            
        await message.reply_text(response)
        
    except Exception as e:
        import traceback
        await error_log(traceback.format_exc())
        await message.reply_text(f"<b>❌ Error: {str(e)}</b>")

@Client.on_message(filters.command("toggleapi", [".", "/"]) & filters.user([7325746010]))
async def cmd_toggleapi(client, message):
    """Admin command to enable/disable an API."""
    try:
        parts = message.text.split(" ")
        if len(parts) < 3:
            await message.reply_text(
                "<b>Usage: /toggleapi [api_name] [on|off]</b>\n"
                "<b>Example: /toggleapi mockaroo on</b>\n"
                "<b>Available APIs: mockaroo, randomapi, randomuser</b>"
            )
            return
            
        api_name = parts[1].lower()
        status = parts[2].lower()
        
        if api_name not in ["mockaroo", "randomapi", "randomuser"]:
            await message.reply_text(
                "<b>⚠️ Invalid API name!</b>\n"
                "<b>Available APIs: mockaroo, randomapi, randomuser</b>"
            )
            return
            
        if status not in ["on", "off"]:
            await message.reply_text(
                "<b>⚠️ Invalid status! Use 'on' or 'off'.</b>"
            )
            return
            
        enabled = status == "on"
        
        result = API_KEYS.update_one(
            {"api_name": api_name},
            {"$set": {"enabled": enabled}}
        )
        
        if result.modified_count > 0:
            status_text = "enabled" if enabled else "disabled"
            await message.reply_text(
                f"<b>✅ Successfully {status_text} {api_name} API!</b>"
            )
        else:
            await message.reply_text(
                f"<b>⚠️ No changes made. {api_name} API was already {status}.</b>"
            )
            
    except Exception as e:
        import traceback
        await error_log(traceback.format_exc())
        await message.reply_text(f"<b>❌ Error: {str(e)}</b>")

@Client.on_message(filters.command("apiusage", [".", "/"]) & filters.user([7325746010]))
async def cmd_apiusage(client, message):
    """Admin command to view API usage statistics."""
    try:
        parts = message.text.split(" ")
        api_name = parts[1].lower() if len(parts) > 1 else None
        
        query = {"api_name": api_name} if api_name else {}
        api_records = list(API_KEYS.find(query, {"_id": 0}))
        
        if not api_records:
            await message.reply_text(
                "<b>⚠️ No API data found!</b>" + 
                (f" (for {api_name})" if api_name else "")
            )
            return
            
        response = "<b>📈 API Usage Statistics:</b>\n\n"
        
        for api in api_records:
            name = api.get("api_name", "unknown").upper()
            usage = api.get("usage_count", 0)
            quota = api.get("remaining_quota", "∞")
            success_rate = round(api.get("success_rate", 0) * 100, 2)
            
            # Calculate usage percentage
            if quota != "∞" and quota > 0:
                used = 1000 - quota  # Assuming default quota is 1000
                percentage = min(round((used / 1000) * 100, 2), 100)
                quota_text = f"{quota}/1000 ({percentage}% used)"
            else:
                quota_text = "∞ (Unlimited)"
                
            response += f"<b>🔹 {name}:</b>\n"
            response += f"   <b>Total Requests:</b> {usage}\n"
            response += f"   <b>Remaining Quota:</b> {quota_text}\n"
            response += f"   <b>Success Rate:</b> {success_rate}%\n\n"
            
        await message.reply_text(response)
        
    except Exception as e:
        import traceback
        await error_log(traceback.format_exc())
        await message.reply_text(f"<b>❌ Error: {str(e)}</b>")

@Client.on_message(filters.command("testapi", [".", "/"]) & filters.user([7325746010]))
async def cmd_testapi(client, message):
    """Admin command to test API functionality."""
    try:
        parts = message.text.split(" ")
        if len(parts) < 3:
            await message.reply_text(
                "<b>Usage: /testapi [api_name] [country_code]</b>\n"
                "<b>Example: /testapi mockaroo us</b>\n"
                "<b>Available APIs: mockaroo, randomapi, randomuser</b>"
            )
            return
            
        api_name = parts[1].lower()
        country_code = parts[2].lower()
        
        # Import the API testing function from enhanced_fake module
        from BOT.tools.enhanced_fake_api import test_api_request
        
        # Notify user that test is in progress
        status_msg = await message.reply_text("<b>🔄 Testing API request, please wait...</b>")
        
        # Perform the API test
        success, data = await test_api_request(api_name, country_code)
        
        if success:
            response = f"<b>✅ API Test Successful for {api_name.upper()} ({country_code})!</b>\n\n"
            response += "<b>Sample Data Fields:</b>\n"
            
            # Show a sample of the data (first 10 fields)
            field_count = 0
            for key, value in data.items():
                if field_count >= 10:
                    response += "<b>...and more fields</b>\n"
                    break
                    
                response += f"<b>🔹 {key}:</b> {value}\n"
                field_count += 1
                
            # Update success rate in database
            await update_api_success_rate(api_name, True)
            
        else:
            response = f"<b>❌ API Test Failed for {api_name.upper()} ({country_code})!</b>\n\n"
            response += f"<b>Error:</b> {data}\n"
            response += f"<b>Please check your API key and try again.</b>"
            
            # Update success rate in database
            await update_api_success_rate(api_name, False)
        
        # Update the status message with the result
        await client.edit_message_text(
            chat_id=status_msg.chat.id,
            message_id=status_msg.id,
            text=response
        )
        
    except Exception as e:
        import traceback
        await error_log(traceback.format_exc())
        await message.reply_text(f"<b>❌ Error: {str(e)}</b>")

@Client.on_message(filters.command("setschema", [".", "/"]) & filters.user([7325746010]))
async def cmd_setschema(client, message):
    """Admin command to set a schema mapping for a country."""
    try:
        parts = message.text.split(" ")
        if len(parts) < 4:
            await message.reply_text(
                "<b>Usage: /setschema [api_name] [country_code] [schema_id]</b>\n"
                "<b>Example: /setschema mockaroo jp japan-schema-12345</b>\n"
                "<b>Use 'default' as country_code for the default schema.</b>"
            )
            return
            
        api_name = parts[1].lower()
        country_code = parts[2].lower()
        schema_id = parts[3]
        
        if api_name not in ["mockaroo", "randomapi"]:
            await message.reply_text(
                "<b>⚠️ Invalid API name!</b>\n"
                "<b>Available APIs for schemas: mockaroo, randomapi</b>"
            )
            return
            
        # Update or insert the schema mapping
        result = SCHEMA_MAPPINGS.update_one(
            {
                "api_name": api_name,
                "country_code": country_code
            },
            {
                "$set": {
                    "schema_id": schema_id,
                    "last_updated": datetime.now().isoformat()
                }
            },
            upsert=True
        )
        
        if result.modified_count > 0 or result.upserted_id:
            await message.reply_text(
                f"<b>✅ Successfully set schema mapping!</b>\n"
                f"<b>API:</b> {api_name}\n"
                f"<b>Country:</b> {country_code}\n"
                f"<b>Schema ID:</b> {schema_id}"
            )
        else:
            await message.reply_text(
                f"<b>⚠️ No changes made. Schema was already set to this value.</b>"
            )
            
    except Exception as e:
        import traceback
        await error_log(traceback.format_exc())
        await message.reply_text(f"<b>❌ Error: {str(e)}</b>")