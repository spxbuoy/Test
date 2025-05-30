from pyrogram import Client, filters
from FUNC.usersdb_func import *


@Client.on_message(filters.command("credits", [".", "/"]))
async def cmd_credit(Client, message):
    try:
        user_id = str(message.from_user.id)
        regdata = await getuserinfo(user_id)
        regdata = str(regdata)
        if regdata == "None":
            resp = f"""<b>
⚠️ Unregistered User ⚠️

Welcome to ª𝗠𝗸𝗨𝘀𝗛𝘅𝗖𝗵𝗞!
You need to register before accessing this feature.

Type /register to continue.
</b>"""
            await message.reply_text(resp, message.id)
            return

        getuser    = await getuserinfo(user_id)
        status     = getuser["status"]
        credit     = getuser["credit"]
        plan       = getuser["plan"]
        first_name = str(message.from_user.first_name)

        resp = f"""<b>
💳 ª𝗠𝗸𝗨𝘀𝗛𝘅𝗖𝗵𝗞 Credits
━━━━━━━━━━━━━━
👤 Name: {first_name}
💰 Credits: {credit}
📊 Status: {status}
🔐 Plan: {plan}

Want More? Type /buy to get more credits.
    </b>"""
        await message.reply_text(resp, message.id)
    except:
        import traceback
        await error_log(traceback.format_exc())
