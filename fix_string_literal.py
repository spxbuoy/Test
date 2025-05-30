#!/usr/bin/env python3

# Read the file
with open("BOT/tools/scrapper.py", "r") as f:
    lines = f.readlines()

# Fix the error
for i in range(len(lines)):
    if "await message.reply_text(\"<b>Error ❌" in lines[i]:
        # Replace broken newline with proper string continuation
        lines[i] = lines[i].replace("\"<b>Error ❌\n", "\"<b>Error ❌\\n\\n")
    if "await message.reply_text(f\"<b>Error ❌" in lines[i]:
        # Replace broken newline with proper string continuation
        lines[i] = lines[i].replace("f\"<b>Error ❌\n", "f\"<b>Error ❌\\n\\n")

# Write back to the file
with open("BOT/tools/scrapper.py", "w") as f:
    f.writelines(lines)

print("Fixed string literals in scrapper.py")
