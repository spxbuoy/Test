import re

# File to process
file_path = 'FUNC/scraperfunc.py'

try:
    # Read the file content
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
        content = file.read()
    
    # Fix the issue with special character ϟ
    modified = re.sub(r'@amkuushϟ', '@amkuush', content)
    
    # Write the changes back
    with open(file_path, 'w', encoding='utf-8') as file:
        file.write(modified)
    
    print(f"Successfully updated {file_path}")

except Exception as e:
    print(f"Error processing {file_path}: {e}")