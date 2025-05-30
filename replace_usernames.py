import os
import re

def replace_in_file(file_path):
    try:
        # Read the file content
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
            content = file.read()
        
        # Perform replacements
        modified = content.replace('@amkuush', '@amkuush')
        modified = modified.replace('@amkuushcheckerbot', '@VoidVisaRobot')
        modified = modified.replace('@amkuushϟ', '@amkuush')
        
        # Only write if changes were made
        if content != modified:
            with open(file_path, 'w', encoding='utf-8') as file:
                file.write(modified)
            return True
        return False
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return False

def process_directory(directory):
    changes_made = 0
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.py'):
                file_path = os.path.join(root, file)
                if replace_in_file(file_path):
                    changes_made += 1
                    print(f"Updated: {file_path}")
    return changes_made

# Process both main directories
changes = process_directory('.')
print(f"Total files updated: {changes}")