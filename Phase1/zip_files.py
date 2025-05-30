#!/usr/bin/env python3
import os
import zipfile
from datetime import datetime

def create_zip_archive():
    """
    Create a ZIP archive of the current directory,
    excluding certain directories and files.
    """
    # Get timestamp for the zip filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    zip_filename = f"code_backup_{timestamp}.zip"
    
    # Directories and files to exclude
    exclude_dirs = {'.git', '__pycache__', '.cache', '.upm', '.local', '.pythonlibs'}
    exclude_files = {'git_push.py', 'github_upload.py', 'zip_files.py', zip_filename}
    
    print(f"Creating ZIP archive: {zip_filename}")
    
    # Create the ZIP file
    with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
        file_count = 0
        # Walk through all files and directories
        for root, dirs, files in os.walk('.'):
            # Exclude directories
            dirs[:] = [d for d in dirs if d not in exclude_dirs]
            
            # Process files
            for file in files:
                if file in exclude_files:
                    continue
                
                file_path = os.path.join(root, file)
                # Create archive path (remove leading ./)
                arcname = file_path[2:] if file_path.startswith('./') else file_path
                
                try:
                    zipf.write(file_path, arcname)
                    file_count += 1
                    if file_count % 10 == 0:
                        print(f"Added {file_count} files to archive...")
                except Exception as e:
                    print(f"Error adding {file_path}: {str(e)}")
    
    print(f"\nArchive created with {file_count} files: {zip_filename}")
    print("\nNow you can:")
    print(f"1. Download this ZIP file ({zip_filename})")
    print("2. Go to GitHub.com and create a new repository")
    print("3. Upload this ZIP file or extract it and upload the individual files")
    
    return zip_filename

if __name__ == "__main__":
    print("=== Code Backup Tool ===")
    zip_file = create_zip_archive()
    print("\nDone!")