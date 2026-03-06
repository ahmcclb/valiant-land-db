import os
import json
from sync_service import ValiantLandSync

# Load config
with open('config.json') as f:
    config = json.load(f)

sync = ValiantLandSync(config['local_db'], config['supabase_url'], config['supabase_key'])

# Test 1: Check if files are found
print("=== Checking for files ===")
upload_dirs = ['static/uploads/photos', 'static/uploads/documents']
total_files = 0
for upload_dir in upload_dirs:
    if os.path.exists(upload_dir):
        print(f"Directory exists: {upload_dir}")
        for root, dirs, files in os.walk(upload_dir):
            for file in files:
                print(f"  Found: {os.path.join(root, file)}")
                total_files += 1
    else:
        print(f"Directory MISSING: {upload_dir}")

print(f"\nTotal files found: {total_files}")



# Test 2: Try to sync files
if total_files > 0:
    print("\n=== Attempting file sync ===")
    result = sync.sync_files('to_cloud')
    print(f"Result: {result}")
else:
    print("\nNo files to sync")
    
# Test database sync (includes links)
print("\n=== Testing database sync ===")
result = sync.sync_database('to_cloud')
print(f"Database sync result: {result}")