import os
import json
import gspread
import requests
import datetime
from git import Repo
from google.oauth2.service_account import Credentials

# --- CONFIGURATION ---
BATCH_SIZE = 200
SHEET_NAME = 'Sheet1' 
PING_SERVICES = [
    'http://rpc.pingomatic.com/',
    'http://rpc.twingly.com/',
]
REPO_PATH = '.' # The script will run in the repository's root

# --- HELPER FUNCTION TO CREATE HTML ---
def create_post_html(title, batch_of_urls):
    """Generates the HTML content for a new post page."""
    links_html = ""
    for item in batch_of_urls:
        links_html += f'      <li><a href="{item["url"]}">{item["url"]}</a></li>\n'
    
    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{title}</title>
</head>
<body>
    <h1>{title}</h1>
    <ul>
{links_html}
    </ul>
</body>
</html>"""
    return html_content

# --- MAIN SCRIPT LOGIC ---
def main():
    print("Starting the indexing process...")

    # --- 1. AUTHENTICATION (Google Sheets) ---
    try:
        creds_json = json.loads(os.environ['GOOGLE_CREDENTIALS'])
        scopes = ['https://www.googleapis.com/auth/spreadsheets']
        creds = Credentials.from_service_account_info(creds_json, scopes=scopes)
        gs = gspread.authorize(creds)
        print("Google Sheets authentication successful.")
    except Exception as e:
        print(f"Error during authentication: {e}")
        return

    # --- 2. GET URLS FROM GOOGLE SHEET ---
    try:
        sheet_id = os.environ['SHEET_ID']
        sheet = gs.open_by_key(sheet_id).worksheet(SHEET_NAME)
        all_records = sheet.get_all_records()
        unprocessed_urls = []
        for idx, row in enumerate(all_records):
            if str(row.get('Status', '')).strip() == '':
                unprocessed_urls.append({'url': row['URL'], 'row_num': idx + 2})
        
        if not unprocessed_urls:
            print("No new URLs to process. Exiting.")
            return
            
        batch = unprocessed_urls[:BATCH_SIZE]
        print(f"Found {len(unprocessed_urls)} URLs. Processing a batch of {len(batch)}.")
        
        for item in batch:
            sheet.update_cell(item['row_num'], 2, 'Processing')
            
    except Exception as e:
        print(f"Error reading from Google Sheet: {e}")
        return

    # --- 3. CREATE NEW HTML POST FILE ---
    post_title = f"Link Report: {datetime.datetime.now().strftime('%Y-%m-%d-%H%M%S')}"
    file_name = f"{datetime.datetime.now().strftime('%Y-%m-%d-%H%M%S')}.html"
    
    # Create a 'posts' directory if it doesn't exist
    posts_dir = os.path.join(REPO_PATH, 'posts')
    os.makedirs(posts_dir, exist_ok=True)
    
    file_path = os.path.join(posts_dir, file_name)
    html_to_write = create_post_html(post_title, batch)
    
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(html_to_write)
    print(f"Successfully created HTML file: {file_path}")

    # --- 4. PUSH CHANGES TO GITHUB ---
    try:
        repo = Repo(REPO_PATH)
        # Add the new file
        repo.index.add([file_path])
        
        # You may want to update an index.html file here as well
        # For simplicity, we are just adding the new post for now.

        # Commit the changes
        commit_message = f"Add new link report: {post_title}"
        repo.index.commit(commit_message)
        
        # Push to the origin
        origin = repo.remote(name='origin')
        origin.push()
        print(f"Successfully pushed new post to the repository.")
        
    except Exception as e:
        print(f"Error pushing to GitHub: {e}")
        for item in batch:
            sheet.update_cell(item['row_num'], 2, 'Error - GitHub Push')
        return

    # --- 5. PING THE NEW URL (Optional but Recommended) ---
    # The URL will be based on your GitHub pages structure
    repo_url = os.environ.get('GITHUB_REPOSITORY', 'your-username/your-repo').split('/')
    live_url = f"https://{repo_url[0]}.github.io/{repo_url[1]}/posts/{file_name}"
    print(f"Pinging live URL: {live_url}")

    for service in PING_SERVICES:
        try:
            payload = f'<?xml version="1.0"?><methodCall><methodName>weblogUpdates.ping</methodName><params><param><value>{post_title}</value></param><param><value>{live_url}</value></param></params></methodCall>'
            requests.post(service, data=payload, headers={'Content-Type': 'application/xml'}, timeout=5)
        except Exception as e:
            print(f"  - Error pinging {service}: {e}")

    # --- 6. MARK AS COMPLETED ---
    for item in batch:
        sheet.update_cell(item['row_num'], 2, 'Completed')
        
    print(f"Successfully processed {len(batch)} URLs. Job finished.")

if __name__ == "__main__":
    main()
