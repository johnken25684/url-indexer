import os
import json
import gspread
import requests
import datetime
from github import Github
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# --- CONFIGURATION ---
BATCH_SIZE = 200
SHEET_NAME = 'Sheet1' # The name of the tab in your Google Sheet
PING_SERVICES = [
    'http://rpc.pingomatic.com/',
    'http://rpc.twingly.com/',
    'http://ping.blo.gs/',
    'http://ping.feedburner.com'
]

# --- MAIN SCRIPT LOGIC ---
def main():
    print("Starting the indexing process...")

    # --- 1. AUTHENTICATION ---
    try:
        # Authenticate with Google (for Sheets and Blogger)
        creds_json = json.loads(os.environ['GOOGLE_CREDENTIALS'])
        scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/blogger']
        creds = Credentials.from_service_account_info(creds_json, scopes=scopes)
        gs = gspread.authorize(creds)
        blogger_service = build('blogger', 'v3', credentials=creds)

        # Authenticate with GitHub (for Gists)
        g = Github(os.environ['GITHUB_TOKEN'])
        
        print("Authentication successful.")
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
            if str(row['Status']).strip() == '':
                # Store the URL and its row number for later updates
                unprocessed_urls.append({'url': row['URL'], 'row_num': idx + 2})
        
        if not unprocessed_urls:
            print("No new URLs to process. Exiting.")
            return
            
        # Get the next batch
        batch = unprocessed_urls[:BATCH_SIZE]
        print(f"Found {len(unprocessed_urls)} unprocessed URLs. Taking the next batch of {len(batch)}.")
        
        # Lock the batch by updating their status
        for item in batch:
            sheet.update_cell(item['row_num'], 2, 'Processing')
            
    except Exception as e:
        print(f"Error reading from Google Sheet: {e}")
        return

    # --- 3. CREATE BLOGGER POST ---
    try:
        blog_id = os.environ['BLOG_ID']
        
        # Create HTML content for the post
        post_title = f"Link Index Report: {datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC"
        post_content = "<h3>New Resources Discovered:</h3><ul>"
        for item in batch:
            post_content += f'<li><a href="{item["url"]}">{item["url"]}</a></li>'
        post_content += "</ul>"
        
        post_body = {
            "title": post_title,
            "content": post_content
        }
        
        # Publish the post
        posts = blogger_service.posts()
        insert_request = posts.insert(blogId=blog_id, body=post_body, isDraft=False)
        post = insert_request.execute()
        post_url = post['url']
        print(f"Successfully created Blogger post: {post_url}")
        
    except Exception as e:
        print(f"Error creating Blogger post: {e}")
        # Mark as error and exit
        for item in batch:
            sheet.update_cell(item['row_num'], 2, 'Error - Blogger')
        return

    # --- 4. CREATE GIST RSS FEED ---
    try:
        rss_content = '<?xml version="1.0" encoding="UTF-8"?><rss version="2.0"><channel><title>Link Feed</title>'
        for item in batch:
            rss_content += f'<item><title>{item["url"]}</title><link>{item["url"]}</link></item>'
        rss_content += '</channel></rss>'
        
        gist_filename = 'feed.xml'
        gist_description = f'RSS Feed generated on {datetime.datetime.utcnow().isoformat()}'
        
        # Create the Gist
        gist = g.get_user().create_gist(public=True, files={gist_filename: {"content": rss_content}}, description=gist_description)
        gist_url = gist.files[gist_filename].raw_url
        print(f"Successfully created Gist RSS feed: {gist_url}")
        
    except Exception as e:
        print(f"Error creating GitHub Gist: {e}")
        # Mark as error and exit
        for item in batch:
            sheet.update_cell(item['row_num'], 2, 'Error - Gist')
        return

    # --- 5. PING SERVICES ---
    print("Pinging services...")
    urls_to_ping = [post_url, gist_url]
    for service in PING_SERVICES:
        for url in urls_to_ping:
            try:
                payload = f'<?xml version="1.0"?><methodCall><methodName>weblogUpdates.ping</methodName><params><param><value>{post_title}</value></param><param><value>{url}</value></param></params></methodCall>'
                headers = {'Content-Type': 'application/xml'}
                response = requests.post(service, data=payload, timeout=5)
                if response.status_code == 200:
                    print(f"  - Pinged {service} for {url}")
                else:
                    print(f"  - Failed to ping {service} for {url} - Status: {response.status_code}")
            except Exception as e:
                print(f"  - Error pinging {service}: {e}")

    # --- 6. MARK AS COMPLETED ---
    for item in batch:
        sheet.update_cell(item['row_num'], 2, 'Completed')
        
    print(f"Successfully processed {len(batch)} URLs. Job finished.")

if __name__ == "__main__":
    main()
