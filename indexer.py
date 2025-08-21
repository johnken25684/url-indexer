import os
import json
import gspread
import requests
import datetime
from github import Github
from google.oauth2.service_account import Credentials

# --- CONFIGURATION ---
BATCH_SIZE = 200
SHEET_NAME = 'Sheet1' 
PING_SERVICES = [
    'http://rpc.pingomatic.com/',
    'http://rpc.twingly.com/',
]

# --- WORDPRESS POSTING FUNCTION ---
def post_to_wordpress(post_title, post_content):
    """Posts content to a WordPress.com blog using the public REST API."""
    site_domain = os.environ['WP_URL']  # e.g., indexhub5.wordpress.com
    wp_user = os.environ['WP_USER']
    wp_password = os.environ['WP_PASSWORD']
    
    # Use the official public API endpoint for wordpress.com sites
    api_url = f"https://public-api.wordpress.com/rest/v1.1/sites/{site_domain}/posts/new"
    
    headers = {
        'Content-Type': 'application/json',
        # For public-api, we use a Bearer token. We'll get this using the app password.
    }
    
    # First, get an access token using the application password
    token_url = 'https://public-api.wordpress.com/oauth2/token'
    token_data = {
        'client_id': wp_user, # For app passwords, user is the client_id
        'client_secret': wp_password,
        'grant_type': 'password',
        'username': wp_user,
        'password': wp_password,
    }
    
    try:
        token_res = requests.post(token_url, data=token_data)
        token_res.raise_for_status() # Will raise an exception for HTTP errors
        access_token = token_res.json()['access_token']
    except requests.exceptions.RequestException as e:
        print(f"Error getting WordPress access token: {e}")
        print(f"Response body: {e.response.text}")
        return None

    # Now, use the access token to post
    headers['Authorization'] = f'Bearer {access_token}'
    
    post_data = {
        'title': post_title,
        'content': post_content,
    }
    
    try:
        response = requests.post(api_url, headers=headers, json=post_data)
        response.raise_for_status() # Will raise an exception for HTTP errors
        post_info = response.json()
        print(f"Successfully created WordPress post: {post_info['URL']}")
        return post_info['URL']
    except requests.exceptions.RequestException as e:
        print(f"Error creating WordPress post: {e}")
        print(f"Response body: {e.response.text}")
        return None

# --- MAIN SCRIPT LOGIC (REMAINS THE SAME) ---
def main():
    print("Starting the indexing process...")

    # --- 1. AUTHENTICATION (Google and GitHub) ---
    try:
        creds_json = json.loads(os.environ['GOOGLE_CREDENTIALS'])
        scopes = ['https://www.googleapis.com/auth/spreadsheets']
        creds = Credentials.from_service_account_info(creds_json, scopes=scopes)
        gs = gspread.authorize(creds)
        # Use GT_TOKEN as specified by user
        g = Github(os.environ['GT_TOKEN'])
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
                unprocessed_urls.append({'url': row['URL'], 'row_num': idx + 2})
        
        if not unprocessed_urls:
            print("No new URLs to process. Exiting.")
            return
            
        batch = unprocessed_urls[:BATCH_SIZE]
        print(f"Found {len(unprocessed_urls)} unprocessed URLs. Taking the next batch of {len(batch)}.")
        
        for item in batch:
            sheet.update_cell(item['row_num'], 2, 'Processing')
            
    except Exception as e:
        print(f"Error reading from Google Sheet: {e}")
        return

    # --- 3. CREATE WORDPRESS POST ---
    post_title = f"Link Index Report: {datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC"
    post_content = "<h3>New Resources Discovered:</h3><ul>"
    for item in batch:
        post_content += f'<li><a href="{item["url"]}">{item["url"]}</a></li>'
    post_content += "</ul>"
    
    post_url = post_to_wordpress(post_title, post_content)
    
    if not post_url:
        print("Failed to create WordPress post. Aborting.")
        for item in batch:
            sheet.update_cell(item['row_num'], 2, 'Error - WordPress')
        return

    # --- 4. CREATE GIST RSS FEED ---
    try:
        rss_content = '<?xml version="1.0" encoding="UTF-8"?><rss version="2.0"><channel><title>Link Feed</title>'
        for item in batch:
            rss_content += f'<item><title>{item["url"]}</title><link>{item["url"]}</link></item>'
        rss_content += '</channel></rss>'
        
        gist = g.get_user().create_gist(public=True, files={'feed.xml': {"content": rss_content}})
        gist_url = gist.files['feed.xml'].raw_url
        print(f"Successfully created Gist RSS feed: {gist_url}")
        
    except Exception as e:
        print(f"Error creating GitHub Gist: {e}")
        gist_url = None # Ensure gist_url is defined

    # --- 5. PING SERVICES ---
    print("Pinging services...")
    urls_to_ping = [post_url, gist_url] if gist_url else [post_url]
    for service in PING_SERVICES:
        for url in urls_to_ping:
            try:
                payload = f'<?xml version="1.0"?><methodCall><methodName>weblogUpdates.ping</methodName><params><param><value>{post_title}</value></param><param><value>{url}</value></param></params></methodCall>'
                requests.post(service, data=payload, headers={'Content-Type': 'application/xml'}, timeout=5)
            except Exception as e:
                print(f"  - Error pinging {service}: {e}")

    # --- 6. MARK AS COMPLETED ---
    for item in batch:
        sheet.update_cell(item['row_num'], 2, 'Completed')
        
    print(f"Successfully processed {len(batch)} URLs. Job finished.")

if __name__ == "__main__":
    main()
