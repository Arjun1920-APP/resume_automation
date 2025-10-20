# runtime: python3.11

import os
from process_emails_drive import process_imap_and_upload_to_drive
from vercel import Response

def handler(request, response):
    # Read environment variables (already set in Vercel dashboard)
    # If your script reads os.environ, it will work automatically
    try:
        process_imap_and_upload_to_drive(force=False, days=1)
        return response.json({"status": "success"})
    except Exception as e:
        return response.json({"status": "error", "message": str(e)})
