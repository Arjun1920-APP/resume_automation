#!/usr/bin/env python3
"""
CV automation — local (.eml) and live IMAP -> download CVs -> upload to Drive (date folders) ->
single master Excel with 3 sheets (Vendor / Candidate / Referral), dedupe, processed log.
"""

import os
import re
import email
import imaplib
import argparse
import urllib.parse
from email.header import decode_header
from datetime import datetime, timedelta
from dotenv import load_dotenv
import pandas as pd
from openpyxl import load_workbook

# Google Drive
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

load_dotenv()

# -----------------------------
# Config (from .env)
# -----------------------------
USE_IMAP = os.getenv("USE_IMAP", "false").lower() in ("1", "true", "yes")
SAMPLE_FOLDER = os.getenv("SAMPLE_FOLDER", "sample_emails")
DOWNLOAD_FOLDER = os.getenv("DOWNLOAD_FOLDER", "CVs")
EXCEL_FILE = os.getenv("EXCEL_FILE", "CV_Tracker.xlsx")
PROCESSED_FILE = os.getenv("PROCESSED_FILE", "Processed_Emails.xlsx")
DRIVE_ROOT_FOLDER = os.getenv("DRIVE_ROOT_FOLDER", "HR_CVs")

IMAP_SERVER = os.getenv("IMAP_SERVER", "imap.gmail.com")
EMAIL_ACCOUNT = os.getenv("EMAIL", "")
EMAIL_PASSWORD = os.getenv("PASSWORD", "")
MAIL_FOLDER = os.getenv("MAIL_FOLDER", "INBOX")

SCOPES = ['https://www.googleapis.com/auth/drive.file']  # create/update files created by the app

# -----------------------------
# Helpers
# -----------------------------
def decode_mime_words(s):
    """Safely decode MIME-encoded headers (handles unknown-8bit etc.)."""
    if not s:
        return ""
    parts = decode_header(s)
    out = ""
    for b, enc in parts:
        if isinstance(b, bytes):
            try:
                if not enc or enc.lower() in ("unknown-8bit", "x-unknown", "ascii"):
                    out += b.decode("utf-8", errors="ignore")
                else:
                    out += b.decode(enc, errors="ignore")
            except Exception:
                out += b.decode("utf-8", errors="ignore")
        else:
            out += str(b)
    return out

def safe_filename(name):
    """Return a filesystem-safe filename."""
    return re.sub(r"[^A-Za-z0-9._-]", "_", name).strip("_")

def build_mail_url_from_message_id(message_id):
    """Build Gmail search URL from Message-ID (works when message-id exists in Gmail)."""
    if not message_id:
        return ""
    mid = message_id.strip().strip("<>").strip()
    mid_enc = urllib.parse.quote(mid, safe="")
    return f"https://mail.google.com/mail/u/0/#search/rfc822msgid:{mid_enc}"

# -----------------------------
# Google Drive helpers (OAuth InstalledAppFlow)
# -----------------------------
def get_drive_service():
    """Return Google Drive service (will open browser for OAuth on first run)."""
    creds = None
    if os.path.exists("token.json"):
        try:
            creds = Credentials.from_authorized_user_file("token.json", SCOPES)
        except Exception:
            creds = None
    if not creds or not creds.valid:
        flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
        creds = flow.run_local_server(port=0)
        with open("token.json", "w") as f:
            f.write(creds.to_json())
    return build("drive", "v3", credentials=creds)

def find_folder(service, name, parent_id=None):
    """Find Drive folder by name (optionally under parent). Returns file dict or None."""
    safe_name = name.replace("'", "\\'")
    q = "mimeType='application/vnd.google-apps.folder' and trashed=false and name='{}'".format(safe_name)
    if parent_id:
        q += " and '{}' in parents".format(parent_id)
    resp = service.files().list(q=q, fields="files(id,name)").execute()
    files = resp.get("files", [])
    return files[0] if files else None

def create_folder(service, name, parent_id=None):
    """Create Drive folder and return created file dict."""
    body = {"name": name, "mimeType": "application/vnd.google-apps.folder"}
    if parent_id:
        body["parents"] = [parent_id]
    file = service.files().create(body=body, fields="id,name").execute()
    return file

def ensure_drive_root(service, root_name):
    found = find_folder(service, root_name)
    if found:
        return found["id"]
    newf = create_folder(service, root_name)
    return newf["id"]

def ensure_date_folder(service, root_id, date_str):
    found = find_folder(service, date_str, parent_id=root_id)
    if found:
        return found["id"]
    folder = create_folder(service, date_str, parent_id=root_id)
    return folder["id"]

def upload_file_to_drive(service, local_path, drive_name, parent_id):
    """Upload a local file to Drive parent folder. Returns (file_id, view_url)."""
    media = MediaFileUpload(local_path, resumable=True)
    body = {"name": drive_name, "parents": [parent_id]}
    created = service.files().create(body=body, media_body=media, fields="id").execute()
    fid = created.get("id")
    return fid, f"https://drive.google.com/file/d/{fid}/view"

def find_file_in_folder(service, name, parent_id):
    safe_name = name.replace("'", "\\'")
    q = "trashed=false and name='{}' and '{}' in parents".format(safe_name, parent_id)
    resp = service.files().list(q=q, fields="files(id,name)").execute()
    files = resp.get("files", [])
    return files[0] if files else None

def update_drive_file(service, file_id, local_path):
    media = MediaFileUpload(local_path, resumable=True)
    updated = service.files().update(fileId=file_id, media_body=media).execute()
    return updated.get("id")

# -----------------------------
# Processed IDs (dedupe)
# -----------------------------
def load_processed_ids(path):
    if not os.path.exists(path):
        return set()
    try:
        df = pd.read_excel(path)
        if "ID" in df.columns:
            return set(df["ID"].astype(str))
        return set()
    except Exception:
        return set()

def save_processed_ids(path, ids_set):
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    df = pd.DataFrame(sorted(list(ids_set)), columns=["ID"])
    df.to_excel(path, index=False)

# -----------------------------
# Excel handling (3 sheets with different columns)
# -----------------------------
VENDOR_COLS = ["Date","Vendor Name","Role Applied For","Candidate Name","Candidate Email","Phone Number","Resume Link","Mail URL","Source File","Sr. No.","Current Company","Tenure","Previous Company","Tenure","Highest Education","Second Highest Education","Designation","AUM","AUM Mix - Specify products","Size of Book","Current CTC","Notice Period","Role"]
CANDIDATE_COLS = ["Date","Candidate Name","Candidate Email","Phone Number","Role Applied For","LinkedIn","Resume Link","Mail URL","Source File"]
REFERRAL_COLS = ["Date","Candidate Name","Candidate Email","Referred By","Referrer Email","Resume Link","Mail URL","Source File"]

def ensure_parent_dir_for_file(path):
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)

def append_to_excel_by_sheet(local_excel, vendor_rows, candidate_rows, referral_rows):
    """
    Append new rows into the three sheets; if sheet exists, load old, concat, dedupe and replace the sheet.
    Handles missing or corrupted Excel by recreating a clean template workbook.
    """
    ensure_parent_dir_for_file(local_excel)
    
    def create_base_file():
        print(f"Creating new Excel file at: {local_excel}")
        with pd.ExcelWriter(local_excel, engine="openpyxl", mode="w") as writer:
            pd.DataFrame(columns=VENDOR_COLS).to_excel(writer, sheet_name="Vendor", index=False)
            pd.DataFrame(columns=CANDIDATE_COLS).to_excel(writer, sheet_name="Candidate", index=False)
            pd.DataFrame(columns=REFERRAL_COLS).to_excel(writer, sheet_name="Referral", index=False)

    if not os.path.exists(local_excel):
        create_base_file()

    def load_sheet_df(sheet_name, cols):
        try:
            df_old = pd.read_excel(local_excel, sheet_name=sheet_name)
            for c in cols:
                if c not in df_old.columns:
                    df_old[c] = ""
            return df_old[cols]
        except Exception:
            return pd.DataFrame(columns=cols)

    # Load all existing emails from all sheets for global deduplication
    existing_emails = set()
    if os.path.exists(local_excel):
        try:
            xls = pd.ExcelFile(local_excel)
            for sheet_name in xls.sheet_names:
                df = pd.read_excel(xls, sheet_name=sheet_name)
                if "Candidate Email" in df.columns:
                    # Robustly handle data types, converting to string first
                    emails = df["Candidate Email"].astype(str).str.lower().str.strip().tolist()
                    existing_emails.update(emails)
        except Exception as e:
            print(f"Warning: Could not load existing emails from {local_excel} for global dedupe: {e}")
            try:
                os.remove(local_excel)
                create_base_file()
                existing_emails = set()
            except:
                pass

    # Process new vendor rows
    if vendor_rows:
        df_new = pd.DataFrame(vendor_rows)
        df_new = df_new.reindex(columns=VENDOR_COLS, fill_value="")
        
        # Filter new rows against existing emails
        df_new['Candidate Email_norm'] = df_new['Candidate Email'].astype(str).str.lower().str.strip()
        new_unique_rows = df_new[~df_new['Candidate Email_norm'].isin(existing_emails)].drop(columns=['Candidate Email_norm'])
        
        if not new_unique_rows.empty:
            df_old = load_sheet_df("Vendor", VENDOR_COLS)
            df_all = pd.concat([df_old, new_unique_rows], ignore_index=True)
            print(f"Vendor sheet: Found {len(df_new) - len(new_unique_rows)} duplicate(s). Added {len(new_unique_rows)} new record(s).")
            
            try:
                with pd.ExcelWriter(local_excel, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
                    df_all.to_excel(writer, sheet_name="Vendor", index=False)
                new_emails = new_unique_rows["Candidate Email"].astype(str).str.lower().str.strip().tolist()
                existing_emails.update(new_emails)
            except Exception as e:
                print(f"Warning: failed to write Vendor sheet: {e}. Rewriting full workbook.")
                create_base_file()
                with pd.ExcelWriter(local_excel, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
                    df_all.to_excel(writer, sheet_name="Vendor", index=False)
        else:
            print("Vendor sheet: No new records to add.")
            
    # Process new candidate rows
    if candidate_rows:
        df_new = pd.DataFrame(candidate_rows)
        df_new = df_new.reindex(columns=CANDIDATE_COLS, fill_value="")
        
        df_new['Candidate Email_norm'] = df_new['Candidate Email'].astype(str).str.lower().str.strip()
        new_unique_rows = df_new[~df_new['Candidate Email_norm'].isin(existing_emails)].drop(columns=['Candidate Email_norm'])
        
        if not new_unique_rows.empty:
            df_old = load_sheet_df("Candidate", CANDIDATE_COLS)
            df_all = pd.concat([df_old, new_unique_rows], ignore_index=True)
            print(f"Candidate sheet: Found {len(df_new) - len(new_unique_rows)} duplicate(s). Added {len(new_unique_rows)} new record(s).")

            try:
                with pd.ExcelWriter(local_excel, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
                    df_all.to_excel(writer, sheet_name="Candidate", index=False)
                new_emails = new_unique_rows["Candidate Email"].astype(str).str.lower().str.strip().tolist()
                existing_emails.update(new_emails)
            except Exception as e:
                print(f"Warning: failed to write Candidate sheet in append mode: {e}. Rewriting full workbook.")
                create_base_file()
                with pd.ExcelWriter(local_excel, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
                    df_all.to_excel(writer, sheet_name="Candidate", index=False)

    # Process new referral rows
    if referral_rows:
        df_new = pd.DataFrame(referral_rows)
        df_new = df_new.reindex(columns=REFERRAL_COLS, fill_value="")
        
        df_new['Candidate Email_norm'] = df_new['Candidate Email'].astype(str).str.lower().str.strip()
        new_unique_rows = df_new[~df_new['Candidate Email_norm'].isin(existing_emails)].drop(columns=['Candidate Email_norm'])
        
        if not new_unique_rows.empty:
            df_old = load_sheet_df("Referral", REFERRAL_COLS)
            df_all = pd.concat([df_old, new_unique_rows], ignore_index=True)
            print(f"Referral sheet: Found {len(df_new) - len(new_unique_rows)} duplicate(s). Added {len(new_unique_rows)} new record(s).")
            
            try:
                with pd.ExcelWriter(local_excel, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
                    df_all.to_excel(writer, sheet_name="Referral", index=False)
                new_emails = new_unique_rows["Candidate Email"].astype(str).str.lower().str.strip().tolist()
                existing_emails.update(new_emails)
            except Exception as e:
                print(f"Warning: failed to write Referral sheet in append mode: {e}. Rewriting full workbook.")
                create_base_file()
                with pd.ExcelWriter(local_excel, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
                    df_all.to_excel(writer, sheet_name="Referral", index=False)
    
    return local_excel

# -----------------------------
# Mail type detection & parsers
# -----------------------------
def detect_mail_type(subject, body, frm, recipients):
    s, b = (subject or "").lower(), (body or "").lower()
    if "referr" in s or "referr" in b or "referral" in s or "referred" in b:
        return "referral"
    vendor_signals = ["please find profiles", "please find attached profiles", "profiles for", "candidate name", "current organization", "phone number"]
    if any(k in b for k in vendor_signals) or any(k in s for k in ["profiles", "profile list", "candidates"]):
        emails_found = re.findall(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", b)
        phones_found = re.findall(r"\+?\d[\d\-\s\(\)]{6,}\d", b)
        if len(emails_found) > 1 or len(phones_found) > 1:
            return "vendor"
    if "application" in s or "resume" in s or "cv" in s or re.search(r"\bresume\b|\bcv\b|\bapplication\b", s, re.I):
        return "candidate"
    emails_found = re.findall(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", b)
    if len(emails_found) <= 1:
        return "candidate"
    return "vendor"

PHONE_RE = re.compile(r"(\+?\d[\d\-\s\(\)]{6,}\d)")

def extract_contact(body):
    m = PHONE_RE.search(body or "")
    return m.group(1).strip() if m else ""

def extract_name_from_subject_or_body(subject, body, frm):
    m = re.search(r"(?:application|resume|cv)\s*[-:]\s*(.+)", subject, re.I)
    if m:
        return m.group(1).strip()
    m2 = re.search(r"[-\|]\s*([^|-]{2,80})$", subject)
    if m2:
        return m2.group(1).strip()
    lines = [ln.strip() for ln in (body or "").splitlines() if ln.strip()]
    for i, ln in enumerate(lines[-6:]):
        low = ln.lower()
        if low.startswith(("regards", "thanks", "sincerely", "best")):
            if i+1 < len(lines):
                return lines[i+1].strip()
    if "<" in frm:
        return decode_mime_words(frm.split("<")[0]).strip().strip('"')
    return ""

def parse_candidate_email(msg, subject, body, frm, date_header):
    candidate_name = extract_name_from_subject_or_body(subject, body, frm)
    emails = re.findall(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", body)
    candidate_email = emails[0] if emails else (re.search(r"<([^>]+)>", frm).group(1) if re.search(r"<([^>]+)>", frm) else frm)
    phones = re.findall(r"\+?\d[\d\-\s\(\)]{6,}\d", body)
    linkedin = ""
    mlink = re.search(r"(https?://(www\.)?linkedin\.com[^\s]*)", body, re.I)
    if mlink:
        linkedin = mlink.group(1)
    return [{
        "Date": date_header,
        "Candidate Name": candidate_name or "",
        "Candidate Email": candidate_email or "",
        "Phone Number": phones[0] if phones else "",
        "Role Applied For": subject or "",
        "LinkedIn": linkedin,
        "Resume Link": "",
        "Mail URL": "",
        "Source File": ""
    }]

def parse_referral_email(msg, subject, body, frm, date_header):
    ref_name = ""
    ref_email = ""
    if "<" in frm:
        m = re.search(r"<([^>]+)>", frm)
        ref_email = m.group(1) if m else frm
        ref_name = decode_mime_words(frm.split("<")[0]).strip()
    else:
        ref_email = frm
    emails = re.findall(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", body)
    phones = re.findall(r"\+?\d[\d\-\s\(\)]{6,}\d", body)
    candidate_email = ""
    for e in emails:
        if e.lower() != ref_email.lower():
            candidate_email = e
            break
    candidate_name = ""
    m = re.search(r"[-–\|]\s*(.+)$", subject)
    if m:
        candidate_name = m.group(1).strip()
    return [{
        "Date": date_header,
        "Candidate Name": candidate_name or "",
        "Candidate Email": candidate_email or "",
        "Referred By": ref_name,
        "Referrer Email": ref_email,
        "Resume Link": "",
        "Mail URL": "",
        "Source File": ""
    }]

def parse_vendor_email(msg, subject, body, frm, date_header):
    lines = [ln for ln in (body or "").splitlines() if ln.strip()]
    candidates = []
    header_idx = -1
    for i, ln in enumerate(lines[:40]):
        if re.search(r"candidate name", ln, re.I) and (re.search(r"phone", ln, re.I) or re.search(r"email", ln, re.I)):
            header_idx = i
            break
    if header_idx >= 0:
        for row in lines[header_idx+1:]:
            if row.lower().startswith(("regards", "thanks", "kind regards", "best")):
                break
            cols = re.split(r"\t+|\s{2,}", row)
            emails = re.findall(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", row)
            phones = re.findall(r"\+?\d[\d\-\s\(\)]{6,}\d", row)
            name = cols[0].strip() if cols else ""
            cand = {
                "Date": date_header,
                "Vendor Name": decode_mime_words(frm.split("<")[0]),
                "Role Applied For": subject,
                "Candidate Name": name,
                "Candidate Email": emails[0] if emails else "",
                "Phone Number": phones[0] if phones else "",
                "Resume Link": "",
                "Mail URL": "",
                "Source File": ""
            }
            candidates.append(cand)
        if candidates:
            return candidates

    for i, ln in enumerate(lines):
        emails = re.findall(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", ln)
        phones = re.findall(r"\+?\d[\d\-\s\(\)]{6,}\d", ln)
        if emails or phones:
            name = ""
            if i >= 1 and len(lines[i-1].split()) <= 6:
                name = lines[i-1].strip()
            cand = {
                "Date": date_header,
                "Vendor Name": decode_mime_words(frm.split("<")[0]),
                "Role Applied For": subject,
                "Candidate Name": name,
                "Candidate Email": emails[0] if emails else "",
                "Phone Number": phones[0] if phones else "",
                "Resume Link": "",
                "Mail URL": "",
                "Source File": ""
            }
            candidates.append(cand)
    return candidates

def parse_vendor_excel_attachment(excel_path, subject, frm, date_header):
    """
    Parses a vendor-provided Excel file and extracts candidate details.
    Assumes a standard format for the new columns.
    """
    candidates = []
    vendor_name = decode_mime_words(frm.split("<")[0])
    
    try:
        df = pd.read_excel(excel_path)
        # Normalize column names to a consistent format for easier mapping
        df.columns = [c.strip().replace('\n', ' ').strip() for c in df.columns]

        # Map spreadsheet columns to your data structure
        for _, row in df.iterrows():
            cand = {
                "Date": date_header,
                "Vendor Name": vendor_name,
                "Role Applied For": subject,
                "Candidate Name": row.get("Name", ""),
                "Candidate Email": row.get("Candidate Email", ""),
                "Phone Number": row.get("Phone Number", ""),
                "Resume Link": "",
                "Mail URL": "",
                "Source File": os.path.basename(excel_path),
                "Sr. No.": row.get("Sr. No.", ""),
                "Current Company": row.get("Current Company", ""),
                "Tenure": row.get("Tenure", ""),
                "Previous Company": row.get("Previous Company", ""),
                "Highest Education": row.get("Highest Education", ""),
                "Second Highest Education": row.get("Second Highest Education", ""),
                "Designation": row.get("Designation", ""),
                "AUM": row.get("AUM", ""),
                "AUM Mix - Specify products": row.get("AUM Mix - Specify products", ""),
                "Size of Book": row.get("Size of Book", ""),
                "Current CTC": row.get("Current CTC", ""),
                "Notice Period": row.get("Notice Period", ""),
                "Role": row.get("Role", "")
            }
            # Only add if at least a name or email is present
            if cand.get("Candidate Name") or cand.get("Candidate Email"):
                candidates.append(cand)

    except Exception as e:
        print(f"❌ Error parsing Excel file {os.path.basename(excel_path)}: {e}")

    return candidates

# -----------------------------
# Local (.eml) processing
# -----------------------------
def process_local_and_upload_to_drive(force=False, days=1):
    service = get_drive_service()
    root_id = ensure_drive_root(service, DRIVE_ROOT_FOLDER)

    os.makedirs(SAMPLE_FOLDER, exist_ok=True)
    os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)

    processed = set() if force else load_processed_ids(PROCESSED_FILE)
    new_processed = set()

    vendor_rows, candidate_rows, referral_rows = [], [], []

    files = sorted([f for f in os.listdir(SAMPLE_FOLDER) if f.lower().endswith(".eml")])
    
    # Filter files by modification time
    now = datetime.now()
    limit = now - timedelta(days=days)
    new_files = [f for f in files if datetime.fromtimestamp(os.path.getmtime(os.path.join(SAMPLE_FOLDER, f))) > limit]
    
    print(f"Found {len(files)} .eml files in '{SAMPLE_FOLDER}', processing {len(new_files)} from last {days} days.")

    date_str = datetime.now().strftime("%Y-%m-%d")
    date_folder_id = ensure_date_folder(service, root_id, date_str)

    # Load all existing emails for global deduplication
    existing_emails = set()
    if os.path.exists(EXCEL_FILE):
        try:
            xls = pd.ExcelFile(EXCEL_FILE)
            for sheet_name in xls.sheet_names:
                df = pd.read_excel(xls, sheet_name=sheet_name)
                if "Candidate Email" in df.columns:
                    # Robustly handle data types, converting to string first
                    emails = df["Candidate Email"].astype(str).str.lower().str.strip().tolist()
                    existing_emails.update(emails)
        except Exception as e:
            print(f"Warning: Could not load existing emails from {EXCEL_FILE} for global dedupe: {e}")

    for fname in new_files:
        unique_base = f"local::{fname}"
        path = os.path.join(SAMPLE_FOLDER, fname)
        with open(path, "rb") as fp:
            try:
                msg = email.message_from_binary_file(fp)
            except Exception as e:
                print("❌ Failed reading:", fname, e)
                continue

        subj = decode_mime_words(msg.get("Subject", ""))
        frm = decode_mime_words(msg.get("From", ""))
        date_header = decode_mime_words(msg.get("Date", "")) or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        message_id = decode_mime_words(msg.get("Message-ID") or msg.get("Message-Id") or "")

        # Extract plain text body
        body = ""
        if msg.is_multipart():
            for part in msg.walk():
                if part.get_content_type() == "text/plain" and not part.get_filename():
                    try:
                        body += part.get_payload(decode=True).decode(part.get_content_charset() or "utf-8", errors="ignore")
                    except Exception:
                        try:
                            body += part.get_payload(decode=True).decode("utf-8", errors="ignore")
                        except Exception:
                            pass
        else:
            try:
                body = msg.get_payload(decode=True).decode(msg.get_content_charset() or "utf-8", errors="ignore")
            except Exception:
                try:
                    body = msg.get_payload(decode=True).decode("utf-8", errors="ignore")
                except Exception:
                    body = ""
        
        recipients = " ".join(msg.get_all("To", []) + msg.get_all("Cc", [])).lower()
        
        excel_processed = False
        parsed_list = []
        uploaded_links = []
        
        for part in msg.walk():
            fn = part.get_filename()
            if not fn:
                continue
            fn_dec = decode_mime_words(fn)
            ext = os.path.splitext(fn_dec)[1].lower()
            
            if ext in {".xlsx", ".xls"}:
                safe_fn = safe_filename(fn_dec)
                local_path = os.path.join(DOWNLOAD_FOLDER, safe_fn)
                if os.path.exists(local_path):
                    print(f"⏭ Skipping Excel download: {safe_fn} already exists.")
                    parsed_list.extend(parse_vendor_excel_attachment(local_path, subj, frm, date_header))
                    excel_processed = True
                else:
                    try:
                        with open(local_path, "wb") as wf:
                            wf.write(part.get_payload(decode=True))
                        print(f"📄 Found and downloaded Excel attachment: {safe_fn}")
                        parsed_list.extend(parse_vendor_excel_attachment(local_path, subj, frm, date_header))
                        excel_processed = True
                    except Exception as e:
                        print(f"❌ Failed saving/parsing Excel attachment: {safe_fn}, {e}")

            if ext in {".pdf", ".docx", ".doc"}:
                safe_fn = safe_filename(fn_dec)
                local_path = os.path.join(DOWNLOAD_FOLDER, safe_fn)
                if os.path.exists(local_path):
                    print(f"⏭ Skipping resume download: {safe_fn} already exists.")
                    found_file = find_file_in_folder(service, safe_fn, date_folder_id)
                    if found_file:
                        link = f"https://drive.google.com/file/d/{found_file['id']}/view"
                        uploaded_links.append(link)
                else:
                    try:
                        with open(local_path, "wb") as wf:
                            wf.write(part.get_payload(decode=True))
                        fid, link = upload_file_to_drive(service, local_path, safe_fn, date_folder_id)
                        uploaded_links.append(link)
                    except Exception as e:
                        print(f"❌ Failed uploading attachment to Drive: {safe_fn}, {e}")

        if not excel_processed:
            mtype = detect_mail_type(subj, body, frm, recipients)
            if mtype == "vendor":
                parsed_list = parse_vendor_email(msg, subj, body, frm, date_header)
            elif mtype == "referral":
                parsed_list = parse_referral_email(msg, subj, body, frm, date_header)
            else:
                parsed_list = parse_candidate_email(msg, subj, body, frm, date_header)

        for cand in parsed_list:
            candidate_email = str(cand.get("Candidate Email", "")).lower().strip()
            if candidate_email and candidate_email in existing_emails:
                print(f"Candidate with email {candidate_email} ⏭ Skipped (already exists in master file)")
                continue

            candidate_key = str(cand.get("Candidate Email") or cand.get("Candidate Name") or "").strip()
            unique_id = f"local::{fname}::{candidate_key}"
            if unique_id in processed:
                print(f"File: {fname} Candidate: {candidate_key} ⏭ Skipped (already processed)")
                continue
            
            cand["Resume Link"] = ", ".join(uploaded_links) if uploaded_links else cand.get("Resume Link", "")
            cand["Mail URL"] = build_mail_url_from_message_id(message_id)
            cand["Source File"] = fname
            
            if excel_processed or detect_mail_type(subj, body, frm, recipients) == "vendor":
                vendor_rows.append(cand)
            elif detect_mail_type(subj, body, frm, recipients) == "referral":
                referral_rows.append(cand)
            else:
                candidate_rows.append(cand)
            new_processed.add(unique_id)
            print(f"File: {fname} ✅ Accepted | Candidate: {candidate_key or '[unknown]'} | Attachments: {len(uploaded_links)}")

    if vendor_rows or candidate_rows or referral_rows:
        local_excel = append_to_excel_by_sheet(EXCEL_FILE, vendor_rows, candidate_rows, referral_rows)
        try:
            found = find_file_in_folder(service, os.path.basename(EXCEL_FILE), root_id)
            if found:
                try:
                    update_drive_file(service, found["id"], local_excel)
                    print(f"✅ Updated Excel on Drive (file id: {found['id']})")
                except Exception as e:
                    print(f"⚠️ Failed to update existing Drive file: {e}. Attempting to upload as new file.")
                    fid = upload_file_to_drive(service, local_excel, os.path.basename(local_excel), root_id)[0]
                    print(f"✅ Uploaded Excel to Drive as new file (file id: {fid})")
            else:
                fid = upload_file_to_drive(service, local_excel, os.path.basename(local_excel), root_id)[0]
                print(f"✅ Uploaded Excel to Drive (file id: {fid})")
        except Exception as e:
            print(f"❌ Drive operation failed for Excel: {e}")

    if new_processed:
        processed_update = load_processed_ids(PROCESSED_FILE).union(new_processed)
        save_processed_ids(PROCESSED_FILE, processed_update)
        print(f"📌 Updated processed log: {PROCESSED_FILE}")
    else:
        print("No new valid CVs found.")

# -----------------------------
# IMAP processing (live Gmail)
# -----------------------------
# IMAP processing (live Gmail)
# -----------------------------
def process_imap_and_upload_to_drive(force=False, days=1):
    if not (IMAP_SERVER and EMAIL_ACCOUNT and EMAIL_PASSWORD):
        print("IMAP config missing. Set IMAP_SERVER, EMAIL and PASSWORD in .env.")
        return

    service = get_drive_service()
    root_id = ensure_drive_root(service, DRIVE_ROOT_FOLDER)
    os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)

    processed = set() if force else load_processed_ids(PROCESSED_FILE)
    new_processed = set()
    vendor_rows, candidate_rows, referral_rows = [], [], []

    try:
        conn = imaplib.IMAP4_SSL(IMAP_SERVER)
        conn.login(EMAIL_ACCOUNT, EMAIL_PASSWORD)
        conn.select(MAIL_FOLDER)
        
        since_date = (datetime.now() - timedelta(days=days)).strftime("%d-%b-%Y")
        # Search for messages with attachments to reduce the number of messages to process
        typ, data = conn.search(None, f'(SINCE "{since_date}")')
        ids = data[0].split() if data and data[0] else []
        print(f"Found {len(ids)} messages in IMAP folder '{MAIL_FOLDER}' from last {days} days.")
        date_str = datetime.now().strftime("%Y-%m-%d")
        date_folder_id = ensure_date_folder(service, root_id, date_str)
        
        existing_emails = set()
        if os.path.exists(EXCEL_FILE):
            try:
                xls = pd.ExcelFile(EXCEL_FILE)
                for sheet_name in xls.sheet_names:
                    df = pd.read_excel(xls, sheet_name=sheet_name)
                    if "Candidate Email" in df.columns:
                        emails = df["Candidate Email"].astype(str).str.lower().str.strip().tolist()
                        existing_emails.update(emails)
            except Exception as e:
                print(f"Warning: Could not load existing emails from {EXCEL_FILE} for global dedupe: {e}")

        for eid in ids:
            typ, msg_data = conn.fetch(eid, "(RFC822)")
            if typ != "OK" or not msg_data:
                continue
            raw = msg_data[0][1]
            msg = email.message_from_bytes(raw)
            
            # Use Message-ID for unique identification
            message_id = decode_mime_words(msg.get("Message-ID") or msg.get("Message-Id") or "")

            # Check for duplicates based on Message-ID
            if f"imap::{message_id}" in processed:
                print(f"Message ID: {message_id} ⏭ Skipped (already processed)")
                continue

            subj = decode_mime_words(msg.get("Subject", ""))
            
            # Skip messages that are likely replies or forwards
            if subj.lower().startswith("re:") or subj.lower().startswith("fwd:"):
                print(f"Message ID: {message_id} >> Skipped (reply/forward detected in subject)")
                continue

            frm = decode_mime_words(msg.get("From", ""))
            date_header = decode_mime_words(msg.get("Date", "")) or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            excel_found_in_email = False
            parsed_list = []
            uploaded_links = []
            
            has_relevant_attachment = False

            # First, iterate through parts to find relevant attachments
            for part in msg.walk():
                # Check for Content-Disposition to confirm it's a real attachment
                if part.get_content_disposition() == 'attachment':
                    fn = part.get_filename()
                    if not fn:
                        continue
                    fn_dec = decode_mime_words(fn)
                    ext = os.path.splitext(fn_dec)[1].lower()
                    
                    if ext in {".xlsx", ".xls"}:
                        has_relevant_attachment = True
                        excel_found_in_email = True
                        safe_fn = safe_filename(fn_dec)
                        local_path = os.path.join(DOWNLOAD_FOLDER, safe_fn)
                        if os.path.exists(local_path):
                            print(f"⏭ Skipping Excel download: {safe_fn} already exists.")
                        else:
                            try:
                                with open(local_path, "wb") as wf:
                                    wf.write(part.get_payload(decode=True))
                                print(f"📄 Found and downloaded Excel attachment: {safe_fn}")
                            except Exception as e:
                                print(f"❌ Failed saving/parsing Excel attachment: {safe_fn}, {e}")
                        
                        parsed_list.extend(parse_vendor_excel_attachment(local_path, subj, frm, date_header))
                        break # Exit loop after processing the main excel attachment
                    
                    if ext in {".pdf", ".docx", ".doc"}:
                        has_relevant_attachment = True
                        safe_fn = safe_filename(fn_dec)
                        local_path = os.path.join(DOWNLOAD_FOLDER, safe_fn)
                        if os.path.exists(local_path):
                            print(f"⏭ Skipping resume download: {safe_fn} already exists.")
                            found_file = find_file_in_folder(service, safe_fn, date_folder_id)
                            if found_file:
                                link = f"https://drive.google.com/file/d/{found_file['id']}/view"
                                uploaded_links.append(link)
                        else:
                            try:
                                with open(local_path, "wb") as wf:
                                    wf.write(part.get_payload(decode=True))
                                fid, link = upload_file_to_drive(service, local_path, safe_fn, date_folder_id)
                                uploaded_links.append(link)
                            except Exception as e:
                                print(f"❌ Failed uploading attachment to Drive: {safe_fn}, {e}")
            
            if not has_relevant_attachment:
                print(f"Message ID: {message_id} >> Skipped (no relevant attachments found)")
                continue

            # If no Excel attachment was found, parse the body of the email
            if not excel_found_in_email:
                body = ""
                if msg.is_multipart():
                    for part in msg.walk():
                        if part.get_content_type() == "text/plain" and not part.get_filename():
                            try:
                                body += part.get_payload(decode=True).decode(part.get_content_charset() or "utf-8", errors="ignore")
                            except Exception:
                                pass
                else:
                    try:
                        body = msg.get_payload(decode=True).decode(msg.get_content_charset() or "utf-8", errors="ignore")
                    except Exception:
                        body = ""

                recipients = " ".join(msg.get_all("To", []) + msg.get_all("Cc", [])).lower()
                mtype = detect_mail_type(subj, body, frm, recipients)
                if mtype == "vendor":
                    parsed_list = parse_vendor_email(msg, subj, body, frm, date_header)
                elif mtype == "referral":
                    parsed_list = parse_referral_email(msg, subj, body, frm, date_header)
                else:
                    parsed_list = parse_candidate_email(msg, subj, body, frm, date_header)

            for cand in parsed_list:
                candidate_email = str(cand.get("Candidate Email", "")).lower().strip()
                if candidate_email and candidate_email in existing_emails:
                    print(f"Candidate with email {candidate_email} ⏭ Skipped (already exists in master file)")
                    continue
                
                candidate_key = str(cand.get("Candidate Email") or cand.get("Candidate Name") or "").strip()
                unique_id = f"imap::{message_id}::{candidate_key}"
                
                # Check for a second level of deduplication
                if unique_id in processed:
                    print(f"Message {message_id} Candidate: {candidate_key} ⏭ Skipped (already processed)")
                    continue
                
                cand["Resume Link"] = ", ".join(uploaded_links) if uploaded_links else cand.get("Resume Link", "")
                cand["Mail URL"] = build_mail_url_from_message_id(message_id)
                cand["Source File"] = message_id or ""
                
                if excel_found_in_email or detect_mail_type(subj, body, frm, recipients) == "vendor":
                    vendor_rows.append(cand)
                elif detect_mail_type(subj, body, frm, recipients) == "referral":
                    referral_rows.append(cand)
                else:
                    candidate_rows.append(cand)
                
                new_processed.add(unique_id)
                print(f"Message {message_id} ✅ Accepted | Candidate: {candidate_key or '[unknown]'} | Attachments: {len(uploaded_links)}")

        conn.logout()
    except Exception as e:
        print("❌ IMAP error:", e)
        return

    if vendor_rows or candidate_rows or referral_rows:
        local_excel = append_to_excel_by_sheet(EXCEL_FILE, vendor_rows, candidate_rows, referral_rows)
        try:
            found = find_file_in_folder(service, os.path.basename(EXCEL_FILE), root_id)
            if found:
                try:
                    update_drive_file(service, found["id"], local_excel)
                    print(f"✅ Updated Excel on Drive (file id: {found['id']})")
                except Exception as e:
                    print(f"⚠️ Failed to update existing Drive file: {e}. Uploading as new file.")
                    fid = upload_file_to_drive(service, local_excel, os.path.basename(local_excel), root_id)[0]
                    print(f"✅ Uploaded Excel to Drive as new file (file id: {fid})")
            else:
                fid = upload_file_to_drive(service, local_excel, os.path.basename(local_excel), root_id)[0]
                print(f"✅ Uploaded Excel to Drive (file id: {fid})")
        except Exception as e:
            print(f"❌ Drive operation failed for Excel: {e}")

    if new_processed:
        processed_update = load_processed_ids(PROCESSED_FILE).union(new_processed)
        save_processed_ids(PROCESSED_FILE, processed_update)
        print(f"📌 Updated processed log: {PROCESSED_FILE}")
    else:
        print("No new valid CVs found.")

# -----------------------------
# CLI Entrypoint
# -----------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true", help="ignore processed log (reprocess all)")
    parser.add_argument("--days", type=int, default=1, help="Number of days to process (e.g., --days 7 for last week). Default is 1.")
    args = parser.parse_args()

    if USE_IMAP:
        print("Running in IMAP (live Gmail) mode.")
        process_imap_and_upload_to_drive(force=args.force, days=args.days)
    else:
        print("Running in LOCAL (.eml) mode.")
        process_local_and_upload_to_drive(force=args.force, days=args.days)