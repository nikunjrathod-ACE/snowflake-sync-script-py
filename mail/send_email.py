import json
import os
import logging
import base64
from urllib import request, error

logger = logging.getLogger(__name__)


def _load_mail_config(config_path: str) -> dict:
    cfg = {}
    try:
        with open(config_path, "r") as f:
            cfg = json.load(f)
    except FileNotFoundError:
        logger.warning(f"Mail credentials not found at {config_path}. Will try environment variables.")

    # Overlay environment variables if present (Apps Script URL)
    if os.getenv("APPS_SCRIPT_URL"):
        cfg["apps_script_url"] = os.getenv("APPS_SCRIPT_URL")
    # Optional API key for Apps Script
    if os.getenv("APPS_SCRIPT_API_KEY"):
        cfg["apps_script_api_key"] = os.getenv("APPS_SCRIPT_API_KEY")

    return cfg


def _is_configured(cfg: dict) -> bool:
    url = cfg.get("apps_script_url")
    return bool(url and url != "https://script.google.com/macros/s/REPLACE_WITH_DEPLOYED_EXEC_URL/exec")


def send_email(recipients: list[str], subject: str, body: str,
               config_path: str = "config/mail_credentials.json",
               attachments: list[str] | None = None) -> bool:
    """
    Send an email by POSTing JSON to a Google Apps Script Web App.

    Returns True on success, False otherwise.
    """
    cfg = _load_mail_config(config_path)
    if not _is_configured(cfg):
        logger.warning("Apps Script URL not configured. Skipping email send.")
        return False

    payload = {
        "to": ", ".join(recipients),
        "subject": subject,
        "body": body,
        "attachments": []
    }
    logger.info(f"Preparing to send email to {recipients} with subject '{subject}', body '{body}', and attachments {attachments}")

    api_key = cfg.get("apps_script_api_key")
    if api_key:
        payload["apiKey"] = api_key

    # Attach files if provided (encode as base64 for Apps Script)
    if attachments:
        for path in attachments:
            try:
                if not path or not os.path.exists(path):
                    logger.warning(f"Attachment not found: {path}")
                    continue
                with open(path, "rb") as f:
                    encoded = base64.b64encode(f.read()).decode("utf-8")
                payload["attachments"].append({
                    "fileName": os.path.basename(path),
                    "mimeType": "text/plain" if path.endswith(".log") else "application/octet-stream",
                    "content": encoded
                })
            except Exception as e:
                logger.error(f"Failed attaching file '{path}': {e}")

    try:
        headers = {"Content-Type": "application/json"}
        if api_key:
            headers["X-API-KEY"] = api_key
        req = request.Request(
            cfg["apps_script_url"],
            data=json.dumps(payload).encode("utf-8"),
            headers=headers,
            method="POST"
        )
        with request.urlopen(req, timeout=30) as resp:
            status = resp.getcode()
            resp_body = resp.read().decode("utf-8", errors="ignore")
            if 200 <= status < 300:
                logger.info(f"Sent alert email via Apps Script to {recipients}")
                return True
            else:
                logger.error(f"Apps Script responded with status {status}: {resp_body[:500]}")
                return False
    except error.HTTPError as e:
        body = e.read().decode("utf-8", errors="ignore")
        logger.error(f"Apps Script HTTP {e.code}: {body[:500]}")
        return False
    except error.URLError as e:
        logger.error(f"Failed to send alert email via Apps Script: {e}")
        return False