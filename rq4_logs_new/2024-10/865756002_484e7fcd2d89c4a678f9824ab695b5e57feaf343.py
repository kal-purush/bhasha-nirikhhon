import requests
import tkinter as tk
from tkinter import messagebox

# GitHub API endpoint for the latest release
GITHUB_API_URL = "https://api.github.com/repos/lakshanjs/srp-service-manager/releases/latest"

# Current version of your application
CURRENT_VERSION = "1.1"  # Note: No 'v' here

def get_latest_release():
    """Fetches the latest release information from GitHub."""
    try:
        response = requests.get(GITHUB_API_URL)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error fetching latest release: {response.status_code}")
            return None
    except Exception as e:
        print(f"Error fetching latest release: {e}")
        return None

def is_new_version(latest_version):
    """Compares current version with the latest version."""
    # Strip the 'v' from the latest version (if present) to compare properly
    latest_version = latest_version.lstrip('v')
    return latest_version > CURRENT_VERSION

def check_for_updates():
    """Checks GitHub for the latest release and alerts if there's an update."""
    release_info = get_latest_release()
    if release_info:
        latest_version = release_info['tag_name']  # GitHub uses 'tag_name' for versioning
        release_notes = release_info['body']  # Release notes
        download_url = release_info['html_url']  # Link to the release page

        if is_new_version(latest_version):
            # Display an update alert to the user
            show_update_alert(latest_version, release_notes, download_url)
        else:
            pass
    else:
        show_message("Unable to check for updates.")

def show_update_alert(latest_version, release_notes, download_url):
    """Displays an update alert to the user."""
    message = f"A new version {latest_version} is available!\n\nRelease notes:\n{release_notes}\n\nDownload the update: {download_url}"
    messagebox.showinfo("Update Available", message)

def show_message(message):
    """Displays a message to the user."""
    messagebox.showinfo("Information", message)