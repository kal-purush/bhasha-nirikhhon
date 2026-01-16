import yt_dlp # Import the yt-dlp library
import os
import re

# Provide the playlist URL

playlist_url = ''

# Set the download options
ydl_opts = {
    'outtmpl': '%(title)s.%(ext)s',  # Save using the video title as filename
    'format': 'bestvideo+bestaudio/best',  # Download best quality video and audio combined, fallback to best
}


def sanitize_filename(filename):
    """Sanitize the filename by removing invalid characters."""
    return re.sub(r'[<>:"/\\|?*]', '', filename)

def check_if_downloaded(sanitized_title):
    """Check if the video file already exists."""
    return os.path.exists(f"{sanitized_title}.mp4")

# Download the entire playlist
with yt_dlp.YoutubeDL(ydl_opts) as ydl:
    # Get playlist information without downloading
    playlist_info = ydl.extract_info(playlist_url, download=False)

    for video in playlist_info['entries']:
        if video is None:
            continue

        title = video['title']
        sanitized_title = sanitize_filename(title)
        print(f"Checking: {title}")

        if not check_if_downloaded(sanitized_title):
            print(f"Downloading: {title}")
            try:
                # Download the video using 'webpage_url'
                ydl.download([video['webpage_url']])
                
                # Check the output file name and display it
                output_file = f"{sanitized_title}.mp4"
                if os.path.exists(output_file):
                    print(f"Downloaded: {output_file}")
                else:
                    print(f"Failed to download {sanitized_title}.mp4")
            except yt_dlp.utils.DownloadError as e:
                print(f"Error downloading {title}: {e}")
        else:
            print(f"Already downloaded: {sanitized_title}.mp4")

print("Playlist download completed!")