import subprocess

input_video = r"D:\5. Personal Website\Yinkai-Dong.github.io\Projects\L-PASA-W Hand\SMRDC_2023_Yinkai_Dong.mp4"
output_video = r"D:\5. Personal Website\Yinkai-Dong.github.io\Projects\L-PASA-W Hand\SMRDC_2023_Yinkai_Dong-compressed.mp4"
target_size_mb = 100  # Target size in MB

# Convert target size to bits (20 MB = 20 * 1024 * 1024 bytes = bits * 8)
target_size_bits = target_size_mb * 1024 * 1024 * 8

# Get video duration using ffprobe
probe_command = ["ffprobe", "-v", "error", "-select_streams", "v:0", "-show_entries", "format=duration", "-of", "default=noprint_wrappers=1:nokey=1", input_video]
duration = float(subprocess.check_output(probe_command).strip())

# Calculate target bitrate (bits per second)
target_bitrate = target_size_bits / duration

# Set the video bitrate using ffmpeg
ffmpeg_command = [
    "ffmpeg", 
    "-i", input_video, 
    "-b:v", str(int(target_bitrate)),  # Set target bitrate
    "-bufsize", str(int(target_bitrate)),  # Set buffer size to target bitrate for more control
    "-c:v", "libx264",  # Use H.264 codec
    "-preset", "medium",  # Compression preset
    "-y", output_video
]

# Run the ffmpeg command
subprocess.run(ffmpeg_command)

print(f"Compression complete. The compressed video is saved at: {output_video}")