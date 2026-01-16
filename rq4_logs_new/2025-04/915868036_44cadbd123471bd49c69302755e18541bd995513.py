from SOD_Stream import StreamManager
import cv2
import numpy as np
import time
import queue
import sys
import os
import argparse
import subprocess
import requests
import socket
import tempfile
import threading
import shutil

def check_rtmp_server(url):
    """Check if the RTMP server is reachable"""
    # Extract host from URL
    if url.startswith("rtmp://"):
        host = url.split("//")[1].split("/")[0]
        print(f"Checking connection to RTMP server: {host}")
        
        try:
            # Try to resolve the hostname
            ip = socket.gethostbyname(host)
            print(f"RTMP server resolved to IP: {ip}")
            
            # Try to connect to the RTMP port (1935)
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(5)
            result = s.connect_ex((host, 1935))
            s.close()
            
            if result == 0:
                print("RTMP port 1935 is open and reachable")
                return True
            else:
                print(f"RTMP port check failed with error code: {result}")
                return False
        except socket.gaierror:
            print(f"Could not resolve RTMP server hostname: {host}")
            return False
        except Exception as e:
            print(f"Error checking RTMP server: {str(e)}")
            return False
    return False

def test_ffmpeg_rtmp(rtmp_url="rtmp://a.rtmp.youtube.com/live2", stream_key="test-connection-only"):
    """Test if FFmpeg can connect to YouTube RTMP server"""
    print(f"Testing FFmpeg RTMP connection to {rtmp_url}...")
    
    # Create a simple test pattern
    command = [
        "ffmpeg",
        "-f", "lavfi",
        "-i", "testsrc=size=640x480:rate=30",
        "-t", "3",  # Just test for 3 seconds
        "-f", "flv",
        "-c:v", "libx264",
        "-preset", "ultrafast",
        "-b:v", "1000k",
        f"{rtmp_url}/{stream_key}"
    ]
    
    try:
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        
        # Wait for the process to finish
        stdout, stderr = process.communicate(timeout=10)
        stderr_text = stderr.decode('utf-8', errors='replace')
        
        # Check for specific error messages
        if "Connection refused" in stderr_text:
            print("RTMP connection refused by YouTube server")
            return False
        elif "Handshake failed" in stderr_text:
            print("RTMP handshake failed with YouTube server")
            return False
        elif "Error" in stderr_text and "I/O error" in stderr_text:
            print("I/O error connecting to YouTube RTMP server")
            print("This is often due to an invalid stream key or network restrictions")
            return False
        elif "Error" in stderr_text and rtmp_url in stderr_text:
            print("Error connecting to YouTube RTMP server")
            print(stderr_text)
            return False
        else:
            print("FFmpeg was able to connect to YouTube RTMP server")
            return True
    except subprocess.TimeoutExpired:
        process.kill()
        print("FFmpeg test timed out - this might indicate network issues")
        return False
    except Exception as e:
        print(f"Error testing FFmpeg: {str(e)}")
        return False

def verify_ffmpeg_installation():
    """Verify FFmpeg installation and capabilities"""
    print("Verifying FFmpeg installation...")
    
    try:
        # Check FFmpeg version
        process = subprocess.Popen(
            ["ffmpeg", "-version"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        stdout, stderr = process.communicate()
        version_info = stdout.decode('utf-8', errors='replace')
        
        if "ffmpeg version" in version_info:
            first_line = version_info.split('\n')[0]
            print(f"FFmpeg installed: {first_line}")
        else:
            print("FFmpeg not found or not properly installed")
            return False
        
        # Check for h264 encoding support
        process = subprocess.Popen(
            ["ffmpeg", "-encoders"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        stdout, stderr = process.communicate()
        encoders_info = stdout.decode('utf-8', errors='replace')
        
        if "libx264" in encoders_info:
            print("FFmpeg supports libx264 (software H.264 encoding)")
        else:
            print("Warning: libx264 encoder not found")
        
        if "h264_nvenc" in encoders_info:
            print("FFmpeg supports h264_nvenc (NVIDIA hardware encoding)")
        else:
            print("Warning: h264_nvenc encoder not found - NVIDIA hardware encoding not available")
            
        return True
    except Exception as e:
        print(f"Error verifying FFmpeg: {str(e)}")
        return False

def analyze_webcam_output(cap):
    """Analyze webcam output format"""
    if not cap.isOpened():
        print("Cannot analyze webcam - not opened")
        return
        
    # Get webcam properties
    width = cap.get(cv2.CAP_PROP_FRAME_WIDTH)
    height = cap.get(cv2.CAP_PROP_FRAME_HEIGHT)
    fps = cap.get(cv2.CAP_PROP_FPS)
    fourcc = cap.get(cv2.CAP_PROP_FOURCC)
    
    # Decode fourcc to readable format
    fourcc_str = "".join([chr((int(fourcc) >> 8 * i) & 0xFF) for i in range(4)])
    
    print(f"Webcam format: {width}x{height} @ {fps} FPS")
    print(f"Pixel format: {fourcc_str}")
    
    # Capture a test frame to analyze
    ret, frame = cap.read()
    if ret:
        print(f"Frame shape: {frame.shape}")
        print(f"Frame data type: {frame.dtype}")
        print(f"Frame min/max values: {frame.min()}/{frame.max()}")
    else:
        print("Failed to capture test frame")

def stream_test_pattern(stream_key, rtmp_url, duration=30, software_encoding=False):
    """Stream a test pattern directly using FFmpeg"""
    print(f"Streaming test pattern to YouTube for {duration} seconds")
    print(f"RTMP URL: {rtmp_url}")
    print(f"Stream key: {stream_key[:4]}...{stream_key[-4:]}")
    
    # Build the FFmpeg command
    if software_encoding:
        print("Using software encoding (libx264)")
        command = [
            "ffmpeg",
            "-f", "lavfi",
            "-i", "testsrc=size=1280x720:rate=30",
            "-f", "lavfi",
            "-i", "sine=frequency=440:sample_rate=44100",
            "-t", str(duration),
            "-c:v", "libx264",
            "-preset", "veryfast",
            "-pix_fmt", "yuv420p",
            "-g", "60",
            "-b:v", "2500k",
            "-c:a", "aac",
            "-b:a", "128k",
            "-ar", "44100",
            "-f", "flv",
            f"{rtmp_url}/{stream_key}"
        ]
    else:
        print("Using hardware encoding (h264_nvenc)")
        command = [
            "ffmpeg",
            "-f", "lavfi",
            "-i", "testsrc=size=1280x720:rate=30",
            "-f", "lavfi",
            "-i", "sine=frequency=440:sample_rate=44100",
            "-t", str(duration),
            "-c:v", "h264_nvenc",
            "-preset", "p1",
            "-pix_fmt", "yuv420p",
            "-g", "60",
            "-b:v", "2500k",
            "-c:a", "aac",
            "-b:a", "128k",
            "-ar", "44100",
            "-f", "flv",
            f"{rtmp_url}/{stream_key}"
        ]
    
    print(f"Running FFmpeg command: {' '.join(command)}")
    
    try:
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        
        start_time = time.time()
        print(f"Stream started at {time.strftime('%H:%M:%S')}")
        print(f"Stream will run for {duration} seconds")
        print("Check your YouTube Studio to see if the stream appears")
        
        # Wait for the specified duration
        while time.time() - start_time < duration:
            remaining = int(duration - (time.time() - start_time))
            print(f"\rStreaming... {remaining} seconds remaining", end="")
            time.sleep(1)
            
            # Check if process is still running
            if process.poll() is not None:
                print("\nFFmpeg process terminated unexpectedly")
                break
                
        print("\nStream completed")
        
        # Get the output
        stdout, stderr = process.communicate()
        
        # Print the FFmpeg output
        if stderr:
            stderr_text = stderr.decode('utf-8', errors='replace')
            print("\nFFmpeg output:")
            for line in stderr_text.split('\n')[:20]:  # Print first 20 lines
                if line.strip():
                    print(f"  {line.strip()}")
        
        return True
    except KeyboardInterrupt:
        print("\nStream interrupted by user")
        if process.poll() is None:
            process.terminate()
            process.wait(timeout=5)
        return False
    except Exception as e:
        print(f"Error streaming test pattern: {str(e)}")
        return False

def get_webcam_device_name():
    """Get the name of the first available webcam device"""
    devices = list_webcam_devices()
    if devices:
        return devices[0]
    return "Webcam"  # Default fallback name

def stream_webcam_direct(stream_key, rtmp_url, duration=30, software_encoding=False, width=1280, height=720):
    """Stream webcam directly using FFmpeg"""
    print(f"Streaming webcam directly to YouTube for {duration} seconds")
    print(f"RTMP URL: {rtmp_url}")
    print(f"Stream key: {stream_key[:4]}...{stream_key[-4:]}")
    print(f"Resolution: {width}x{height}")
    
    # Get webcam device name for FFmpeg
    webcam_name = get_webcam_device_name()
    print(f"Using webcam device: {webcam_name}")
    
    # Build the FFmpeg command for Windows
    if os.name == 'nt':  # Windows
        if software_encoding:
            print("Using software encoding (libx264)")
            command = [
                "ffmpeg",
                "-f", "dshow",
                "-video_size", f"{width}x{height}",
                "-framerate", "30",
                "-i", f"video=\"{webcam_name}\"",
                # Add audio tone
                "-f", "lavfi",
                "-i", "anullsrc=r=44100:cl=mono",
                "-t", str(duration),
                "-c:v", "libx264",
                "-preset", "veryfast",
                "-pix_fmt", "yuv420p",
                "-g", "60",
                "-b:v", "2500k",
                "-c:a", "aac",
                "-b:a", "128k",
                "-ar", "44100",
                "-f", "flv",
                f"{rtmp_url}/{stream_key}"
            ]
        else:
            print("Using hardware encoding (h264_nvenc)")
            command = [
                "ffmpeg",
                "-f", "dshow",
                "-video_size", f"{width}x{height}",
                "-framerate", "30",
                "-i", f"video=\"{webcam_name}\"",
                # Add audio tone
                "-f", "lavfi",
                "-i", "anullsrc=r=44100:cl=mono",
                "-t", str(duration),
                "-c:v", "h264_nvenc",
                "-preset", "p1",
                "-pix_fmt", "yuv420p",
                "-g", "60",
                "-b:v", "2500k",
                "-c:a", "aac",
                "-b:a", "128k",
                "-ar", "44100",
                "-f", "flv",
                f"{rtmp_url}/{stream_key}"
            ]
    else:  # Linux/Mac
        if software_encoding:
            print("Using software encoding (libx264)")
            command = [
                "ffmpeg",
                "-f", "v4l2",
                "-video_size", f"{width}x{height}",
                "-framerate", "30",
                "-i", "/dev/video0",
                # Add audio tone
                "-f", "lavfi",
                "-i", "anullsrc=r=44100:cl=mono",
                "-t", str(duration),
                "-c:v", "libx264",
                "-preset", "veryfast",
                "-pix_fmt", "yuv420p",
                "-g", "60",
                "-b:v", "2500k",
                "-c:a", "aac",
                "-b:a", "128k",
                "-ar", "44100",
                "-f", "flv",
                f"{rtmp_url}/{stream_key}"
            ]
        else:
            print("Using hardware encoding (h264_nvenc)")
            command = [
                "ffmpeg",
                "-f", "v4l2",
                "-video_size", f"{width}x{height}",
                "-framerate", "30",
                "-i", "/dev/video0",
                # Add audio tone
                "-f", "lavfi",
                "-i", "anullsrc=r=44100:cl=mono",
                "-t", str(duration),
                "-c:v", "h264_nvenc",
                "-preset", "p1",
                "-pix_fmt", "yuv420p",
                "-g", "60",
                "-b:v", "2500k",
                "-c:a", "aac",
                "-b:a", "128k",
                "-ar", "44100",
                "-f", "flv",
                f"{rtmp_url}/{stream_key}"
            ]
    
    # For Windows, we need to handle the command differently
    if os.name == 'nt':
        # Create a proper command string for Windows
        cmd_str = "ffmpeg "
        for i, arg in enumerate(command[1:]):  # Skip the first element (ffmpeg)
            if i == 7:  # This is the input argument after -i for video
                cmd_str += f"{arg} "  # Keep the quotes in the string
            else:
                cmd_str += f"{arg} "
        print(f"Running FFmpeg command: {cmd_str}")
        
        # Start FFmpeg process with real-time output
        process = subprocess.Popen(
            cmd_str,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=True,  # Use shell on Windows
            universal_newlines=True,
            bufsize=1
        )
    else:
        print(f"Running FFmpeg command: {' '.join(command)}")
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            bufsize=1
        )
    
    # Create a thread to read and display FFmpeg output in real-time
    def ffmpeg_output_reader():
        for line in iter(process.stderr.readline, ''):
            print(f"FFmpeg: {line.strip()}")
    
    output_thread = threading.Thread(target=ffmpeg_output_reader)
    output_thread.daemon = True
    output_thread.start()
    
    print(f"Stream started at {time.strftime('%H:%M:%S')}")
    print(f"Stream will run for {duration} seconds")
    print("Check your YouTube Studio to see if the stream appears")
    
    try:
        # Wait for the process to complete or timeout
        process.wait(timeout=duration + 5)  # Wait for duration plus a small buffer
    except subprocess.TimeoutExpired:
        print("FFmpeg process timed out, terminating...")
        process.terminate()
        
    print("\nStream completed")
    
    # Make sure to terminate the FFmpeg process
    if process and process.poll() is None:
        print("Terminating FFmpeg process...")
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            print("FFmpeg process did not terminate, killing it...")
            process.kill()
    
    return True

def stream_webcam_with_preview(stream_key, rtmp_url, duration=30, software_encoding=False, width=1280, height=720):
    """Stream webcam directly using FFmpeg with a preview window"""
    print(f"Streaming webcam directly to YouTube for {duration} seconds")
    print(f"RTMP URL: {rtmp_url}")
    print(f"Stream key: {stream_key[:4]}...{stream_key[-4:]}")
    print(f"Resolution: {width}x{height}")
    print("Opening preview window...")
    
    # Initialize webcam with OpenCV for preview
    cap = cv2.VideoCapture(0)
    if not cap.isOpened():
        print("Error: Could not open webcam for preview")
        return False
    
    # Set webcam properties
    cap.set(cv2.CAP_PROP_FRAME_WIDTH, width)
    cap.set(cv2.CAP_PROP_FRAME_HEIGHT, height)
    
    # Get actual frame dimensions (may differ from requested)
    actual_width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    actual_height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    print(f"Actual webcam resolution: {actual_width}x{actual_height}")
    
    # Create a preview window
    cv2.namedWindow('YouTube Stream Preview', cv2.WINDOW_NORMAL)
    
    # Read a few frames to make sure webcam is working
    for _ in range(5):
        ret, frame = cap.read()
        if not ret:
            print("Error: Failed to capture initial frames from webcam")
            cap.release()
            return False
        time.sleep(0.1)
    
    # Build the FFmpeg command to read from stdin
    if software_encoding:
        print("Using software encoding (libx264)")
        command = [
            "ffmpeg",
            "-f", "rawvideo",
            "-pix_fmt", "bgr24",
            "-s", f"{actual_width}x{actual_height}",
            "-r", "30",
            "-i", "pipe:0",  # Read from stdin
            "-f", "lavfi",
            "-i", "anullsrc=r=44100:cl=mono",
            "-t", str(duration),
            "-c:v", "libx264",
            "-preset", "veryfast",
            "-pix_fmt", "yuv420p",
            "-g", "60",
            "-b:v", "2500k",
            "-c:a", "aac",
            "-b:a", "128k",
            "-ar", "44100",
            "-f", "flv",
            f"{rtmp_url}/{stream_key}"
        ]
    else:
        print("Using hardware encoding (h264_nvenc)")
        command = [
            "ffmpeg",
            "-f", "rawvideo",
            "-pix_fmt", "bgr24",
            "-s", f"{actual_width}x{actual_height}",
            "-r", "30",
            "-i", "pipe:0",  # Read from stdin
            "-f", "lavfi",
            "-i", "anullsrc=r=44100:cl=mono",
            "-t", str(duration),
            "-c:v", "h264_nvenc",
            "-preset", "p1",
            "-pix_fmt", "yuv420p",
            "-g", "60",
            "-b:v", "2500k",
            "-c:a", "aac",
            "-b:a", "128k",
            "-ar", "44100",
            "-f", "flv",
            f"{rtmp_url}/{stream_key}"
        ]
    
    print(f"Running FFmpeg command: {' '.join(command)}")
    
    # Start FFmpeg process with pipe for input
    process = subprocess.Popen(
        command,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=False,
        bufsize=10**8  # Use a large buffer
    )
    
    # Create a thread to read and display FFmpeg output in real-time
    def ffmpeg_output_reader():
        while True:
            line = process.stderr.readline()
            if not line:
                break
            try:
                line_text = line.decode('utf-8', errors='replace').strip()
                if line_text:
                    print(f"FFmpeg: {line_text}")
            except:
                pass
    
    output_thread = threading.Thread(target=ffmpeg_output_reader)
    output_thread.daemon = True
    output_thread.start()
    
    print(f"Stream started at {time.strftime('%H:%M:%S')}")
    print(f"Stream will run for {duration} seconds")
    print("Check your YouTube Studio to see if the stream appears")
    print("Preview window opened - press 'q' to stop streaming")
    
    try:
        # Show preview while FFmpeg is streaming
        start_time = time.time()
        frame_count = 0
        
        while time.time() - start_time < duration:
            # Check if FFmpeg process is still running
            if process.poll() is not None:
                print("\nFFmpeg process terminated unexpectedly")
                break
                
            # Read frame from webcam for preview
            ret, frame = cap.read()
            if not ret:
                print("Error: Failed to capture frame from webcam")
                time.sleep(0.5)
                continue
                
            # Add timestamp and info to preview
            elapsed = time.time() - start_time
            remaining = max(0, int(duration - elapsed))
            cv2.putText(frame, f"Time: {elapsed:.1f}s", (50, 50), 
                        cv2.FONT_HERSHEY_SIMPLEX, 1, (0, 255, 0), 2)
            cv2.putText(frame, f"Remaining: {remaining}s", (50, 100), 
                        cv2.FONT_HERSHEY_SIMPLEX, 1, (0, 255, 0), 2)
            cv2.putText(frame, "YouTube Test Stream", (50, 150),
                        cv2.FONT_HERSHEY_SIMPLEX, 1, (0, 255, 255), 2)
            
            # Display the frame locally
            cv2.imshow('YouTube Stream Preview', frame)
            
            # Send the frame to FFmpeg
            try:
                process.stdin.write(frame.tobytes())
            except BrokenPipeError:
                print("\nFFmpeg pipe broken")
                break
                
            frame_count += 1
            
            # Break the loop if 'q' is pressed
            if cv2.waitKey(1) & 0xFF == ord('q'):
                print("\nUser requested stop")
                break
                
            # Print progress (overwrite line)
            print(f"\rStreaming... {remaining} seconds remaining - Frame: {frame_count}", end="")
            
            # Small delay to reduce CPU usage
            time.sleep(0.01)
                
        print("\nStream completed")
        
    except KeyboardInterrupt:
        print("\nStream interrupted by user")
    except Exception as e:
        print(f"Error streaming webcam: {str(e)}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        # Clean up resources
        cap.release()
        cv2.destroyAllWindows()
        
        # Make sure to terminate the FFmpeg process
        if process and process.poll() is None:
            print("Terminating FFmpeg process...")
            try:
                process.stdin.close()  # Close stdin to signal EOF to FFmpeg
            except:
                pass
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                print("FFmpeg process did not terminate, killing it...")
                process.kill()
    
    return True

def list_webcam_devices():
    """List available webcam devices using FFmpeg"""
    print("Listing available webcam devices...")
    
    try:
        process = subprocess.Popen(
            ["ffmpeg", "-list_devices", "true", "-f", "dshow", "-i", "dummy"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        
        stdout, stderr = process.communicate()
        stderr_text = stderr.decode('utf-8', errors='replace')
        
        # Extract and print webcam devices
        print("\nAvailable video devices:")
        lines = stderr_text.split('\n')
        in_video_devices = False
        devices = []
        
        for line in lines:
            if "DirectShow video devices" in line:
                in_video_devices = True
                continue
            elif "DirectShow audio devices" in line:
                in_video_devices = False
                continue
                
            if in_video_devices and "Alternative name" not in line and "\"" in line:
                try:
                    device = line.split("\"")[1]
                    devices.append(device)
                    print(f"  - {device}")
                except IndexError:
                    continue
                
        return devices
    except Exception as e:
        print(f"Error listing webcam devices: {str(e)}")
        return []

def main():
    """
    Test script for YouTube streaming using webcam.
    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Test YouTube streaming with webcam")
    parser.add_argument("--key", default="3qsu-m42f-vp02-9w0r-f42a", help="YouTube stream key")
    parser.add_argument("--software", action="store_true", help="Force software encoding")
    parser.add_argument("--duration", type=int, default=30, help="Duration in seconds (0 for unlimited)")
    parser.add_argument("--width", type=int, default=1280, help="Frame width")
    parser.add_argument("--height", type=int, default=720, help="Frame height")
    parser.add_argument("--diagnose", action="store_true", help="Run diagnostic tests only")
    parser.add_argument("--rtmp", default="rtmp://a.rtmp.youtube.com/live2", help="RTMP server URL")
    parser.add_argument("--verbose", action="store_true", help="Show verbose output")
    parser.add_argument("--test-pattern", action="store_true", help="Stream test pattern instead of webcam")
    parser.add_argument("--webcam-direct", action="store_true", help="Stream webcam directly with FFmpeg")
    parser.add_argument("--try-all-servers", action="store_true", help="Try all YouTube RTMP servers")
    parser.add_argument("--list-devices", action="store_true", help="List available webcam devices")
    parser.add_argument("--use-streammanager", action="store_true", help="Use StreamManager instead of direct FFmpeg")
    parser.add_argument("--preview", action="store_true", help="Show webcam preview while streaming")
    args = parser.parse_args()
    
    # List devices if requested
    if args.list_devices:
        list_webcam_devices()
        return 0
    
    # Use the provided stream key
    stream_key = args.key
    rtmp_url = args.rtmp
    
    print("Starting YouTube streaming test")
    print(f"Using stream key: {stream_key[:4]}...{stream_key[-4:]}")
    print(f"RTMP URL: {rtmp_url}")
    print(f"Resolution: {args.width}x{args.height}")
    print(f"Encoding: {'Software (CPU)' if args.software else 'Hardware (NVIDIA)'}")
    
    # Run diagnostics if requested
    if args.diagnose:
        print("\n=== Running Diagnostics ===")
        verify_ffmpeg_installation()
        
        # Try different YouTube RTMP servers
        youtube_servers = [
            "rtmp://a.rtmp.youtube.com/live2",
            "rtmp://b.rtmp.youtube.com/live2",
            "rtmp://c.rtmp.youtube.com/live2",
            "rtmp://d.rtmp.youtube.com/live2"
        ]
        
        for server in youtube_servers:
            print(f"\nTesting server: {server}")
            check_rtmp_server(server)
            test_ffmpeg_rtmp(server)
        
        # Test webcam
        print("\nTesting webcam...")
        list_webcam_devices()
        cap = cv2.VideoCapture(0)
        if cap.isOpened():
            analyze_webcam_output(cap)
            cap.release()
        else:
            print("Failed to open webcam")
            
        print("=== Diagnostics Complete ===\n")
        return 0
    
    # Try all YouTube RTMP servers
    if args.try_all_servers:
        youtube_servers = [
            "rtmp://a.rtmp.youtube.com/live2",
            "rtmp://b.rtmp.youtube.com/live2",
            "rtmp://c.rtmp.youtube.com/live2",
            "rtmp://d.rtmp.youtube.com/live2"
        ]
        
        for server in youtube_servers:
            print(f"\nTrying server: {server}")
            if args.test_pattern:
                if stream_test_pattern(stream_key, server, args.duration, args.software):
                    print(f"Successfully streamed to {server}")
                    return 0
            elif args.webcam_direct or not args.use_streammanager:
                if args.preview:
                    return 0 if stream_webcam_with_preview(stream_key, server, args.duration, args.software, args.width, args.height) else 1
                else:
                    return 0 if stream_webcam_direct(stream_key, server, args.duration, args.software, args.width, args.height) else 1
            else:
                # Set the RTMP URL for the StreamManager
                rtmp_url = server
                # Continue with webcam streaming (code below)
                break
    
    # Stream test pattern if requested
    if args.test_pattern:
        return 0 if stream_test_pattern(stream_key, rtmp_url, args.duration, args.software) else 1
    
    # Stream webcam directly if requested or by default
    if args.webcam_direct or not args.use_streammanager:
        if args.preview:
            return 0 if stream_webcam_with_preview(stream_key, rtmp_url, args.duration, args.software, args.width, args.height) else 1
        else:
            return 0 if stream_webcam_direct(stream_key, rtmp_url, args.duration, args.software, args.width, args.height) else 1
    
    # Only use StreamManager if specifically requested
    print("\nUsing StreamManager (legacy mode)...")
    
    # Create a StreamManager
    stream_manager = StreamManager(frame_width=args.width, frame_height=args.height)
    
    # Set the stream key and RTMP URL
    stream_manager.stream_key = stream_key
    if rtmp_url != "rtmp://a.rtmp.youtube.com/live2":
        # Override the default RTMP URL if a custom one is provided
        stream_manager.stream_url = f"{rtmp_url}/{stream_key}"
    
    # Disable frame resizing
    stream_manager.set_adapt_to_frame_size(False)
    
    # Set encoding mode based on command line argument
    if args.software:
        stream_manager.set_software_encoding(True)
    
    # Check RTMP server before starting
    print("\nVerifying connection to YouTube RTMP server...")
    if not check_rtmp_server(rtmp_url):
        print("Warning: Could not verify YouTube RTMP server connection")
        response = input("Continue anyway? (y/n): ")
        if response.lower() != 'y':
            return 1
    
    # Create a queue for frames
    frames_queue = queue.Queue(maxsize=120)
    
    # Initialize webcam
    print("Initializing webcam...")
    cap = cv2.VideoCapture(0)
    
    # Check if webcam is opened successfully
    if not cap.isOpened():
        print("Error: Could not open webcam")
        return 1
    
    # Set webcam resolution
    cap.set(cv2.CAP_PROP_FRAME_WIDTH, args.width)
    cap.set(cv2.CAP_PROP_FRAME_HEIGHT, args.height)
    
    # Get actual webcam resolution (may differ from requested)
    actual_width = cap.get(cv2.CAP_PROP_FRAME_WIDTH)
    actual_height = cap.get(cv2.CAP_PROP_FRAME_HEIGHT)
    print(f"Webcam initialized with resolution: {actual_width}x{actual_height}")
    
    # Analyze webcam output
    analyze_webcam_output(cap)
    
    # Verify what we're sending to YouTube
    print("\nVideo stream being sent to YouTube:")
    print(f"- Resolution: {actual_width}x{actual_height}")
    print(f"- Codec: {'h264_nvenc (NVIDIA GPU)' if not args.software else 'libx264 (CPU)'}")
    print(f"- Format: FLV (Flash Video)")
    print(f"- Color space: BGR24 -> YUV420P (converted by FFmpeg)")
    print(f"- Frame rate: 30 fps")
    print(f"- Bitrate: 2500 kbps")
    print(f"- Keyframe interval: 60 frames (2 seconds)")
    
    # Start streaming
    print("\nStarting YouTube stream...")
    if not stream_manager.start_streaming(frames_queue):
        print("Failed to start streaming")
        cap.release()
        return 1
    
    print("\nIMPORTANT: If YouTube still shows 'Connect streaming software to go live':")
    print("1. Verify your stream key is correct and active")
    print("2. Check that your YouTube account has live streaming enabled")
    print("3. Try using the --software flag if using NVIDIA encoding")
    print("4. It may take up to 30-60 seconds for YouTube to detect the stream\n")
    
    # Stream from webcam
    start_time = time.time()
    frame_count = 0
    error_count = 0
    max_errors = 5  # Maximum number of errors before giving up
    
    print("Streaming from webcam. Press 'q' to stop...")
    
    try:
        while error_count < max_errors:
            # Check if duration limit reached
            if args.duration > 0 and (time.time() - start_time) > args.duration:
                print(f"Duration limit of {args.duration} seconds reached")
                break
                
            # Capture frame from webcam
            ret, frame = cap.read()
            
            if not ret:
                print("Error: Failed to capture frame from webcam")
                error_count += 1
                time.sleep(0.5)
                continue
            
            # Add timestamp and info to frame
            elapsed = time.time() - start_time
            cv2.putText(frame, f"Time: {elapsed:.1f}s", (50, 50), 
                        cv2.FONT_HERSHEY_SIMPLEX, 1, (0, 255, 0), 2)
            cv2.putText(frame, f"Frames: {frame_count}", (50, 100), 
                        cv2.FONT_HERSHEY_SIMPLEX, 1, (0, 255, 0), 2)
            cv2.putText(frame, "YouTube Test Stream", (50, 150),
                        cv2.FONT_HERSHEY_SIMPLEX, 1, (0, 255, 255), 2)
            
            # Send the frame
            try:
                success = stream_manager.stream_frame(frame)
                if not success:
                    print(f"Warning: Failed to stream frame {frame_count}")
                    error_count += 1
                else:
                    error_count = 0  # Reset error count on success
                frame_count += 1
            except Exception as e:
                print(f"Error streaming frame: {str(e)}")
                error_count += 1
            
            # Display the frame locally
            cv2.imshow('Webcam Stream', frame)
            
            # Break the loop if 'q' is pressed
            if cv2.waitKey(1) & 0xFF == ord('q'):
                print("User requested stop")
                break
            
            # Add a small delay to reduce CPU usage
            time.sleep(0.01)
            
    except KeyboardInterrupt:
        print("Test interrupted by user")
    finally:
        # Stop streaming
        print("Stopping stream...")
        stream_manager.stop_streaming()
        
        # Release webcam and close display window
        cap.release()
        cv2.destroyAllWindows()
        
    # Print summary
    elapsed_time = time.time() - start_time
    print(f"Test completed. Streamed {frame_count} frames in {elapsed_time:.1f} seconds")
    print(f"Average FPS: {frame_count / elapsed_time:.1f}")
    
    if error_count >= max_errors:
        print("Stream stopped due to too many errors")
        print("\nPossible issues:")
        print("1. Invalid stream key - verify in YouTube Studio")
        print("2. Network connectivity problems - check your internet connection")
        print("3. YouTube server issues - check YouTube status page")
        print("4. FFmpeg configuration problems - try different settings")
        print("\nTroubleshooting steps:")
        print("1. Run with --diagnose flag to check connectivity")
        print("2. Try running with --software flag to use CPU encoding instead of NVIDIA")
        print("3. Try a lower resolution with --width 854 --height 480")
        print("4. Check if your firewall is blocking outgoing connections on port 1935")
        print("5. Try the --test-pattern flag to bypass the webcam")
        print("6. Try the --try-all-servers flag to test all YouTube RTMP servers")
        return 1
    
    print("\nIf YouTube still shows 'Connect streaming software to go live':")
    print("1. Wait a few more minutes - sometimes YouTube takes time to register the stream")
    print("2. Verify your stream key is correct in YouTube Studio")
    print("3. Make sure your YouTube account has live streaming enabled")
    print("4. Try running with the --diagnose flag to check connectivity")
    print("5. Try the --test-pattern flag to bypass the webcam")
    
    return 0

if __name__ == "__main__":
    sys.exit(main()) 