# Import required modules
import os
import socket
import time
import logging
import configparser
from typing import Optional
from astropy.io import fits
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Read configuration settings
config = configparser.ConfigParser()
config_file_path = "astro-project/forPc/config.ini"

try:
    config.read(config_file_path)
except FileNotFoundError:
    logging.error(f"Configuration file not found: {config_file_path}")
    exit(1)
except Exception as e:
    logging.error(f"Error reading configuration file: {e}")
    exit(1)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("mittens.log"),
        logging.StreamHandler(),
    ],
)

# Define constants
SOCKET_PI_ADDRESS = config.get("Socket Settings", "pi_address")
SOCKET_PORT = int(config.get("Socket Settings", "port"))
FOLDER_TO_WATCH = config.get("Folder Settings", "folder_path")


def send_azimuth(azimuth: Optional[float]) -> None:
    """
    Send the azimuthal value to the Raspberry Pi over a socket connection.

    Args:
        azimuth (Optional[float]): The azimuthal value to send. If None, send 'stop'.
    """
    try:
        # Create socket object
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # Connect to the server
        sock.connect((SOCKET_PI_ADDRESS, SOCKET_PORT))

        # Send the message (azimuth value or 'stop')
        message = str(azimuth) if azimuth is not None else "stop"
        sock.send(message.encode("utf-8"))

        # Close the connection
        sock.close()

    except socket.error as e:
        logging.error(f"Socket error: {e}")
    except Exception as e:
        logging.error(f"Exception error when sending azimuth: {e}")


class FITSEventHandler(FileSystemEventHandler):
    """
    Event handler for monitoring the FITS image folder and sending azimuthal data.
    """

    def on_created(self, event) -> None:
        """
        Handler for the 'created' event.

        Args:
            event: The event object representing the file system event.
        """
        if event.is_directory:
            return

        # Check if the file is a FITS file
        if os.path.splitext(event.src_path)[1] == ".fits":
            time.sleep(2)  # Wait for the file to be fully written

            # Open the FITS image
            try:
                header = fits.getheader(event.src_path)
                centaz_value = header.get("CENTAZ")
                send_azimuth(centaz_value)
            except Exception as e:
                logging.error(f"Error reading FITS header: {e}")


def main() -> None:
    """
    Main function to start the file monitoring and event handling.
    """
    event_handler = FITSEventHandler()
    observer = Observer()
    observer.schedule(event_handler, FOLDER_TO_WATCH, recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()

    observer.join()


if __name__ == "__main__":
    main()