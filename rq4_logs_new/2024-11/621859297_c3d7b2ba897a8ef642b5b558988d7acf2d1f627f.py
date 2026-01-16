import subprocess
from pathlib import Path
from typing import Optional

from lib.core_utils.config_loader import configs
from lib.core_utils.logging_utils import custom_logger

logging = custom_logger(__name__.split(".")[-1])


def transfer_report(
    report_path: Path, project_id: str, sample_id: Optional[str] = None
) -> bool:
    server = configs["report_transfer"]["server"]
    user = configs["report_transfer"]["user"]
    destination_path = configs["report_transfer"]["destination_path"]
    ssh_key = configs["report_transfer"].get("ssh_key")

    if sample_id:
        remote_path = f"{user}@{server}:{destination_path}/{project_id}/{sample_id}/"
    else:
        remote_path = f"{user}@{server}:{destination_path}/{project_id}/"

    rsync_command = [
        "rsync",
        "-avz",
        "-e",
        f"ssh -i {ssh_key}" if ssh_key else "ssh",
        str(report_path),
        remote_path,
    ]

    try:
        # Execute the rsync command
        subprocess.run(
            rsync_command,
            check=True,
            text=True,
            capture_output=True,
        )

        logging.info(f"Report transferred successfully to {remote_path}")
        return True
    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to transfer report: {e.stderr.strip()}")
        return False
    except Exception as e:
        logging.error(f"Unexpected error during report transfer: {e}")
        return False