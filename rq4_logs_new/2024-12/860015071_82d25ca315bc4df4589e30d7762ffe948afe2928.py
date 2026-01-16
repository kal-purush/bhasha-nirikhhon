import logging
from datetime import datetime
import pytest

logging.basicConfig(
    filename='hb_test.log',
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    force=True
)
logger = logging.getLogger("log_event")


def check_logs(logs_to_read):
    with open(logs_to_read, 'r') as log_file:
        content = log_file.read()
        lines = content.splitlines()
        previous_date = None

        for line in lines:
            if "Key TSTFEED0300|7E3E|0400" in line:
                timeStamp: str = line.split("Timestamp ")[1].split(" ")[0]
                date = datetime.strptime(timeStamp, "%H:%M:%S")
                if previous_date is None:
                    previous_date = date
                    continue
                else:
                    dif_time = previous_date - date
                    write_new_logs(dif_time.total_seconds(), date, line)
                    previous_date = date


def write_new_logs(dif_time, date: datetime, line):
    date_str: str = f"{date.hour}:{date.minute}:{date.second}"
    if 31 <= dif_time < 33:
        logger.warning(f"WARNING  {date_str} = {line}")
    elif dif_time >= 33:
        logger.error(f"ERROR {date_str} = {line}")


@pytest.mark.parametrize("logs_to_read, logs_to_write", [
    ("hblog.txt", "hb_test.log")
])
def test_log_file(logs_to_read, logs_to_write):
    check_logs(logs_to_read)
    with open(logs_to_write, 'r') as log_file:
        content = log_file.read()
        assert len(content) > 0
        assert "WARNING" in content
        assert "ERROR" in content
