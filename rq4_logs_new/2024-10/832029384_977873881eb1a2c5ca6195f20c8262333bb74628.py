import json
import logging
from datetime import datetime, timedelta
from logging import config
import pathlib
import os  # Import os for checking file existence


def setup_logging():
    # Load logging configuration from a JSON file
    config_file = pathlib.Path("./logger_config.json")
    with open(config_file) as file:
        desired_configuration = json.load(file)

    logging.config.dictConfig(desired_configuration)


logger = logging.getLogger(__name__)


class HeartbeatMonitoringLogger:

    # Default thread pattern for filtering log entries
    __thread_pattern: str = "Key TSTFEED0240|7E3E|0400"
    __filtered_lines_list: list[str]

    def __init__(self, thread_pattern: str):
        # Initialize the logger and set the thread pattern
        setup_logging()
        self.__thread_pattern = thread_pattern

    def check_system_and_return_logs(self):
        # Get filtered log lines in reverse order
        self.__filtered_lines_list = list(self.__get_lines_with_desired_pattern().__reversed__())

        # Iterate through the filtered log lines
        for period, line in enumerate(self.__filtered_lines_list):
            if period < len(self.__filtered_lines_list) - 1:
                # Get the start and end timestamps from the log lines
                start_time: datetime = self.__format_str_time_into_datetime(
                    self.__return_timestamp_value_from_line(line))

                end_time: datetime = self.__format_str_time_into_datetime(
                    self.__return_timestamp_value_from_line(self.__filtered_lines_list[period + 1]))

                # Log messages based on the time difference between two timestamps
                self.__log_based_on_timedelta(start_time, end_time)

    def __log_based_on_timedelta(self, start_time: datetime, end_time: datetime):
        # Log warning or error messages based on the time difference
        if (end_time - start_time) == timedelta(seconds=32):
            self.__write_warning_logs()
        elif (end_time - start_time) >= timedelta(seconds=33):
            self.__write_error_logs()

    def __format_str_time_into_datetime(self, str_time) -> datetime:
        # Convert a string time in the format H:M:S into a datetime object
        return datetime.strptime(str_time, "%H:%M:%S")

    def __get_lines_with_desired_pattern(self) -> list[str]:
        # Check if the log file exists
        log_file_path = "./hblog.txt"
        if not os.path.exists(log_file_path):
            logger.error(f"Log file {log_file_path} does not exist.")
            return []

        try:
            # Open the log file and filter lines containing the desired thread pattern
            with open(log_file_path, "r") as file:
                return [line.strip() for line in file if line.find(self.__thread_pattern) != -1]
        except Exception as e:
            logger.error(f"Error reading log file: {e}")
            return []

    def __return_timestamp_value_from_line(self, line: str) -> str:
        # Split the line and return the value following the "Timestamp" keyword
        list_of_line_items: list[str] = line.split()

        # Check if "Timestamp" is in the line to avoid index errors
        if "Timestamp" in list_of_line_items:
            return list_of_line_items[list_of_line_items.index("Timestamp") + 1]
        else:
            logger.error(f"Timestamp not found in line: {line}")
            return "00:00:00"  # Return a default time if "Timestamp" is missing

    def __write_warning_logs(self) -> None:
        # Log a warning message
        logger.warning("This is a warning log")

    def __write_error_logs(self) -> None:
        # Log an error message
        logger.error("This is an error log")


# Create an instance of HeartbeatMonitoringLogger with a specific thread pattern
heart_beat_monitoring_logger: HeartbeatMonitoringLogger = HeartbeatMonitoringLogger("Key TSTFEED0300|7E3E|0400")

# Check the system and return logs
heart_beat_monitoring_logger.check_system_and_return_logs()