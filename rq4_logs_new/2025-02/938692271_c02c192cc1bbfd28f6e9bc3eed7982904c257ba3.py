import sys
import os

def extract_logs(log_file, target_date, output_dir="output"):
    """
    Extracts logs for the given date from a large log file.

    Args:
        log_file (str): Path to the log file.
        target_date (str): The date to filter logs (YYYY-MM-DD).
        output_dir (str): Directory to store output files.
    """
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    output_file = os.path.join(output_dir, f"output_{target_date}.txt")

    try:
        with open(log_file, "r", encoding="utf-8") as f, open(output_file, "w", encoding="utf-8") as out:
            for line in f:
                if line.startswith(target_date):  # Efficient date filtering
                    out.write(line)

        print(f"Logs for {target_date} saved to {output_file}")

    except FileNotFoundError:
        print(f"Error: Log file '{log_file}' not found.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python extract_logs.py <log_file_path> <YYYY-MM-DD>")
        sys.exit(1)

    log_file_path = "./logs_2024.log"
    date = sys.argv[2]

    extract_logs(log_file_path, date)