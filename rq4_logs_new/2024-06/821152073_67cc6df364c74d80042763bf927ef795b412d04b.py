#!/usr/bin/python3
import csv
import subprocess
import os

def get_job_id_from_pid(pid):
    """
    Extracts the job ID for a given process ID (PID) from the cgroup info assuming it is a SLURM job.

    :param pid: Process ID for which to find the job ID.
    :return: Job ID as an integer if found, None otherwise.
    """
    cmd = f"grep 'slurm' /proc/{pid}/cgroup | grep -o 'job_[0-9]*' | grep -o '[0-9]*'"
    try:
        job_id = subprocess.check_output(cmd, shell=True).decode('utf-8').strip()
        return int(job_id.split('\n')[0]) if job_id else None
    except subprocess.CalledProcessError:
        return None

def parse_csv(file_path):
    """
    Parses a CSV file into a list of dictionaries.

    :param file_path: Path to the CSV file to parse.
    :return: A list of dictionaries, each representing a row in the CSV.
    """
    data = []
    with open(file_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            cleaned_row = {k.strip(): v.strip() for k, v in row.items()}
            data.append(cleaned_row)
    return data

def get_nvidia_metrics():
    """
    Gathers NVIDIA GPU metrics using the nvidia-smi tool and stores the output in CSV files.
    It then parses these CSV files to prepare a mapping of GPU metrics.

    :return: A mapping dictionary containing GPU metrics keyed by GPU index.
    """
    # Execute nvidia-smi commands to gather GPU info and compute application usage
    try:
        subprocess.run('nvidia-smi --query-gpu=gpu_uuid,index,name,utilization.gpu --format=csv > gpu_info.csv', shell=True, check=True)
        subprocess.run('nvidia-smi --query-compute-apps=pid,used_gpu_memory,gpu_uuid --format=csv > compute_apps_usage.csv', shell=True, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error running nvidia-smi commands: {e}")
        return {}

    # Parse the output CSV files
    gpu_info = parse_csv('gpu_info.csv')
    compute_apps_usage = parse_csv('compute_apps_usage.csv')

    # Prepare the mapping
    mapping = {}
    for app in compute_apps_usage:
        uuid = app['gpu_uuid']
        for gpu in gpu_info:
            if gpu['uuid'] == uuid:
                index = gpu['index']
                mapping[index] = {
                    'uuid': uuid,
                    'pid': app['pid'],
                    'utilization': gpu['utilization.gpu [%]'].strip(' %'),
                    'used_memory_mib': app['used_gpu_memory [MiB]'].strip(' MiB')
                }

    return mapping

def get_io_metrics(pid_to_job):
    """
    Retrieves I/O metrics for a set of processes, identified by their PIDs, from the Linux proc filesystem.

    :param pid_to_job: A dictionary mapping PIDs to job IDs.
    :return: A dictionary where each key is a PID and each value is another dictionary containing 'read_bytes' and 'write_bytes'.
    """
    io_metrics = {}
    for pid, job_id in pid_to_job.items():
        if job_id is None:  # Skip if no job ID
            continue
        try:
            with open(f'/proc/{pid}/io', 'r') as file:
                io_data = file.read()
            io_metrics[pid] = parse_io_data(io_data)
        except IOError as e:
            print(f"Error reading IO data for PID {pid}: {e}")
            io_metrics[pid] = {'read_bytes': 'N/A', 'write_bytes': 'N/A'}
    return io_metrics

def parse_io_data(io_data):
    """
    Parses the content of an I/O metric file from the Linux proc filesystem for a specific process.

    :param io_data: The content of an I/O metric file as a string.
    :return: A dictionary containing the I/O metrics, specifically 'read_bytes' and 'write_bytes'.
    """
    io_info = {}
    for line in io_data.split('\n'):
        parts = line.split(':')
        if len(parts) == 2:
            key, value = parts
            io_info[key.strip()] = value.strip()
    return io_info

def write_to_textfile_collector(pid_to_job, gpu_metrics, io_metrics):
    """
    Writes GPU and I/O metrics to an output file formatted for compatibility with Prometheus node exporter.

    :param pid_to_job: A dictionary mapping PIDs to job IDs.
    :param gpu_metrics: A dictionary containing GPU metrics for each GPU index.


    :param io_metrics: A dictionary containing I/O metrics for each PID.
    """
    output_file = "/path/to/node_exporter/textfile_collector/metrics.prom"
    with open(output_file, 'w') as f:
        for key, value in gpu_metrics.items():
            minor_number = key
            gpu_utilization = value['utilization']
            gpu_memory_usage_bytes = int(value['used_memory_mib']) * 1024 * 1024
            job_id = get_job_id_from_pid(value['pid'])

            if job_id:
                f.write(f'cgroups_nvidia_gpu_utilization{{gpu_id="{minor_number}", job_id="{job_id}"}} {gpu_utilization}\n')
                f.write(f'cgroups_nvidia_gpu_memory_usage_bytes{{gpu_id="{minor_number}", job_id="{job_id}"}} {gpu_memory_usage_bytes}\n')

        for pid, metrics in io_metrics.items():
            job_id = pid_to_job[pid]
            if job_id and metrics['read_bytes'] != 'N/A' and metrics['write_bytes'] != 'N/A':
                f.write(f'cgroups_io_read_bytes{{pid="{pid}", job_id="{job_id}"}} {metrics["read_bytes"]}\n')
                f.write(f'cgroups_io_write_bytes{{pid="{pid}", job_id="{job_id}"}} {metrics["write_bytes"]}\n')

def main():
    pid_to_job = {pid: get_job_id_from_pid(pid) for pid in os.listdir('/proc') if pid.isdigit()}
    gpu_metrics = get_nvidia_metrics()
    io_metrics = get_io_metrics(pid_to_job)
    write_to_textfile_collector(pid_to_job, gpu_metrics, io_metrics)
   
if __name__ == "__main__":
    main()