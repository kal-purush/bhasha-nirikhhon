import subprocess
import json
import logging
import os
import time
from scapy.all import AsyncSniffer, wrpcap

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def run_bolt_plan(inventory, plan, params=None):
    cmd = [
        "bolt", "plan", "run", plan,
        "--inventory", inventory,
        "--format", "json"
    ]

    if params:
        for key, value in params.items():
            cmd.append(f"{key}={value}")

    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        logging.error(f"Bolt plan failed with error: {result.stderr}")
        return None

    try:
        output = json.loads(result.stdout)
        return output
    except json.JSONDecodeError as e:
        logging.error(f"Failed to parse JSON output: {e}")
        return None

def collect_interface_facts(targets):
    facts = {}
    for target in targets:
        cmd = [
            "bolt", "command", "run", "show ip interface brief",
            "--targets", target,
            "--inventory", "inventory.yaml",
            "--format", "json"
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode != 0:
            logging.error(f"Failed to collect facts for {target} with error: {result.stderr}")
            continue

        try:
            output = json.loads(result.stdout)
            facts[target] = output['results'][0]['value']['stdout']
        except json.JSONDecodeError as e:
            logging.error(f"Failed to parse JSON output for {target}: {e}")
        except KeyError as e:
            logging.error(f"Unexpected output format for {target}: {e}")
    
    return facts

def run_plan_and_collect_data(inventory, plan, targets):
    stats = []

    for i in range(1, 11):
        logging.debug(f"Run {i}")

        # Start packet sniffing
        sniffer = AsyncSniffer()
        sniffer.start()

        start_time = time.time()

        # Run Bolt plan
        result = run_bolt_plan(inventory, plan)

        end_time = time.time()
        duration = end_time - start_time

        # Stop packet sniffing
        packets = sniffer.stop()
        pcap_file = f"{task_name}_packets_{i}.pcap"
        wrpcap(pcap_file, packets)
        logging.debug(f"Packet sniffing stopped for run {i}")

        num_packets = len(packets)
        file_size = os.path.getsize(pcap_file) / 1024  # in KB
        data_size = sum(len(pkt) for pkt in packets) / 1024  # in KB
        data_byte_rate = data_size / duration if duration > 0 else 0  # in KBps
        data_bit_rate = data_byte_rate * 8  # in kbps
        avg_packet_size = (data_size * 1024) / num_packets if num_packets > 0 else 0  # in bytes
        avg_packet_rate = num_packets / duration if duration > 0 else 0  # in packets/s

        logging.debug(f"Run {i} completed in {duration:.2f} seconds")
        logging.debug(f"STDOUT: {result}")

        stats.append({
            "run": i,
            "duration": duration,
            "num_packets": num_packets,
            "file_size": file_size,
            "data_size": data_size,
            "data_byte_rate": data_byte_rate,
            "data_bit_rate": data_bit_rate,
            "avg_packet_size": avg_packet_size,
            "avg_packet_rate": avg_packet_rate,
            "result": result
        })

    return stats

if __name__ == "__main__":
    inventory = "inventory.yaml"
    plan = "configure_routers"
    task_name = "puppet_bolt"

    # Collect interface facts for all targets
    targets = ["R2", "R3", "R4", "R6", "R7", "R8", "R9", "R10", "R11", "R12", "R13", "R14", "R15", "R16"]
    facts = collect_interface_facts(targets)
    
    logging.debug(f"Collected interface facts: {facts}")

    # Run Bolt plan and collect data
    stats = run_plan_and_collect_data(inventory, plan, targets)
    
    logging.debug("Collected stats: %s", stats)
    print("Collected stats:", stats)