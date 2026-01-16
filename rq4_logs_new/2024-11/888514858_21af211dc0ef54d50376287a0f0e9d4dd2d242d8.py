import http.server
import socketserver
import socket
import os
import time
import threading
import requests
from art import text2art
from concurrent.futures import ThreadPoolExecutor

# ANSI color codes
RESET = "\033[0m"
GREEN = "\033[32m"
BLUE = "\033[33m"
CYAN = "\033[36m"
YELLOW = "\033[33m"
RED = "\033[31m"  # Red color code

def clear_screen():
    if os.name == 'nt': 
        os.system('cls')
    else:  
        os.system('clear')

# ASCII
text = "      N3T MAPP3R       "
ascii_art = text2art(text, font="Standard")
red_ascii_art = RED + ascii_art + RESET  # Apply red color to ASCII art

# UDP Test
def udp_stress_test(target_host, target_port, packet_count, packet_size, num_threads, sleep_time):
    try:
        def send_packets():
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) 
                message = b"A" * packet_size  
                for _ in range(packet_count // num_threads):
                    try:
                        sock.sendto(message, (target_host, target_port))
                        time.sleep(sleep_time)
                    except socket.error as e:
                        print(f"Error sending packet: {e}")
                        break
                sock.close()
            except Exception as e:
                print(f"Error in UDP packet sender: {e}")

        
        print(f"\n{'-'*40}")
        print(f"  UDP Packet Sent to: {target_host}:{target_port}")
        print(f"  Total Packets: {packet_count}")
        print(f"  Packet Size: {packet_size} bytes")
        print(f"  Number of Threads: {num_threads}")
        print(f"  Sleep Time Between Packets: {sleep_time} seconds")
        print(f"{'-'*40}\n")

        
        threads = []
        for _ in range(num_threads):
            thread = threading.Thread(target=send_packets)
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        print(f"\n{packet_count} UDP packets sent to {target_host}:{target_port}")
    except KeyboardInterrupt:
        print("\n\nUDP stress test interrupted (Ctrl+C).")
    except socket.error as e:
        print(f"Socket error during UDP stress test: {e}")
    except Exception as e:
        print(f"Error during UDP stress test: {e}")

# Port Mapper
def get_port_info(port):
    """ Returns service information based on the port number """
    port_services = {
        20: "FTP Data Transfer", 21: "FTP Control", 22: "SSH", 23: "Telnet", 
        25: "SMTP", 53: "DNS", 67: "DHCP", 68: "DHCP", 69: "TFTP", 
        80: "HTTP", 110: "POP3", 111: "RPC", 135: "MS RPC", 139: "NetBIOS", 
        443: "HTTPS", 445: "Microsoft-DS", 993: "IMAPS", 995: "POP3S", 
        3389: "RDP", 8080: "HTTP Proxy", 8443: "HTTPS Proxy", 3306: "MySQL", 
        5432: "PostgreSQL", 5900: "VNC", 6379: "Redis"
    }
    return port_services.get(port, "Unknown Service")

def scan_port(host, port):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(1)
            try:
                result = sock.connect_ex((host, port))
                if result == 0:
                    service = get_port_info(port)
                    return port, service
                else:
                    return port, None
            except socket.error as e:
                print(f"Error connecting to {host}:{port} - {e}")
                return port, None
    except Exception as e:
        print(f"Error during port scan for {host}:{port} - {e}")
        return port, None

def scan_ports(host, port_range):
    open_ports = []
    with ThreadPoolExecutor(max_workers=100) as executor:
        futures = {executor.submit(scan_port, host, port): port for port in port_range}
        for future in futures:
            port, service = future.result()
            if service:
                open_ports.append((port, service))
    return open_ports

# Banner Grabber
def grab_banner(host, port):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(1)
            try:
                sock.connect((host, port))
                banner = sock.recv(1024).decode().strip()
                if banner:
                    return port, banner
                return port, None
            except socket.timeout:
                print(f"Timeout while grabbing banner from {host}:{port}")
                return port, None
            except socket.error as e:
                print(f"Error while grabbing banner from {host}:{port} - {e}")
                return port, None
    except Exception as e:
        print(f"Error during banner grabbing for {host}:{port} - {e}")
        return port, None

def banner_grabbing(host, port_range):
    open_ports_with_banners = []
    with ThreadPoolExecutor(max_workers=100) as executor:
        futures = {executor.submit(grab_banner, host, port): port for port in port_range}
        for future in futures:
            port, banner = future.result()
            if banner:
                open_ports_with_banners.append((port, banner))
    return open_ports_with_banners

def get_default_ports():
    return [20, 21, 22, 23, 25, 53, 67, 68, 69, 80, 110, 111, 135, 139, 443, 445, 993, 995, 3389, 8080, 8443, 3306, 3389, 5432, 5900, 6379]

# CMS Identifier
def cms_scanner(url):
    try:
        response = requests.get(url, timeout=5)
        response.raise_for_status()  # Ensure we get a successful response
        if 'wp-content' in response.text:
            return "WordPress"
        elif 'Umbraco' in response.text:
            return "Umbraco"
        elif 'joomla' in response.text.lower():
            return "Joomla"
        elif 'phpcms' in response.text.lower():
            return "PhpCMS"
        elif 'drupal' in response.text.lower():
            return "Drupal"
        elif 'october' in response.text.lower():
            return "October"
        elif 'Apache Tomcat' in response.text.lower():
            return "OpenCms"
        elif 'mage' in response.text.lower():
            return "Magento"
        else:
            return "No CMS detected"
    except requests.RequestException as e:
        print(f"Error during CMS detection: {e}")
        return "Error: Unable to reach the host"

def start_python_server():
    handler = http.server.SimpleHTTPRequestHandler
    port = 80
    try:
        with socketserver.TCPServer(("", port), handler) as httpd:
            print(f"{YELLOW}Serving HTTP on port {port}...")
            print("\n")
            print(f"{RED}Press Ctrl + C to stop the server.")
            httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nHTTP server stopped by user (Ctrl + C).")
        clear_screen()

# IP Geolocation
def ip_geolocation_lookup(ip_address):
    try:
        # Using the ipinfo.io API to get geolocation info
        api_url = f"http://ipinfo.io/{ip_address}/json"
        response = requests.get(api_url)
        response.raise_for_status()  # Ensure we get a successful response
        data = response.json()
        
        if "error" in data:
            print("Error: Unable to get geolocation information for this IP.")
        else:
            print(f"IP: {data.get('ip')}")
            print(f"Location: {data.get('city')}, {data.get('region')}, {data.get('country')}")
            print(f"Org: {data.get('org')}")
            print(f"Coordinates: {data.get('loc')}")
    except requests.RequestException as e:
        print(f"Error during IP geolocation lookup: {e}")

# Program
def main():
    while True:
        clear_screen()
        print(red_ascii_art)  # Print ASCII art in red
        print(f"{CYAN}Choose from the options below :")
        print("\n") 
        print(f"{GREEN}[1]{RESET} {BLUE}Port Mapper (Scanner)")
        print(f"{GREEN}[2]{RESET} {BLUE}Banner Grabber")
        print(f"{GREEN}[3]{RESET} {BLUE}UDP Stress Test")
        print(f"{GREEN}[4]{RESET} {BLUE}CMS Scanner")
        print(f"{GREEN}[5]{RESET} {BLUE}HTTP Server")
        print(f"{GREEN}[6]{RESET} {BLUE}IP Geolocation Lookup")
        print(f"{GREEN}[q]{RESET} {RED}Quit")
        
        choice = input(f"{CYAN}Enter your choice: ")

        if choice == '1':
            target_host = input(f"{CYAN}Enter the target IP address: ")
            port_range = list(map(int, input(f"{CYAN}Enter port range (e.g., 20-100): ").split('-')))
            open_ports = scan_ports(target_host, range(port_range[0], port_range[1]+1))
            for port, service in open_ports:
                print(f"{GREEN}Port {port} is open, Service: {service}")
            input(f"{CYAN}Press Enter to continue...")
        
        elif choice == '2':
            target_host = input(f"{CYAN}Enter the target IP address: ")
            port_range = list(map(int, input(f"{CYAN}Enter port range (e.g., 20-100): ").split('-')))
            open_ports_with_banners = banner_grabbing(target_host, range(port_range[0], port_range[1]+1))
            for port, banner in open_ports_with_banners:
                print(f"{GREEN}Port {port} banner: {banner}")
            input(f"{CYAN}Press Enter to continue...")
        
        elif choice == '3':
            target_host = input(f"{CYAN}Enter the target IP address: ")
            target_port = int(input(f"{CYAN}Enter the target port: "))
            packet_count = int(input(f"{CYAN}Enter the number of packets: "))
            packet_size = int(input(f"{CYAN}Enter the packet size in bytes: "))
            num_threads = int(input(f"{CYAN}Enter number of threads: "))
            sleep_time = float(input(f"{CYAN}Enter the sleep time between packets (in seconds): "))
            udp_stress_test(target_host, target_port, packet_count, packet_size, num_threads, sleep_time)

        elif choice == '4':
            url = input(f"{CYAN}Enter the URL for CMS detection: ")
            cms = cms_scanner(url)
            print(f"{CYAN}CMS Detected: {cms}")
            input(f"{CYAN}Press Enter to continue...")

        elif choice == '5':
            start_python_server()

        elif choice == '6':
            ip_address = input(f"{CYAN}Enter the IP address for geolocation lookup: ")
            ip_geolocation_lookup(ip_address)

        elif choice.lower() == 'q':
            print(f"{RED}Goodbye!")
            break

if __name__ == "__main__":
    main()