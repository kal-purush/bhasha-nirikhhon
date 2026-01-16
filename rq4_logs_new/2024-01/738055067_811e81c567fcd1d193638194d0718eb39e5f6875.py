#!/usr/bin/python3

import subprocess
from colorama import Fore, Style

def is_nmap_installed():
    try:
        subprocess.check_output(['which', 'nmap'])
        return True
    except subprocess.CalledProcessError:
        return False

def run_command(command):
    try:
        output = subprocess.check_output(command, shell=True)
        return output.decode('utf-8')
    except Exception as e:
        print("Error during command execution: {}".format(e))
        return ""

def print_banner():
    print(Fore.RED + Style.BRIGHT + r"""
 ▄▄▄▄▄▄▄ ▄▄   ▄▄ ▄▄   ▄▄ ▄▄▄▄▄▄▄ ▄▄    ▄    ▄▄▄▄▄▄▄ ▄▄▄▄▄▄▄ ▄▄▄▄▄▄▄ ▄▄    ▄ ▄▄    ▄ ▄▄▄▄▄▄▄ ▄▄▄▄▄▄   
█       █  █ █  █  █▄█  █       █  █  █ █  █       █       █       █  █  █ █  █  █ █       █   ▄  █  
█  ▄▄▄▄▄█  █ █  █       █   ▄   █   █▄█ █  █  ▄▄▄▄▄█       █   ▄   █   █▄█ █   █▄█ █    ▄▄▄█  █ █ █  
█ █▄▄▄▄▄█  █▄█  █       █  █▄█  █       █  █ █▄▄▄▄▄█     ▄▄█  █▄█  █       █       █   █▄▄▄█   █▄▄█▄ 
█▄▄▄▄▄  █       █       █       █  ▄    █  █▄▄▄▄▄  █    █  █       █  ▄    █  ▄    █    ▄▄▄█    ▄▄  █
 ▄▄▄▄▄█ █       █ ██▄██ █   ▄   █ █ █   █   ▄▄▄▄▄█ █    █▄▄█   ▄   █ █ █   █ █ █   █   █▄▄▄█   █  █ █
█▄▄▄▄▄▄▄█▄▄▄▄▄▄▄█▄█   █▄█▄▄█ █▄▄█▄█  █▄▄█  █▄▄▄▄▄▄▄█▄▄▄▄▄▄▄█▄▄█ █▄▄█▄█  █▄▄█▄█  █▄▄█▄▄▄▄▄▄▄█▄▄▄█  █▄█
""")

def main():
    if not is_nmap_installed():
        print("Nmap is not installed. Please install Nmap and try again.")
        return

    print_banner()
    print(Fore.BLUE + "Combining all steps:")

    # Step 1: nmap -sn -n --min-rate 10000 -T4 10.0.12.0/24 > ip.txt
    subnet = input(Fore.BLUE + "Enter the target subnet (e.g., 10.0.12.0/24): ")
    run_command('nmap -sn -n --min-rate 10000 -T4 {} > ip.txt'.format(subnet))
    print(Fore.GREEN + "Step 1 completed. Scan results saved in 'ip.txt'")

    # Step 2: cat ip.txt | grep "report" | cut -d " " -f 5 > iponly.txt
    run_command('cat ip.txt | grep "report" | cut -d " " -f 5 > iponly.txt')
    print(Fore.GREEN + "Step 2 completed. IP addresses extracted and saved in 'iponly.txt'")

    # Step 3: nmap -sCV --min-rate 10000 -T4 --script smb-security-mode -p 445 -iL iponly.txt > smb-security-modes.txt
    run_command('nmap --min-rate 10000 -T4 --script smb-security-mode -p 445 -iL iponly.txt > smb-security-modes.txt')
    print(Fore.GREEN + "Step 3 completed. Detailed service version detection and smb-security-mode script scan results saved in 'smb-security-modes.txt'")

    # Step 4: smb-protocols
    run_command('nmap --min-rate 10000 -T4 --script smb-protocols -p 139,445 -iL iponly.txt > smb-protocols.txt')
    print(Fore.GREEN + "Step 4 completed. Detailed service version detection and smb-protocols script scan results saved in 'smb-protocols.txt'")

    # Step 5 : ssl-cert
    run_command('nmap --min-rate 10000 -T4 --script ssl-cert -p- -iL iponly.txt > ssl-cert.txt')
    print(Fore.GREEN + "Step 5 completed. Detailed service version detection and ssl-cert script scan results saved in 'ssl-cert.txt'")

    # Step 6 : ssl-enum-ciphers
    run_command('nmap --min-rate 10000 -T4 --script ssl-enum-ciphers -p 443 -iL iponly.txt > ssl-enum-ciphers.txt')
    print(Fore.GREEN + "Step 6 completed. Detailed service version detection and ssl-enum-ciphers scan results saved in 'ssl-enum-ciphers.txt'")

    print(Style.RESET_ALL + Fore.BLUE + "All steps completed. Final output saved in 'smb-security-modes.txt'")

if __name__ == "__main__":
    main()