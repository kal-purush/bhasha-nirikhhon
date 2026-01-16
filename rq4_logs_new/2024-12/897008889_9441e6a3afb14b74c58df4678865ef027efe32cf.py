import socket
import ipaddress
import logging
import tempfile
import pathlib
import argparse
import shutil
import sys
import subprocess

from typing import TypeAlias

import psutil
# import httpx

_LG: logging.Logger = logging.getLogger(__name__)
# _DEFAULT_TTL: int = 600

# Python 3.12 only
# type Ips = ipaddress.IPv4Address | ipaddress.IPv6Address
Ips: TypeAlias = ipaddress.IPv4Address | ipaddress.IPv6Address

def remove_ips_in_ignore_list(hostname: str, ips: list[Ips], ignore_networks: list[Ips]) -> list[Ips]:
    def in_networks(ip: ipaddress.IPv4Address | ipaddress.IPv6Address, networks: list[Ips]):
        in_network: bool = False

        for network in networks:
            if ip in network:
                in_network = True
                break

        return in_network

    valid_ips: list[Ips] = []

    for ip in ips:
        temp_ip: Ips = ipaddress.ip_address(ip)
        if not in_networks(temp_ip, ignore_networks):
            valid_ips.append(temp_ip)
        else:
            _LG.debug("IP %s was found in the ignore list. Skipping.", temp_ip)
        
    return valid_ips

def build_nsupdate_commands(dns_server: str, hosts: list[str], forward_fqdn: str, 
                            ips: list[Ips], ttl: int = 0, do_reverse_zone: bool = False) -> str:
    #TODO validate forward_fqdn
    #TODO build arpa zone
    nsupdate_commands: list[str] = [f"server {dns_server}"]
    fqdns: list[str] = [host + "." + forward_fqdn for host in hosts]
    
    for fqdn in fqdns:
        for ip in ips:
            zone_cmds: list[str] = ["update", "add", fqdn]
            if ttl:
                zone_cmds.append(str(ttl))
            zone_cmds.append("IN")
            if isinstance(ip, ipaddress.IPv4Address):
                zone_cmds.append("A")
                zone_cmds.append(str(ip))
            elif isinstance(ip, ipaddress.IPv6Address):
                zone_cmds.append("AAAA")
                zone_cmds.append(str(ip))
            else:
                raise TypeError("The IP type informmed is not supported. Only IPv4 or IPv6 Address.")
            zone_cmds.append(" ")
            nsupdate_commands.append(" ".join(zone_cmds))
            _LG.debug("Command '%s' has been added", nsupdate_commands[-1])
            # TODO arpa zone
            # cmds
            #  "update add <arpa> <ttl> IN A <fqdn>",
            # "send",
    nsupdate_commands.append("send")

    return "\n".join(nsupdate_commands)


def run_nsupdate(nsupate_path: str, commands: str, key_file_path: pathlib.Path) -> subprocess.CompletedProcess:
    _LG.debug("Attempting to create temporary file with commands...")
    # Python 3.12 only
    # with tempfile.NamedTemporaryFile(delete_on_close=False) as fp:
    with tempfile.NamedTemporaryFile(delete=False) as fp:
        _LG.debug("Dumping commands in temporary file...")
        fp.write(commands.encode("utf-8"))
        fp.close()
        _LG.info("Temporary file is ready.")

        cmd: list[str] = [nsupate_path, "-k", key_file_path, fp.name]
        completed_process: subprocess.CompletedProcess = subprocess.run(cmd, capture_output=True, 
                                                   check=True, timeout=60)
        
    #delete file
    _LG.debug("Removing temporary file...")
    pathlib.Path(fp.name).unlink()

    return completed_process

def get_ips_from_interfaces() -> list[Ips]:
    valid_families: socket.AddressFamily = [socket.AF_INET, socket.AF_INET6]
    ips: list[Ips] = []

    addrs = psutil.net_if_addrs()
    for if_name, addresses in addrs.items():
        for address_info in addresses:
            if address_info.family in valid_families:
                ips.append(address_info.address)
                
    return ips

def _config_log(level: int) -> None:
    log_level: int = 0
    ch: logging.StreamHandler = logging.StreamHandler()

    if level == 1:
        log_level = logging.INFO
    elif level >= 2:
        log_level = logging.DEBUG
        
    _LG.setLevel(log_level)
    ch.setLevel(log_level)
    _LG.addHandler(ch)

    # formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    # ch.setFormatter(formatter)


def get_args():
    parser = argparse.ArgumentParser(
                    prog="pynsupdate",
                    description="A wrapper to automatically update DNS entries using nsupdate")
    hostname: str = socket.gethostname()
    default_ignore_networks: list[Ips] =  [
        "127.0.0.0/8",
        "169.254.0.0/16",
        "fe80::/10",
        "::1/128"
    ]

    parser.add_argument("--host", action="extend", nargs="*", dest="hostnames",
                        help=("The hostnames to be added to the zone. Can be specified multiple times. "
                        "The special value '<HOSTNAME>' get replaced by the machine's hostname"))
    parser.add_argument("--zone", action="store", required=True, type=str,
                        help=("The zone the hosts will be added to, in the format '<subdomain>.<domain>.<tld>.'"
                        "Note the '.' at the end. Together with the 'host' options this should form a FQDN."))
    parser.add_argument("--dns-server", action="store", required=True, type=str, dest="dns_server",
                        help="Address of the DNS Server. Must be a resovable name or an IP.")
    parser.add_argument("--ip", action="extend", nargs="*", dest="ips",
                        help=("The IPs that will be updated or added to the zone. If not provided, "
                        "the machine IPs will be used. Can be specified multiple times."))
    parser.add_argument("--ignore-network", action="extend", nargs="*", dest="ignore_networks", #type=ipaddress.ip_network,
                        help=("IPs belonging to the networks provided in this option will be removed "
                        "from the updating list. If not provided, it defaults to 127.0/8, 169.254/16 and fe80::/10."))  
    # parser.add_argument("--add-reverse-zone", action="store_true", dest="reverse_zone", default=False,
    #                     help=("Automatically generate the reverse zones and add them to the update request. "
    #                     "The DNS server holding the zones must be the same and use the same key to update."))
    parser.add_argument("--key-file", action="store", required=True, type=pathlib.Path, dest="key_file",
                        help=("The path to the location of the key file."
                        ""))
    parser.add_argument("--ttl", action="store", type=int, default=0,
                        help=("The TTL that will associated to every entry, in seconds. If not provided "
                        "it will use the default value in the zone configuration file."))
    parser.add_argument("--verbose", "-v", action="count", default=0,
                        help="Enable or disable verbose.")
    # parser.add_argument("--zone", action="store",
    #                     help="")

    args = parser.parse_args()
    args.reverse_zone = False
    
    return args

def validate_args(args: argparse.Namespace) -> None:
    hostname: str = socket.gethostname()
    default_ignore_networks: list[str] =  [
        "127.0.0.0/8",
        "169.254.0.0/16",
        "fe80::/10",
        "::1/128"
    ]

    if not args.hostnames or args.hostnames is None:
        _LG.info("No hostnames provided. Using machine's hostname.")
        args.hostnames = [hostname]
    else:
        try:
            _LG.debug("Replacing meta <HOSTNAME> with correct hostname...")
            args.hostnames[args.hostnames.index("<HOSTNAME>")] = hostname
        except ValueError as e:
            _LG.debug("Meta <HOSTNAME>  not found. Skip.")

    if not args.zone.endswith("."):
        parser.error("The --zone option must end in a dot ('.').")
    if not args.ignore_networks or args.ignore_networks is None:
        args.ignore_networks = default_ignore_networks #[ ipaddress.ip_network(ip) for ip in default_ignore_networks]
    if args.ips is None:
        _LG.info("No IPs provided. Getting IPs assigned to the machine.")
        args.ips = get_ips_from_interfaces()
        _LG.debug("IP list: %s", args.ips)
    if args.verbose:
        _config_log(args.verbose)
    
    _LG.debug("Arguments: %s", args)

def main():
    args = get_args()
    validate_args(args)
    nsupate_path: str = shutil.which("nsupdate")

    try:
        if nsupate_path is None:
            raise RuntimeError("Can't find the 'nsupdate' binary. Please check your PATH variable.")

        ignore_networks: list[Ips] = [ipaddress.ip_network(net) for net in args.ignore_networks]
        record_ips: list[Ips] = [ipaddress.ip_address(ip) for ip in args.ips]
        _LG.debug("IPs in these networks will be ignored: %s", args.ignore_networks)

        ips: list[Ips] = remove_ips_in_ignore_list(socket.gethostname(), record_ips, ignore_networks)
        _LG.debug("List of valid IPs: %s", str(ips))
        if not ips:
            print("No valid IPs available. Nothing to do.")
        else:
            commands: str = build_nsupdate_commands(args.dns_server, args.hostnames, args.zone, ips, args.ttl, args.reverse_zone)
            completed_process: subprocess.CompletedProcess = run_nsupdate(nsupate_path, commands, args.key_file)

            if not completed_process.returncode:
                _LG.debug("'nsupdate' output: %s", completed_process.stdout)
    except ValueError as e:
        # Case for ValueError raised from converting the IPs and networks
        print(e, file=sys.stderr)
        sys.exit(3)
    except RuntimeError as e:
        print(e, file=sys.stderr)
        sys.exit(4)
    except subprocess.TimeoutExpired as e:
        print("'nsupdate' has not finished in time. Potentially no entries were updated", file=sys.stderr)
        sys.exit(5)
    except subprocess.CalledProcessError as e:
        print("'nsupdate' has finished with errors. Runs with '-vvv' for more details", file=sys.stderr)
        print("'nsupdate' error output:", str(e.stderr))
        _LG.debug("ns update output output: %s", e.stdout)
        sys.exit(5)        


if __name__ == "__main__":
    # Options? list of ignored, list of ips, find ipv4 externally, find ipb6 externally
    main()
    
