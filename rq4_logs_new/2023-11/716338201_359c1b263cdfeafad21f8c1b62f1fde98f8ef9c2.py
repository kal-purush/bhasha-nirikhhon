#!/usr/bin/env python3
"""Tool to flattend and compare SPF records of a domain, highlighting missing/extra IP ranges and mechanisms."""

import os
import sys
from datetime import date

import logging
import argparse
import dns.resolver

def clean_txt_string(txt_string: str) -> str:
    """Clean TXT string by removing unnecessary characters considering the spf TXT format."""
    return txt_string.replace('" "', "").replace('"', "").replace("\n", "").replace("\r", "")


def dns_resolve_domain(domain: str, retry_count: int = 0) -> dns.resolver.Answer:
    """Resolve SPF TXT records for a domain and handles common DNS resolution errors."""
    try:
        records = dns.resolver.resolve(domain, "TXT")
        return records
    except dns.resolver.NXDOMAIN:
        logging.error("SPF records not found for %s", domain)
    except dns.resolver.Timeout:
        # Retrying for 3 times to resolve the SPF record when timeout error occurs
        if retry_count < 3:
            logging.warning("DNS resolution timed out for %s. Retrying...", domain)
            return dns_resolve_domain(domain, retry_count + 1)
        logging.error("DNS resolution timed out for %s. Exiting..", domain)
    except dns.resolver.NoNameservers:
        logging.error("No reachable DNS servers for %s. Exiting..", domain)
    except dns.resolver.DNSException as except_error:
        logging.error(
            "An unknown error occurred while resolving SPF for domain %s: %s. Exiting..", domain, except_error
        )
    sys.exit(1)


def is_valid_file_path(file_path: str) -> bool:
    """Check if the provided path is a valid file."""
    return os.path.isfile(file_path)


def process_domain_file(file_path: str) -> list[str]:
    """Validate and extract domains from the input file."""
    with open(file_path, "r", encoding="utf-8") as file:
        domains_to_check = [line.strip() for line in file.readlines()]
        logging.debug("Domains to be checked against: %s", domains_to_check)
        return domains_to_check


def retrieve_spf_records(domain: str) -> tuple[list[str], str]:
    """Retrieve the IP list and qualifier from the domain's spf record."""
    logging.debug("Processing domain: %s", domain)
    records = dns_resolve_domain(domain)
    flattened_ip_list = []
    for record in records:
        txt_record = record.to_text()
        if txt_record.startswith('"v=spf'):
            # Remove the unnecessary characters like " " from the txt_record for processing
            txt_string_formatted = clean_txt_string(txt_record)
            split_spf_record = txt_string_formatted.split()

            # Discard the prefix "v=spf1" from the record and get the IP list and qualifier
            spf_ip_list, all_qualifier = split_spf_record[1:-1], split_spf_record[-1]

            # Parse each mechanism in the spf record
            for mechanism in spf_ip_list:
                # Check if the mechanism is a special modifier like redirect, ptr etc.
                if "=" in mechanism:
                    logging.error("Record has special modifiers, please parse manually. Exiting..")
                    sys.exit(1)
                # Recursively process the include domains
                elif mechanism.startswith("include:"):
                    include_ip_list, _ = retrieve_spf_records(mechanism[8:])
                    flattened_ip_list.extend(include_ip_list)
                # Retrieve all IP4/IP6 addresses
                elif mechanism.startswith(("ip4", "ip6")):
                    flattened_ip_list.append(mechanism)
                # Retrieve other mechanisms (e.g., mx, exists, -ip4/-ip6) that differ from the previous cases
                # Need to be modified for adding/removing mechanisms
                else:
                    flattened_ip_list.append(mechanism)
    return flattened_ip_list, all_qualifier


def find_missing_and_extra_ips(
    flattened_ips: list[str], ips_to_be_checked: dict[str, list[str]]
) -> tuple[dict[str, list[str]], list[str]]:
    """Identify the missing IPs from the domain."""
    # Initialize dictionaries to store missing and extra IPs
    missing_ips = {}
    extra_ips = []

    # Identify missing IPs
    for domain, ip_list in ips_to_be_checked.items():
        if ip_list:
            # Find IPs in ip_list that are not in flattened_ips
            ips_not_in_flattened_ips = [ip for ip in ip_list if ip not in flattened_ips]
            if ips_not_in_flattened_ips:
                missing_ips.setdefault(domain, []).extend(ips_not_in_flattened_ips)

    # Identify extra IPs
    extra_ips = [ip for ip in flattened_ips if not any(ip in ip_values for ip_values in ips_to_be_checked.values())]

    # Return missing and extra IPs
    return missing_ips, extra_ips


def set_logging_config(debug_flag: bool) -> None:
    """Create the log file and sets the configuration needed for logging."""
    # Configure the logging module based on the debug_flag
    # If debug_flag is True, set logging level to DEBUG; otherwise, use INFO
    logging.basicConfig(
        level=logging.DEBUG if debug_flag else logging.INFO,
        format="[%(levelname)s] %(asctime)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            # Log to the console (stdout)
            logging.StreamHandler(sys.stdout),
        ],
    )


def main(domain: str, file_path: str) -> tuple[str, dict[str, list], list[str]]:
    """Retrieve and flatten domain's SPF record IPs, identify missing ones by comparing with the input file."""
    logging.info("Flattening SPF records for %s", domain)
    flattened_ips_current, all_qualifier = retrieve_spf_records(domain)

    # Initialize a dict for IPs to be checked
    flattened_ips_to_be_checked = {}

    # Iterate over domains in the input file to get the spf records and flatten the IP's
    logging.debug("Retrieving the domains and their spf records from the input file")
    for each_domain in process_domain_file(file_path):
        file_domain_ip_list, _ = retrieve_spf_records(each_domain)
        flattened_ips_to_be_checked[each_domain] = file_domain_ip_list
    logging.debug("Domains and their IPs to be checked against: %s", flattened_ips_to_be_checked)

    # Find missing and extra IPs
    missing_ips, extra_ips = find_missing_and_extra_ips(flattened_ips_current, flattened_ips_to_be_checked)

    output_flattened_spf_record = "v=spf1 " + " ".join(flattened_ips_current) + " " + all_qualifier
    logging.info("Flattened SPF record for %s : %s", domain, output_flattened_spf_record)

    # Log missing entries
    if not missing_ips:
        logging.info("No missing IP's found")
    else:
        logging.warning("Missing IP's: %s", missing_ips)

    # Log extra entries
    if not extra_ips:
        logging.info("No extra IP's found")
    else:
        # Print and return the list of extra IPs
        logging.warning("Extra IP's: %s", extra_ips)

    logging.info("Script execution completed.")

    return output_flattened_spf_record, missing_ips, extra_ips


if __name__ == "__main__":
    # Parse command-line arguments
    parser = argparse.ArgumentParser(
        description=(
            "Tool to compare SPF records of a domain with the domains in input file, highlighting missing and extra IPs"
        )
    )
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    parser.add_argument("--domain", help="Input domain")
    parser.add_argument("--file-path", help="Path to the file with domain list")

    args = parser.parse_args()

    # Configure logging based on debug flag
    set_logging_config(args.debug)
    logging.info("Script execution started")

    # Check if input arguments are not provided and default values are considered
    if args.file_path == None:
        logging.warning("Input file path not provided")
        sys.exit(1)
    if args.domain == None:
        logging.warning("Input domain is not provided")
        sys.exit(1)


    # Check if the input file is valid
    if not is_valid_file_path(args.file_path):
        logging.error("Invalid file path or file does not exist: %s. Exiting..", args.file_path)
        sys.exit(1)

    logging.info("Starting SPF parsing for the domain: %s", args.domain)

    # Execute the main function
    main(args.domain, args.file_path)