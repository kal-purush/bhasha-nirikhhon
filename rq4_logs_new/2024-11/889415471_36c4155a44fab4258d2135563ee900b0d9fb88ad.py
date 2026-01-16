import socket
import ssl
from datetime import datetime
import subprocess
import requests # type: ignore


def check_ssl_tls_configuration(host, port=443):
    try:
        print(f"Connecting to {host}:{port}...")
        context = ssl.create_default_context()
        with socket.create_connection((host, port)) as sock:
            with context.wrap_socket(sock, server_hostname=host) as ssock:
                cert = ssock.getpeercert()
                print(f"Certificate details: {cert}")  # Debugging certificate content

                protocol = ssock.version()
                print(f"Protocol used: {protocol}")  # Debugging protocol

                cipher = ssock.cipher()
                print(f"Cipher details: {cipher}")  # Debugging cipher

                # Safely handle cipher details
                cipher_name = cipher[0] if len(cipher) > 0 else "Unknown"
                protocol_used = cipher[1] if len(cipher) > 1 else "Unknown"
                key_size = cipher[2] if len(cipher) > 2 else "Unknown"

                # Extract certificate details with fallback
                not_before = cert.get('notBefore', 'Unavailable')
                not_after = cert.get('notAfter', 'Unavailable')
                san = cert.get('subjectAltName', [])
                issuer = safe_extract_dict(cert.get('issuer', []), 'Issuer')
                issued_to = safe_extract_dict(cert.get('subject', []), 'Subject')

                # Debug extracted fields
                print(f"Issuer: {issuer}")
                print(f"Issued To: {issued_to}")
                print(f"Valid From: {not_before}")
                print(f"Valid To: {not_after}")

                # Convert dates
                current_time = datetime.utcnow()
                cert_status = evaluate_certificate_status(not_before, not_after, current_time)

                # Vulnerabilities analysis
                vulnerabilities = evaluate_vulnerabilities(protocol, cipher_name, key_size)

                # Check for HSTS support
                hsts_support = check_hsts_support(host)

                # Check for supported protocols and ciphers
                protocols_supported = check_supported_protocols(host, port)
                ciphers_supported = check_supported_ciphers(host, port)

                # Certificate chain validation
                chain_validation = validate_certificate_chain(host, port)

                # Generate a grade
                grade = calculate_grade(cert_status, vulnerabilities, chain_validation)

                # Report
                print(f"\n--- SSL/TLS Configuration Report for {host} ---")
                print(f"Protocol: {protocol}")
                print(f"Cipher: {cipher_name} (Protocol Used: {protocol_used}, Key Size: {key_size})")
                print(f"Issuer: {issuer}")
                print(f"Issued To: {issued_to}")
                print(f"Valid From: {not_before if not_before != 'Unavailable' else 'N/A'}")
                print(f"Valid To: {not_after if not_after != 'Unavailable' else 'N/A'}")
                print(f"Certificate Status: {cert_status}")
                print(f"HSTS Support: {'Yes' if hsts_support else 'No'}")
                print(f"Certificate Chain Validation: {'Valid' if chain_validation else 'Invalid'}")
                print(f"Supported Protocols: {', '.join(protocols_supported)}")
                print(f"Supported Ciphers: {', '.join(ciphers_supported)}")
                print(f"Grade: {grade}")
                if vulnerabilities:
                    print(f"Vulnerabilities Detected: {', '.join(vulnerabilities)}")
                else:
                    print("No common vulnerabilities detected.")

    except ssl.SSLError as e:
        print(f"SSL error: {e}")
    except Exception as e:
        print(f"Error: {e}")


def safe_extract_dict(cert_field, field_name):
    """Safely extract dictionary-like information from a certificate field."""
    try:
        return dict(x[0] for x in cert_field)
    except Exception as e:
        print(f"Error extracting {field_name}: {e}")
        return {}


def evaluate_certificate_status(not_before, not_after, current_time):
    """Evaluate the certificate status based on dates."""
    try:
        if not_before != 'Unavailable' and not_after != 'Unavailable':
            not_before_dt = datetime.strptime(not_before, '%b %d %H:%M:%S %Y %Z')
            not_after_dt = datetime.strptime(not_after, '%b %d %H:%M:%S %Y %Z')
            if current_time > not_after_dt:
                return "Expired"
            elif current_time < not_before_dt:
                return "Not yet valid"
            else:
                return "Valid"
        else:
            return "Certificate dates unavailable"
    except Exception as e:
        print(f"Error evaluating certificate status: {e}")
        return "Unknown"


def evaluate_vulnerabilities(protocol, cipher_name, key_size):
    """Evaluate vulnerabilities based on protocol and cipher details."""
    vulnerabilities = []
    try:
        if protocol in ["SSLv2", "SSLv3", "TLSv1", "TLSv1.1"]:
            vulnerabilities.append(f"Weak protocol detected: {protocol}")
        if any(weak in cipher_name for weak in ["RC4", "MD5", "DES", "3DES"]):
            vulnerabilities.append(f"Weak cipher detected: {cipher_name}")
        if isinstance(key_size, int) and key_size < 2048:
            vulnerabilities.append(f"Small key size: {key_size} bits")
    except Exception as e:
        print(f"Error evaluating vulnerabilities: {e}")
    return vulnerabilities


def check_hsts_support(host):
    try:
        response = requests.get(f"https://{host}", timeout=5)
        return "strict-transport-security" in response.headers
    except Exception:
        return False


def check_supported_protocols(host, port):
    protocols = []
    for protocol in ["ssl2", "ssl3", "tls1", "tls1_1", "tls1_2", "tls1_3"]:
        cmd = f"openssl s_client -connect {host}:{port} -{protocol} </dev/null 2>/dev/null | grep 'Protocol'"
        try:
            output = subprocess.check_output(cmd, shell=True).decode()
            if protocol in output:
                protocols.append(protocol.upper().replace("_", "."))
        except subprocess.CalledProcessError:
            pass
    return protocols


def check_supported_ciphers(host, port):
    ciphers = []
    try:
        output = subprocess.check_output(f"openssl s_client -connect {host}:{port} -cipher ALL </dev/null", shell=True).decode()
        for line in output.splitlines():
            if "Cipher is" in line:
                cipher = line.split(":", 1)[1].strip()
                ciphers.append(cipher)
    except subprocess.CalledProcessError:
        pass
    return ciphers


def validate_certificate_chain(host, port):
    cmd = f"openssl s_client -connect {host}:{port} -showcerts </dev/null 2>/dev/null | openssl verify"
    try:
        output = subprocess.check_output(cmd, shell=True).decode()
        return "OK" in output
    except subprocess.CalledProcessError:
        return False


def calculate_grade(cert_status, vulnerabilities, chain_validation):
    if cert_status != "Valid" or not chain_validation:
        return "F"
    if vulnerabilities:
        return "C" if len(vulnerabilities) > 2 else "B"
    return "A"


# Example usage
host = input("Enter the hostname (e.g., example.com): ")
check_ssl_tls_configuration(host)