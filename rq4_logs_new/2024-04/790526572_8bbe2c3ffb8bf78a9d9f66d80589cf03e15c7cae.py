import requests
from bs4 import BeautifulSoup

# Function to check for SQL Injection vulnerability
def check_sql_injection(url):
    """
    Check for SQL Injection vulnerability by injecting a malicious SQL payload.
    
    Args:
    - url (str): The URL of the vulnerable page.
    
    Returns:
    None
    """
    payload = "' OR '1'='1"  # SQL injection payload
    response = requests.get(url + payload)
    
    if "SQL syntax error" in response.text:
        print(f"[+] SQL Injection Vulnerability detected at {url}")
    else:
        print(f"[-] No SQL Injection Vulnerability detected at {url}")

# Function to check for XSS vulnerability
def check_xss(url):
    """
    Check for XSS vulnerability by injecting a malicious script payload.
    
    Args:
    - url (str): The URL of the vulnerable page.
    
    Returns:
    None
    """
    payload = "<script>alert('XSS')</script>"
    response = requests.get(url + payload)
    
    soup = BeautifulSoup(response.text, 'html.parser')
    if soup.find('script', string="alert('XSS')"):
        print(f"[+] XSS Vulnerability detected at {url}")
    else:
        print(f"[-] No XSS Vulnerability detected at {url}")

# Function to check for CSRF vulnerability
def check_csrf(url, csrf_token_name):
    """
    Check for CSRF vulnerability by exploiting the lack of CSRF protection on a login form.
    
    Args:
    - url (str): The URL of the vulnerable login page.
    - csrf_token_name (str): The name of the CSRF token input field.
    
    Returns:
    None
    """
    session = requests.Session()
    
    # Retrieve CSRF token from the form
    response = session.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    csrf_token = soup.find('input', {'name': csrf_token_name})['value']
    
    # Craft a POST request with CSRF exploit
    payload = {
        'username': 'attacker',
        'password': 'password',
        csrf_token_name: csrf_token
    }
    
    response = session.post(url, data=payload)
    
    if "Login successful" in response.text:  # Assuming "Login successful" is present in successful login response
        print(f"[+] CSRF Vulnerability detected at {url}")
    else:
        print(f"[-] No CSRF Vulnerability detected at {url}")

if __name__ == "__main__":
    # Replace with the actual URLs and CSRF token name
    target_url = "http://example.com/vulnerable_page?id="
    csrf_url = "http://example.com/login"
    csrf_token_name = "csrf_token"  # Replace with the actual CSRF token name
    
    # Check for vulnerabilities
    check_sql_injection(target_url)
    check_xss(target_url)
    check_csrf(csrf_url, csrf_token_name)