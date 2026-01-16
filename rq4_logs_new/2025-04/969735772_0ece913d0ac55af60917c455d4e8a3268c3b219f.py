from flask import Flask, render_template, request, redirect, url_for, session, jsonify, Response
import os
import json
import re
import socket
import whois
import requests
import base64
from datetime import datetime

app = Flask(__name__)
app.secret_key = 'supersecretkey'

USER_DB = 'password/users.json'
ADMIN_DB = 'password/adminpswd.json'

# Ensure password folder and files exist
os.makedirs('password', exist_ok=True)
if not os.path.exists(USER_DB):
    with open(USER_DB, 'w') as f:
        json.dump({}, f)
if not os.path.exists(ADMIN_DB):
    with open(ADMIN_DB, 'w') as f:
        json.dump({"admin": "admin"}, f)

# Load API keys from config.json
with open("config.json") as f:
    api_keys = json.load(f)

def extract_domain(url):
    match = re.search(r"https?://([^/]+)", url)
    return match.group(1) if match else url

def get_domain_age(domain):
    try:
        w = whois.whois(domain)
        creation_date = w.creation_date
        if isinstance(creation_date, list):
            creation_date = creation_date[0]
        return (datetime.now() - creation_date).days
    except:
        return -1

def check_security_headers(url):
    try:
        r = requests.get(url, timeout=10)
        headers = r.headers
        header_list = [
            "Content-Security-Policy",
            "Strict-Transport-Security",
            "X-Content-Type-Options",
            "X-Frame-Options",
            "Referrer-Policy",
            "Permissions-Policy",
            "X-XSS-Protection"
        ]
        return {header: header in headers for header in header_list}
    except:
        return {header: False for header in header_list}

def abuseipdb_scan(ip):
    try:
        url = f"https://api.abuseipdb.com/api/v2/check?ipAddress={ip}&maxAgeInDays=90"
        headers = {
            "Key": api_keys['abuseipdb_api_key'],
            "Accept": "application/json"
        }
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()
        return data.get("data", {})
    except requests.exceptions.RequestException as e:
        return {"error": f"AbuseIPDB request failed: {str(e)}"}
    except ValueError:
        return {"error": "Invalid JSON response from AbuseIPDB"}

def shodan_scan(ip):
    try:
        url = f"https://api.shodan.io/shodan/host/{ip}?key={api_keys['shodan_api_key']}"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        return {"error": f"Shodan request failed: {str(e)}"}
    except ValueError:
        return {"error": "Invalid JSON response from Shodan"}

def virustotal_scan(url):
    try:
        headers = {"x-apikey": api_keys['virustotal_api_key']}
        encoded_url = base64.urlsafe_b64encode(url.encode()).decode().strip("=")
        report_url = f"https://www.virustotal.com/api/v3/urls/{encoded_url}"
        response = requests.get(report_url, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()
        return data["data"]["attributes"]["last_analysis_stats"]
    except requests.exceptions.RequestException as e:
        return {"error": f"VirusTotal request failed: {str(e)}"}
    except ValueError:
        return {"error": "Invalid JSON response from VirusTotal"}

@app.route('/')
def home():
    if 'user' in session:
        return render_template('index.html', username=session['user'])
    return redirect(url_for('login'))

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']

        with open(ADMIN_DB) as f:
            if json.load(f).get(username) == password:
                session['admin'] = username
                return redirect(url_for('admin_panel'))

        with open(USER_DB) as f:
            user_data = json.load(f)
            if username in user_data and user_data[username]['password'] == password:
                session['user'] = username
                return redirect(url_for('home'))

        return "Invalid credentials. <a href='/login'>Try again</a>"

    return render_template('login.html')

@app.route('/signup', methods=['GET', 'POST'])
def signup():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        email = request.form['email']

        with open(USER_DB) as f:
            users = json.load(f)

        if username in users:
            return "User already exists."

        users[username] = {'email': email, 'password': password}

        with open(USER_DB, 'w') as f:
            json.dump(users, f)

        return redirect(url_for('login'))

    return render_template('signup.html')

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))

@app.route('/admin_panel', methods=['GET', 'POST'])
def admin_panel():
    if 'admin' not in session:
        return redirect(url_for('login'))

    with open(USER_DB) as f:
        users = json.load(f)

    if request.method == 'POST':
        username = request.form['username']
        email = request.form['email']
        password = request.form['password']
        users[username] = {'email': email, 'password': password}

        with open(USER_DB, 'w') as f:
            json.dump(users, f)

    return render_template('admin_panel.html', users=users)

@app.route('/scan')
def scan():
    if 'user' not in session and 'admin' not in session:
        return redirect(url_for('login'))

    url = request.args.get('url')
    if not url:
        return jsonify({"error": "No URL provided"}), 400

    domain = extract_domain(url)
    try:
        ip = socket.gethostbyname(domain)
    except:
        ip = "Unavailable"

    age = get_domain_age(domain)
    headers = check_security_headers(url)
    heuristics = {
        "IP in URL": bool(re.search(r"\d+\.\d+\.\d+\.\d+", domain)),
        "Domain Length > 30": len(domain) > 30,
        "Special Characters (@, -, _)": any(c in domain for c in ['@', '-', '_'])
    }

    abuse_data = abuseipdb_scan(ip) if ip != "Unavailable" else {"error": "IP unavailable"}
    shodan_data = shodan_scan(ip) if ip != "Unavailable" else {"error": "IP unavailable"}
    vt_stats = virustotal_scan(url)

    return jsonify({
        "domain": domain,
        "ip": ip,
        "age": age,
        "headers": headers,
        "heuristics": heuristics,
        "abuseipdb": abuse_data,
        "shodan": shodan_data,
        "virustotal": vt_stats
    })

if __name__ == '__main__':
    app.run(debug=True)