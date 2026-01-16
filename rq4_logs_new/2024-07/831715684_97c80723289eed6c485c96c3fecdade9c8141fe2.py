import requests

def test_firewall_rules():
    test_cases = [
        {'src_ip': '192.168.1.1', 'dst_ip': '10.0.0.1', 'src_port': 8080, 'dst_port': 80, 'protocol': 'TCP', 'payload': 'test'},
        {'src_ip': '192.168.1.2', 'dst_ip': '10.0.0.1', 'src_port': 8080, 'dst_port': 80, 'protocol': 'TCP', 'payload': 'malicious'}
    ]
    
    for case in test_cases:
        response = requests.post('http://localhost:5000/process_packet', json=case)
        result = response.json().get('result', '')
        
        # Determine if the penetration is successful or failed based on the result
        if "ALLOWED" in result and case['payload'] == 'malicious':
            print(f"Failed Penetration Test: {case} - Expected denial but got: {result}")
        elif "DENIED" in result and case['payload'] == 'malicious':
            print(f"Successful Penetration Test: {case} - Correctly denied")
        else:
            print(f"Test Case Result: {case} - {result}")

if __name__ == "__main__":
    test_firewall_rules()