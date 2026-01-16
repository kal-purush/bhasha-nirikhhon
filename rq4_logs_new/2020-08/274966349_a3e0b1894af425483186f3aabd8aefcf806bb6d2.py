import argparse
import base64
import requests
import urllib3


def xforce(ibm_url, ibm_key, ibm_pass, args):
    token = ibm_key + ":" + ibm_pass
    token = token.encode('utf-8')
    token = base64.urlsafe_b64encode(token)
    token = str(token).strip("b'")
    # Build header
    headers = {'Authorization': "Basic " + token, 'Accept': 'application/json'}
    urllib3.disable_warnings()
    # Perform get request
    xforce_value = args.k
    url = requests.get(ibm_url + xforce_value, params='', headers=headers, verify=False)
    with url as result:
        # Check web response
        if result.status_code == 200:
            # Assign JSON response to data variable
            data = result.json()
            score = data.get('result', {}).get('score', {})
            if score:
                if score >= 7:
                    ibm_result = "High Risk"
                elif 5 < score < 7:
                    ibm_result = "Moderate"
                else:
                    ibm_result = "Low"
            else:
                ibm_result = "Unknown"
            str(ibm_result)
            return ibm_result
        else:
            print(str(result.status_code) + " Error")

if __name__ == '__main__':
    # Arg Parse
    parser = argparse.ArgumentParser()
    group_creation = parser.add_argument_group('Arguments')
    group_creation.add_argument('-c', '--config', help='Config File', nargs='?', const='./conf/config.conf', dest="c")
    group_creation.add_argument('-k', '--keyword', help='Keyword search if list is not used', nargs='?', dest="k")
    args = parser.parse_args()
    xforce()