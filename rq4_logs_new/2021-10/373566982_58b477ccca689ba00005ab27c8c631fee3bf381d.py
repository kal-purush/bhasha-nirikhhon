"""
    End to End Tests Support Functions

    Class designed to contain all the functions needed to run
    all E2E tests pertaining to the Amlight-SDx napp
"""

import sys
import requests
from napps.amlight.sdx.settings import kytos_topology_url, new_headers


def submit_get_req_to_amlight_sdx_topology_api(schema_url):
    """ Connects to Amlight-SDX topology API, looks for 200 status code,
     and returns sdx topology in json format"""

    try:
        response = requests.get(schema_url + "topology", headers=new_headers)
    except Exception as err:
        print("Error connecting to Amlight-SDX API")
        print(err)
        sys.exit(1)

    if response.status_code != 200:
        raise Exception("Return code is not 201, response = %s" % response.content)

    print("Success")
    return response.json()


def submit_get_req_to_retrieve_oxp_endpoint(endpoint_name, schema_url):
    """ Submits request to SDX API to retrieve and validate the oxp_name &
    oxp_url"""

    try:
        endpoint = requests.get(schema_url + endpoint_name, headers=new_headers).json()
    except Exception as err:
        print("Error connecting to Amlight-SDX API")
        print(err)
        sys.exit(1)

    return endpoint


def submit_get_req_to_retrieve_kytos_topology(api_url):
    """ Submits a GET request to Kytos APi to retrieve the current Kytos
    topology"""

    try:
        topo = requests.get(api_url, headers=new_headers).json()
    except Exception as err:
        print("Error connecting to Kytos API to retrieve topology")
        print(err)
        sys.exit(1)

    return topo


def submit_post_req_to_enable_kytos_link(link_id):
    """ Submits a POST request to Kytos API to enable a specific link"""

    enable_api_url = kytos_topology_url + "/links/" + link_id + "/enable"
    try:
        response = requests.post(enable_api_url, headers=new_headers)
    except Exception as err:
        print("Error connecting to Kytos topology API to enable link")
        print(err)
        sys.exit(1)

    if response.status_code != 201:
        raise Exception("Return code is not 200, response = %s" % response.content)


def submit_post_req_to_disable_kytos_link(link_id):
    """ Submits a POST request to Kytos API to disable a specific link"""

    disable_api_url = kytos_topology_url + "/links/" + link_id + "/disable"
    try:
        requests.post(disable_api_url, headers=new_headers)
        content = requests.get(kytos_topology_url, headers=new_headers).json()
    except Exception as err:
        print("Error connecting to Kytos topology API to disable link")
        print(err)
        sys.exit(1)

    return content


def validate_sdx_nodes_number_and_content(sdx_topo, expected_nodes):
    """ Receives the sdx_topology and number of expected nodes to be found
    in either the amlight, sax, or tenet topologies and validates that the
    nodes list is NOT empty and contains the expected amount of nodes"""\

    if len(sdx_topo['nodes']) == 0: return Exception("List of nodes is EMPTY "
                                                     "after Napp initialization")

    if isinstance(sdx_topo, dict):
        if len(sdx_topo['nodes']) == expected_nodes:
            print("Success")
    else:
        raise Exception("The returned Amlight-SDX topology is not a dictionary")