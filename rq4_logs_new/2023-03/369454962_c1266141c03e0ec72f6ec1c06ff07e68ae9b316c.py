import redis
import os
import datetime

from evolved5g.sdk import LocationSubscriber, QosAwareness, ConnectionMonitor, TSNManager

# Get environment variables
REDIS_HOST = os.getenv('REDIS_HOST')
REDIS_PORT = os.environ.get('REDIS_PORT')
tsn_host = os.getenv('TSN_IP')
tsn_port = os.environ.get('TSN_PORT')
capif_host = os.getenv('CAPIF_HOSTNAME')
capif_https_port = os.environ.get('CAPIF_PORT_HTTPS')
folder_path_for_certificates_and_capif_api_key = os.environ.get('PATH_TO_CERTS')

netapp_ids_tokens = (
    {}
)  # Stores the clearance token of each profile application to a NetApp
netapp_name = "MyNetapp1"  # The name of our NetApp

tsn = TSNManager(  # Initialization of the TNSManager
        folder_path_for_certificates_and_capif_api_key=folder_path_for_certificates_and_capif_api_key,
        capif_host=capif_host,
        capif_https_port=capif_https_port,
        https=False,
        tsn_host=tsn_host,
        tsn_port=tsn_port
    )


def get_profiles():

    profiles = tsn.get_tsn_profiles()
    print(f"Found {len(profiles)} profiles")
    for profile in profiles:
        profile_configuration = profile.get_configuration_for_tsn_profile()

        print(
            f"Profile {profile.name} with configuration parameters {profile_configuration.get_profile_configuration_parameters()}")


def apply_tsn_profile():
    """
    Demonstrates how to apply a TSN profile configuration to a NetApp
    """
    profiles = tsn.get_tsn_profiles()
    # For demonstration purposes,  let's select the last profile to apply,
    profile_to_apply = profiles[-1]
    profile_configuration = profile_to_apply.get_configuration_for_tsn_profile()
    # Let's create an TSN identifier for this Net App.
    # This tsn_netapp_identifier can be used in two scenarios
    # a) When you want to apply a profile configuration for your net app
    # b) When you want to clear a profile configuration for your net app
    tsn_netapp_identifier = tsn.TSNNetappIdentifier(netapp_name=netapp_name)

    print(
        f"Generated TSN traffic identifier for Netapp: {tsn_netapp_identifier.value}"
    )
    print(
        f"Apply {profile_to_apply.name} with configuration parameters"
        f"{profile_configuration.get_profile_configuration_parameters()} to NetApp {netapp_name} "
    )
    clearance_token = tsn.apply_tsn_profile_to_netapp(
        profile=profile_to_apply, tsn_netapp_identifier=tsn_netapp_identifier
    )
    print(
        f"The profile configuration has been applied to the netapp. The returned token {clearance_token} can be used "
        f"to reset the configuration"
    )

    return tsn_netapp_identifier,clearance_token


def clear_profile_configuration(tsn_netapp_identifier: tsn.TSNNetappIdentifier, clearance_token:str):
    """
    Demonstrates how to clear a previously applied TSN profile configuration from a NetApp
    """
    tsn.clear_profile_for_tsn_netapp_identifier(tsn_netapp_identifier,clearance_token)
    print(f"Cleared TSN configuration from {netapp_name}")


if __name__ == '__main__':

    r = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        decode_responses=True,
    )

    tsn_netapp_id = None
    clear_token = None
    profile_conf = None

    try:
        ans = input("Do you want to test TSN API get_profiles? (Y/n) ")
        if ans == "Y" or ans == 'y':
            get_profiles()
    except Exception as e:
        status_code = e.args[1]
        print(e)

    try:
        ans = input("Do you want to apply TSN profile to netapp? (Y/n) ")
        if ans == "Y" or ans == 'y':
            # When we apply a profile we get back an identifier and a clearance token.
            (tsn_netapp_id, clear_token) = apply_tsn_profile()
    except Exception as e:
        status_code = e.args[1]
        print(e)

    print(tsn_netapp_id)
    print(clear_token)

    try:
        ans = input("Do you want to clear TSN profile? (Y/n) ")
        if ans == "Y" or ans == 'y':
            # These can be used to clear the existing configuration
            clear_profile_configuration(tsn_netapp_id, clear_token)
    except Exception as e:
        status_code = e.args[1]
        print(e)