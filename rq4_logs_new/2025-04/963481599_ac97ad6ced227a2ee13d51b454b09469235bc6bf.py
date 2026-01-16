import esi
import openstack
import os

def get_openstack_connection(cloud=''):
    """
    Establishes an OpenStack connection using openstacksdk.
    Assumes clouds.yaml is in ~/.config/openstack/clouds.yaml
    """
    try:
        conn = openstack.connect(cloud=cloud)
        return conn
    except Exception as e:
        raise RuntimeError(f"Failed to connect to OpenStack cloud'{cloud}': {e}")

def get_esi_connection(cloud=''):
    """
    Establishes an ESI connection using esisdk.
    Assumes clouds.yaml is in ~/.config/openstack/clouds.yaml
    """
    try:
        conn = esi.connect(cloud=cloud)
        return conn
    except Exception as e:
        raise RuntimeError(f"Failed to connect to ESI cloud'{cloud}': {e}")