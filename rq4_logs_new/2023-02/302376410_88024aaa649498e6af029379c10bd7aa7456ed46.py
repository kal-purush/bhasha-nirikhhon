import asyncio
from netmiko import ConnectHandler

async def connect_to_cisco_devices(devices_list):
    """
    Connects to multiple Cisco devices simultaneously, using Netmiko and asyncio.

    Parameters:
        devices_list (list): A list of dictionaries, each representing a device, with the following keys:
            - 'ip': the IP address of the device
            - 'username': the username to use for authentication
            - 'password': the password to use for authentication
            - 'device_type': the device type (e.g. 'cisco_ios')

    Returns:
        A list of tuples, where each tuple contains the device IP address and the output of the "show version" command.
    """
    results = []
    tasks = []
    for device in devices_list:
        task = asyncio.create_task(run_command_on_device(device))
        tasks.append(task)
    results = await asyncio.gather(*tasks)
    return results

async def run_command_on_device(device):
    """
    Connects to a single Cisco device using Netmiko, runs the "show version" command, and returns the output.

    Parameters:
        device (dict): a dictionary containing the following keys:
            - 'ip': the IP address of the device
            - 'username': the username to use for authentication
            - 'password': the password to use for authentication
            - 'device_type': the device type (e.g. 'cisco_ios')

    Returns:
        A tuple containing the device IP address and the output of the "show version" command.
    """
    connection = ConnectHandler(
        ip=device['ip'],
        username=device['username'],
        password=device['password'],
        device_type=device['device_type']
    )
    output = connection.send_command("show version")
    return (device['ip'], output)

async def main():
    devices_list = [
        {'ip': '192.168.1.1', 'username': 'cisco', 'password': 'cisco', 'device_type': 'cisco_ios'},
        {'ip': '192.168.1.2', 'username': 'cisco', 'password': 'cisco', 'device_type': 'cisco_ios'},
        {'ip': '192.168.1.3', 'username': 'cisco', 'password': 'cisco', 'device_type': 'cisco_ios'},
    ]
    results = await connect_to_cisco_devices(devices_list)
    for device_ip, output in results:
        with open(f"{device_ip}.txt", "w") as f:
            f.write(output)

asyncio.run(main())