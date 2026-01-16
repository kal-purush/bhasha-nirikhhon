#example python test case

import ta5k_gen

#would be out in a library of show commands with related functions
def get_interface_port_auth_status(session, interface):
    #perform command
    response = session.perform_command('show port-auth interface efm-port ' + interface)
    #analyze response to return port auth status - could do this in main if desired
    response = response.split('\r\n')
    port_auth_status = 0
    for line in response:
        if "Port Auth Status" in line:
            line = line.split(':')
            port_auth_status = line[1]
    if port_auth_status == 0:
        print 'port auth error'
    return port_auth_status
        

#initialize variables
ip = '10.213.253.40'
interface = '1/17/1'

#open sessions
new_session = ta5k_gen.loginCli()
new_session.login(ip)

#provision/verify something
interface_port_auth_status = get_interface_port_auth_status(new_session, interface)
print 'interface ' + interface + ' port status: ' + interface_port_auth_status

#perform some other commands if needed
output = new_session.perform_command('show system inventory')
print output


