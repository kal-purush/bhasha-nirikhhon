import subprocess

def ping(servers):
    #command you want to execute
    cmd = 'ping'

    #send one packet of data to the host
    #this is specified by '-c 1' in the argument list
    output_list = []
    #iterate over all the servers in the list and ping each server
    for server in servers:
        temp = subprocess.Popen([cmd, '-c 1', server], stdout=subprocess.PIPE)
        #get the output as a string
        output = str(temp.communicate())
        #store the output in the list
        output_list.append(output)

    return output_list

if __name__ == '__main__':
    #get list of servers from the text file
    servers = list(open('servers.txt'))
    #iterate over all the servers that we read from the text file
    #and remove all extra lines. This is just a preprocessing step
    #to make sure there aren't any unncesesary lines
    for i in range(len(servers)):
        servers[i] = servers[i].strip('\n')
    output_list = ping(servers)
    print(output_list)