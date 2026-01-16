IP = '172.16.6.10'
USER = 'admin'
Password = 'admin'
PORT = '22'

#---------------- Establishing to the device -----------------------# 

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy)

try:
    ssh.connect(IP, username=USER, password=Password, port=PORT)
    connection = ssh.invoke_shell()

    connection.send('\nenvironment no more\n')
    time.sleep(.5)
    print(connection.recv(65000).decode())

    connection.send('\nshow log log-id 99 | match "SFF unsupported type" pre-lines 1\n')
    time.sleep(.5)
    sr_7_2_output = connection.recv(65000).decode()
    time.sleep(3)
    print('\n\n')
    print(str(sr_7_2_output) + '\n')
    with open('18122021.txt', 'w') as f:
        f.write(sr_7_2_output)

except paramiko.AuthenticationException:
    print('Try different info please')

with open("18122021.txt", "r") as f:
    lines = f.readlines()

indices = []

for i in range(len(lines)):
    if lines[i][0].isdigit():
        indices.append(i)

lines3 = []
lines4 = []

k = 0
j = 0
while True:
    v = 0
    while indices[j]+v < indices[k+1]:
        lines4.append(lines[indices[j]+v])
        v = v + 1
        if indices[j]+v == indices[k+1]:
            lines3.append(lines4)
            lines4 = []
    j = j + 1
    k = k + 1
    
    if k + 1 == len(indices):
        break

new_log = []
new_log2 = []

for line16 in lines3:
    new_log = line16[0] + line16[2]
    new_log2.append(new_log)

pattern = "Port [0-9]*.[0-9].[0-9]*"
port_numbers = []

for log in new_log2:
    port_number = re.findall(pattern, log)
    port_numbers.append(port_number)