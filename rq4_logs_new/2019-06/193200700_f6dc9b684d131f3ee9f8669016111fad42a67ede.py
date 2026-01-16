import paramiko

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

ssh.connect("192.168.100.70", username="robot", password="maker")
ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command("pybricks-micropython hff_lego/main.py")

ssh2 = paramiko.SSHClient()
ssh2.set_missing_host_key_policy(paramiko.AutoAddPolicy())

ssh2.connect("192.168.100.15", username="robot", password="maker")
ssh2_stdin, ssh2_stdout, ssh2_stderr = ssh2.exec_command("pybricks-micropython hff_lego/main.py")

command = input("> ")

if (command == "stop"):
    ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command("pkill -f main.py")
    ssh_stdin, ssh2_stdout, ssh2_stderr = ssh2.exec_command("pkill -f main.py")