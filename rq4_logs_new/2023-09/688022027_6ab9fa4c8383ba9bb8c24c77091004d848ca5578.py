import subprocess

POWERSHELL_SSH_CMD = ['ssh', 'gs-3922@192.168.56.101', '-p', '22']
SUB_COMMAND = "exit"


def validate(func):
    return_code = func()
    if return_code == 0:
        print("Output generated on the file 'resource.txt'")
    else:
        print("Error: Could not generate the output file")


@validate
def ssh_connection():
    process = subprocess.Popen(POWERSHELL_SSH_CMD,
                               stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)

    process.stdin.write(SUB_COMMAND.encode())
    process.stdin.flush()

    output, error = process.communicate()
    output = output.decode()

    with open("./output.txt", mode="w") as cmd_output:
        cmd_output.write(output)

    return_code = process.wait()
    return return_code

