import subprocess
import os
import random
import shutil
import string

def validateCode(user_code, validate_code, lang):
    if (lang == "python"):
        return validate_python(user_code, validate_code)
    elif (lang == "javascript"):
        return validate_javascript(user_code, validate_code)


def validate_python(user_code, validate_code):

    if (not os.path.isdir('run')):
         os.mkdir("run")

    fileID = generateRandomId()
    os.mkdir(f"run/{fileID}")

    with open(f'run/{fileID}/solution.py', 'w') as file:
        file.write(user_code)
    with open(f'run/{fileID}/test.py', 'w') as file:
        file.write(validate_code)

    subprocess.run(f"docker create --name {fileID} python-validator:latest", stdout = subprocess.DEVNULL)
    subprocess.run(f"docker cp run/{fileID}/. {fileID}:/workspace", stdout = subprocess.DEVNULL)
    subprocess.run(f"docker start {fileID}", stdout = subprocess.DEVNULL)
    result = subprocess.run(f"docker logs {fileID}", stdout = subprocess.PIPE)
    subprocess.run(f"docker rm -f {fileID}", stdout = subprocess.DEVNULL)

    shutil.rmtree(f"run/{fileID}")
    return result.stdout.decode()

def validate_javascript(user_code, validate_code):

    if (not os.path.isdir('run')):
         os.mkdir("run")

    fileID = generateRandomId()
    os.mkdir(f"run/{fileID}")

    data = ""
    with open(f'test.js', 'r') as file:
        data = file.read()

    with open(f'run/{fileID}/test.js', 'w') as file:
        file.write(data)
        file.write(user_code + "\n")
        file.write(validate_code)


    subprocess.run(f"docker create --name {fileID} javascript-validator:latest", stdout = subprocess.DEVNULL)
    subprocess.run(f"docker cp run/{fileID}/. {fileID}:/workspace", stdout = subprocess.DEVNULL)
    subprocess.run(f"docker start {fileID}", stdout = subprocess.DEVNULL)
    result = subprocess.run(f"docker logs {fileID}", stdout = subprocess.PIPE)
    subprocess.run(f"docker rm -f {fileID}", stdout = subprocess.DEVNULL)

    shutil.rmtree(f"run/{fileID}")
    return result.stdout.decode()


def generateRandomId(length=6):
    characters = string.ascii_lowercase + string.digits
    random_id = ''.join(random.choice(characters) for _ in range(length))
    return random_id