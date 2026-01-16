def app_print(app_name, msg):
    OKBLUE = '\033[94m'
    ENDC = '\033[0m'
    print(OKBLUE + msg + ENDC)


def dev_print(dev_name, msg):
    OKGREEN = '\033[92m'
    ENDC = '\033[0m'
    print(OKGREEN + msg + ENDC)