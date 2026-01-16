"""a few functions used both by the workers and master"""

class Bcolor():
    """enumerating some conts"""
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

def print_warning(message):
    """prints an error message"""
    print(Bcolor.WARNING + str(message) + Bcolor.ENDC)

def print_error(message):
    """prints an error message"""
    print(Bcolor.FAIL + str(message) + Bcolor.ENDC)

def print_success(message):
    """prints a success message"""
    print(Bcolor.OKGREEN + str(message) + Bcolor.ENDC)

def print_info(message):
    """prints a success message"""
    print(Bcolor.OKBLUE + str(message) + Bcolor.ENDC)


def new_request_dict(action, source, field1, field2, data):
    """Return a dict in the wanted format"""
    return_dict = {}
    return_dict["header"] = "True"
    return_dict["action"] = action #[ "RESIZE" | "SHUTDOWN" | "PING"]
    return_dict["source"] = source #[ $URL | 'DATA' | $KEY ]
    return_dict["field1"] = field1 #[ '' | $TARGET_WIDTH]
    return_dict["field2"] = field2 #[ '' | $TARGET_HEIGHT]
    return_dict["data"] = data   #[ '' | $PING_DATA |$IMAGE]
    return return_dict