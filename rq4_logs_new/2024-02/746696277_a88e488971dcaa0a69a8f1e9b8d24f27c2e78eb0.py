class Colors:
    RESET = "\033[0m"
    RED = "\033[31m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    BLUE = "\033[34m"
    MAGENTA = "\033[35m"
    CYAN = "\033[36m"
    WHITE = "\033[37m"

# Function to print colored text
def print_colored_text(text, color):
    print(f"{color}{text}{Colors.RESET}")

def print_log(*args, **kwargs):
    args_string = ' '.join(map(str, args))
    print(f"{Colors.GREEN}{args_string}{Colors.RESET}")

def print_error(*args, **kwargs):
    args_string = ' '.join(map(str, args))
    print(f"{Colors.RED}{args_string}{Colors.RESET}")    

def print_warning(*args, **kwargs):
    args_string = ' '.join(map(str, args))
    print(f"{Colors.YELLOW}{args_string}{Colors.RESET}") 