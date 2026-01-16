# ======== PHONEBOOK <HW11> =========
# Stores and displays contact's information.
# Creates records of contacts by their names with phone number, e-mail and address info.
# Once created record can't be edited, only removed.
# Contact names can't duplicate.


def show_stats(phonebook: dict):
    """
    Displays quantity of existing records in phonebook.
    Takes dict() as an argument, counts number of items in it,
    displays result and returns dict() back
    :param phonebook: dict()
    :return: dict()
    """
    if not phonebook:
        print("= PHONEBOOK IS EMPTY =")
    else:
        print(f"= You have {len(phonebook)} contacts =")
    return phonebook


def check_available(name: str, contacts):
    """
     Confirms if name is valid to be recorded or rejects it.
     Returns False if str() argument is empty, spaces only or is in second argument.
     True if none of above.
     """
    if not name or name.isspace() or name in contacts:
        print("= Record NOT created. Reason :", end=" ")
        if not name:
            print("Entered nothing =")
        elif name.isspace():
            print("Can't accept only spaces =")
        else:
            print("Contact already exists =")
        return False
    return True


def add_contact(phonebook: dict):
    """
    Creates new record in 'phonebook'

    Takes dict() as an argument and adds new item to it from input if dict key is valid to use.
    Returns updated dict() or dict() without changes if key not valid.
    :param phonebook: dict()
    :return: dict()
    """
    contact_data = {"Phone number": "",
                    "E-mail": "",
                    "Address": ""
                    }
    print("Enter contact name")
    name = input("Name: -> ")

    if check_available(name, phonebook.keys()):
        print(f"Enter < {name} > contact data")

        for info in contact_data.keys():
            contact_data[info] = input(f"{info}: > ")

        phonebook[name] = contact_data
        print(f"Contact < {name} > created.")

    return phonebook


def delete_contact(name: str, phonebook: dict):
    """
    Removes item 'name' from 'phonebook'

    Takes str() and dict() as an arguments.
    Removes item from dict() if key == str() and input confirmation.
    Returns updated or not updated dict().
    :param name: str() - key value to remove from dict()
    :param phonebook: dict()
    :return: dict() with or without changes
    """
    if name in phonebook.keys():
        if input(f"Type 'yes' if you agree to delete < {name} >: -> ").lower() == "yes":
            phonebook.pop(name)
            print(f"= Contact < {name} > deleted =")
    else:
        print(f"= Contact < {name} > doesn't exist =")
    return phonebook


def search(match: str, phonebook: dict):
    """
    Displays all contacts which have any matches with searched name

    Takes str() and dict() as arguments. Compares dict().keys() values withAdds dict().keys() to
    :param match:
    :param phonebook:
    :return:
    """
    found = []
    for name in phonebook.keys():
        if match.lower() in name.lower():
            found.append(name)
    if not found:
        print(f"< {match} > not found")
    else:
        print("Matches found in:")
        for finding in found:
            print(f"< {finding} >")
    return phonebook


def list_names(phonebook: dict):
    """
    Displays all recorded names in phonebook

    Prints keys of dict() argument if it's not empty.
    :param phonebook: dict()
    :return: dict()
    """
    if not phonebook.keys():
        print("= There is no contacts yet =")
    else:
        print("Recorded contacts:")
        for contact in phonebook.keys():
            print(f" < {contact} >")
    return phonebook


def show_name(name: str, phonebook: dict):
    """
    Displays selected contact full info

    Displays value of key name if it key is in phonebook
    :param name: str() - searched key
    :param phonebook: dict() - phonebook storage
    :return: dict()
    """
    if name not in phonebook.keys():
        print(f"= There is no contact < {name} > =")
    else:
        print(f"< {name} >")
        for info, data in phonebook[name].items():
            print(f" - {info}: {data}")
    return phonebook


def extract_name(entry: str):
    """
    Extracts name parameter from user's query

    Takes str() as an argument.
    Returns empty string or the argument's string without first and last characters
    if first and last characters of the argument's string are '<' and '>'
    :param entry: str()
    :return: str()
    """
    entry = entry.strip()
    if len(entry) >= 3 and entry[0] == "<" and entry[-1] == ">":
        name = entry[1:-1].strip()
        return name
    return ""


def extract(executable: str):
    """
    Extracts command from user's query

    Takes str() as an arguments, divides it into two strings by the first found space character.
    Returns dict() with two items.
    :param executable: str()
    :return: dict() = {"command": str(), "name": str()}
    """
    first_space = executable.find(" ")
    command = executable.strip()[:first_space]
    if first_space == -1:
        command = executable.strip()
    name = extract_name(executable[first_space:])
    return {"command": command, "name": name}


def show_help():
    """
    Displays list of commands with descriptions
    :return: None
    """
    help_info = {"=COMMAND=": "=DESCRIPTION=",
                 "add": "create new contact",
                 "delete <name>": "delete selected contact",
                 "exit": "finish program operation",
                 "help": "show description of all commands",
                 "list": "show all contacts names",
                 "search <name>": "show contacts with matches in name",
                 "show <name>": "show selected contact data",
                 "stats": "show total records quantity"
                 }

    for item, description in help_info.items():
        spaces = " " * (15 - len(item))
        separator = "  -  "
        if item == "=COMMAND=":
            separator = "     "
        print(f"{item+spaces}{separator}{description}")


def execute_command(command: str, phonebook: dict) -> dict:
    """
    Executes user's command

    Takes str() and dict() as arguments.
    Passes dict() and str() to other functions after str() verification by extract()
    :param command: str() - expected string with keywords
    :param phonebook: dict() - dict()
    :return: dict() - modified or unmodified 'phonebook'
    """
    executable = extract(command)

    match executable["command"].lower():
        case "stats":
            return show_stats(phonebook)

        case "add":
            return add_contact(phonebook)

        case "delete":
            if executable["name"]:
                return delete_contact(executable["name"], phonebook)
            print("= Invalid syntax or no name entry =")

        case "list":
            return list_names(phonebook)

        case "show":
            if executable["name"]:
                return show_name(executable["name"], phonebook)
            print("= Invalid syntax or no name entry =")

        case "search":
            if executable["name"]:
                return search(executable["name"], phonebook)
            print("= Invalid syntax or no name entry =")

        case "help":
            show_help()

        case _:
            print(f"= Invalid command \'{executable['command']}\'. Try 'help' for info =")

    return phonebook


def main(phonebook={}):
    """
    Starts program and receives user's commands

    Cycles through the input to receive and pass commands for execution.
    Dict() is used for phonebook data storage.
    :param phonebook: dict() - main storage
    :return: None
    """
    print("       ========= PHONEBOOK  V.0.1.1 =========")
    print("              = Welcome to PHONEBOOK =")
    print("       You can always execute 'help' for info")
    command = ""
    while command.lower() != "exit":
        print("--------------------------------------------------------")
        command = input("@-> ")
        if not command or command.lower() == "exit":
            continue
        else:
            print("--------------------------------------------------------")
            phonebook = execute_command(command, phonebook)


if __name__ == "__main__":
    main()