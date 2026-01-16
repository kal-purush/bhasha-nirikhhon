from llm_integration import query_gpt

command_verbs = [
    "log in", "sign in", "hook up", "power up", "turn on", "boot up", "start up", "set up", "pull down", "pull down", "click on", "scroll up", "scroll down", "run out of", "back up", "print out", "hack into", "go down", "wipe out", "pop up", "popup", "plug in", "sign up", "key in", "opt in", "opt out", "filter out", "turn off", "shut down", "power down", "go online", "go offline", "make", "do", "write", "take", "close", "open", "create", "add", "delete"
]

def validate_data(data: dict):
    if "language" in data:
        if data["language"] != "en":
            return "I am having trouble understanding your language"
    if "text" in data:
        if data["text"] in command_verbs:
            print("")


    gpt_reply = query_gpt(data["text"])
    return gpt_reply


