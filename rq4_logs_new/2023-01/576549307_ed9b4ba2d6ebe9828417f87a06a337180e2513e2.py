import os

entries = os.listdir(os.getcwd())

for entry in entries:
    if entry != "replace.py" and ".a" not in entry:
        file = open(entry, "r")
        print(f"opening file: {entry}\n")
        content = file.read()
        file.close()

        if "holberton" in content:
            content = content.replace("holberton", "main")
            file = open(entry, "w")
            file.write(content)
            file.close()

            print(f"Written to {entry} successfully")

        elif "HOLBERTON" in content:
            content = content.replace("HOLBERTON", "MAIN")
            file = open(entry, "w")
            file.write(content)
            file.close()

            print(f"Written to {entry} successfully")