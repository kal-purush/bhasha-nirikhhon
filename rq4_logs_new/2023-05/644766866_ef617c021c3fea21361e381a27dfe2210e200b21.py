import datetime
import platform

# Get the current date and time
current_datetime = datetime.datetime.now()

# Get the OS name and version
os_name = platform.system()
os_version = platform.version()

# Format the information
output = f"Current date & time: {current_datetime}\n"
output += f"OS name: {os_name}\n"
output += f"OS version: {os_version}\n"

# Save the information to a text file
file_name = "system_info.txt"
with open(file_name, "w") as file:
    file.write(output)

# Print a confirmation message
print(f"System information has been saved to {file_name}")