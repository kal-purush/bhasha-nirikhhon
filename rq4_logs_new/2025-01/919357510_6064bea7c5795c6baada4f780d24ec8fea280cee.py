import os
import re

# Path to the target Python file
file_path = os.path.expanduser("~/.platformio/platforms/espressif32@3.5.0/builder/main.py")

if not os.path.isfile(file_path):
    file_path = os.path.expanduser("~/.platformio/platforms/espressif32/builder/main.py")
    if not os.path.isfile(file_path):
        print("Error: Could not find the main.py file.")
        exit(1)

# New code to replace the `elif upload_protocol == "esptool":` block
new_code = """
    def install_esptool():
        import subprocess
        from shutil import which
        try:
            if not which("esptool"):
                print("Installing esptool via pip...")
                subprocess.check_call([env.subst("$PYTHONEXE"), "-m", "pip", "install", "--upgrade", "esptool"])
        except Exception as e:
            sys.stderr.write(f"Failed to install esptool: {e}")
            env.Exit(1)

    # Ensure esptool is installed
    install_esptool()

    # Update UPLOADER to use the installed esptool
    env.Replace(
        UPLOADER="esptool",
        UPLOADERFLAGS=[
            "--chip", mcu,
            "--port", '"$UPLOAD_PORT"',
            "--baud", "$UPLOAD_SPEED",
            "--before", "default_reset",
            "--after", "hard_reset",
            "write_flash", "-z",
            "--flash_mode", "${__get_board_flash_mode(__env__)}",
            "--flash_freq", "${__get_board_f_flash(__env__)}",
            "--flash_size", "detect"
        ],
        UPLOADCMD='"$PYTHONEXE" -m esptool $UPLOADERFLAGS $ESP32_APP_OFFSET $SOURCE'
    )

    for image in env.get("FLASH_EXTRA_IMAGES", []):
        env.Append(UPLOADERFLAGS=[image[0], env.subst(image[1])])

    if "uploadfs" in COMMAND_LINE_TARGETS:
        env.Replace(
            UPLOADERFLAGS=[
                "--chip", mcu,
                "--port", '"$UPLOAD_PORT"',
                "--baud", "$UPLOAD_SPEED",
                "--before", "default_reset",
                "--after", "hard_reset",
                "write_flash", "-z",
                "--flash_mode", "$BOARD_FLASH_MODE",
                "--flash_size", "detect",
                "$SPIFFS_START"
            ],
            UPLOADCMD='"$PYTHONEXE" -m esptool $UPLOADERFLAGS $SOURCE',
        )

    upload_actions = [
        env.VerboseAction(env.AutodetectUploadPort, "Looking for upload port..."),
        env.VerboseAction("$UPLOADCMD", "Uploading $SOURCE")
    ]
"""

# Regex pattern to find the `elif upload_protocol == "esptool":` block
pattern = r"elif upload_protocol\s*==\s*[\"']esptool[\"']:\s*(?:\n[ \t]+.*)+"

# Read the content of the file
with open(file_path, "r") as file:
    content = file.read()

# Replace the block with the new code
updated_content = re.sub(pattern, f"elif upload_protocol == \"esptool\":\n{new_code}", content, flags=re.MULTILINE)

# Write the updated content back to the file
with open(file_path, "w") as file:
    file.write(updated_content)

print(f"Updated {file_path} successfully.")