import os
import shutil

def get_download_files():
    """Obtains files in Downloads folder and stores them in a list.

    Returns:
        List[string]: List of files in the Downloads folder.
    """
    downloads_path = os.path.join(os.path.expanduser("~"), "Downloads")
    return os.listdir(downloads_path)

def filter_desktop_files():
    """Filters desktop files into three categories: shortcuts, directories, and other files.

    Returns:
        dict: A dictionary with keys 'shortcuts', 'directories', and 'files'.
    """
    desktop_path = os.path.join(os.path.expanduser("~"), "Desktop")
    desktop_files = {
        "shortcuts": [],
        "directories": [],
        "files": []
    }

    for filename in os.listdir(desktop_path):
        filepath = os.path.join(desktop_path, filename)
        name, extension = os.path.splitext(filename)

        if extension in [".url", ".lnk"]:
            desktop_files["shortcuts"].append(filename)
        elif os.path.isdir(filepath):
            desktop_files["directories"].append(filename)
        else:
            desktop_files["files"].append(filename)

    return desktop_files

def organize_desktop_files():
    """Organizes desktop files into respective folders."""
    try:
        desktop_files = filter_desktop_files()
        desktop_path = os.path.join(os.path.expanduser("~"), "Desktop")

        # Define subdirectories
        subdirs = {
            "shortcuts": os.path.join(desktop_path, "Shortcuts"),
            "directories": os.path.join(desktop_path, "Directories"),
            "files": os.path.join(desktop_path, "Files")
        }

        # Create or clear subdirectories
        for subdir in subdirs.values():
            if os.path.exists(subdir):
                shutil.rmtree(subdir)
            os.mkdir(subdir)

        # Move files to respective folders
        for shortcut in desktop_files["shortcuts"]:
            shutil.move(os.path.join(desktop_path, shortcut), subdirs["shortcuts"])

        for directory in desktop_files["directories"]:
            shutil.move(os.path.join(desktop_path, directory), subdirs["directories"])

        for file in desktop_files["files"]:
            shutil.move(os.path.join(desktop_path, file), subdirs["files"])

        print("Desktop files organized successfully.")

    except Exception as e:
        print(f"An error occurred: {e}")

def clear_downloads():
    """Clears all files and directories in the Downloads folder."""
    downloads_path = os.path.join(os.path.expanduser("~"), "Downloads")

    for item in os.listdir(downloads_path):
        item_path = os.path.join(downloads_path, item)
        try:
            if os.path.isdir(item_path):
                shutil.rmtree(item_path)
                print(f"Directory '{item}' was successfully deleted.")
            else:
                os.remove(item_path)
                print(f"File '{item}' was successfully deleted.")
        except Exception as e:
            print(f"Failed to delete '{item}': {e}")

if __name__ == "__main__":
    while True:
        print("\nHow can I help? (type 'e' to exit Neatipy)")
        print("1. Organize Desktop Files and Folders")
        print("2. Display Downloads Folder")
        print("3. Clear Download Files")

        choice = input("Enter your choice: ").strip()
        if choice == "e":
            break

        if choice == "1":
            warning = input("Warning: This is a destructive action. Proceed? (y/n): ").strip().lower()
            if warning == "y":
                organize_desktop_files()
            elif warning == "n":
                print("Operation cancelled.")
            else:
                print("Invalid input.")

        elif choice == "2":
            download_files = get_download_files()
            if download_files:
                print("Files and directories in Downloads:")
                for file in download_files:
                    print(f"- {file}")
            else:
                print("The Downloads folder is empty.")

        elif choice == "3":
            clear_downloads()

        else:
            print("Invalid choice. Please try again.")