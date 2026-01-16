import os

class DirectoryInspector:
    @staticmethod
    def list_directory_contents(path):
        try:
            contents = os.listdir(path)
            print(f"Contents of {path}:")
            for item in contents:
                print(item)
        except FileNotFoundError:
            print("The specified path does not exist.")
        except PermissionError:
            print("Permission denied to access the directory.")

class FileSizeReporter:
    @staticmethod
    def report_file_sizes(directory):
        try:
            for filename in os.listdir(directory):
                filepath = os.path.join(directory, filename)
                if os.path.isfile(filepath):
                    print(f"{filename}: {os.path.getsize(filepath)} bytes")
        except FileNotFoundError:
            print("The specified directory does not exist.")
        except PermissionError:
            print("Permission denied to access the directory.")

class FileExtensionCounter:
    @staticmethod
    def count_file_extensions(directory):
        extension_count = {}
        try:
            for filename in os.listdir(directory):
                if os.path.isfile(os.path.join(directory, filename)):
                    _, ext = os.path.splitext(filename)
                    ext = ext.lower()
                    if ext in extension_count:
                        extension_count[ext] += 1
                    else:
                        extension_count[ext] = 1
            for ext, count in extension_count.items():
                print(f"{ext.upper()}: {count}")
        except FileNotFoundError:
            print("The specified directory does not exist.")
        except PermissionError:
            print("Permission denied to access the directory.")

# Example usage:
# Replace 'your_directory_path' with an actual directory path before running
directory_path = 'D:\Desktop\Think-Chicago-Skill-Builders-Software-Engineering-Module3\Lesson-5-python-file-handling'

print("Directory Inspector:")
DirectoryInspector.list_directory_contents(directory_path)

print("\nFile Size Reporter:")
FileSizeReporter.report_file_sizes(directory_path)

print("\nFile Extension Counter:")
FileExtensionCounter.count_file_extensions(directory_path)