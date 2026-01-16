import os
import yaml

def load_yaml_config(yaml_path):
    """Load YAML configuration specifying allowed and ignored names and extensions."""
    with open(yaml_path, 'r') as file:
        return yaml.safe_load(file)

def is_file_allowed(file_name, allowed_names, allowed_extensions, ignored_names, ignored_extensions):
    """Check if a file is allowed based on its name or extension, while excluding ignored ones."""
    file_extension = os.path.splitext(file_name)[1]
    
    # Exclude ignored files
    if file_name in ignored_names or file_extension in ignored_extensions:
        return False
    
    # Include allowed files
    return file_name in allowed_names or file_extension in allowed_extensions

def is_folder_allowed(folder_name, allowed_names, ignored_names):
    """Check if a folder is allowed based on its name, while excluding ignored ones."""
    if folder_name in ignored_names:
        return False
    return folder_name in allowed_names or not allowed_names  # Allow if not explicitly ignored

def collect_project_files(project_path, config):
    """Collect and filter project files and folders based on the YAML configuration."""
    allowed_names = config.get('allowed_names', [])
    allowed_extensions = config.get('allowed_extensions', [])
    ignored_names = config.get('ignored_names', [])
    ignored_extensions = config.get('ignored_extensions', [])
    
    collected_files = []
    for root, dirs, files in os.walk(project_path):
        # Filter directories
        dirs[:] = [d for d in dirs if is_folder_allowed(d, allowed_names, ignored_names)]
        
        # Filter files
        for file in files:
            if is_file_allowed(file, allowed_names, allowed_extensions, ignored_names, ignored_extensions):
                collected_files.append(os.path.join(root, file))
    return collected_files

def write_files_to_text(collected_files, output_file):
    """Write the paths and contents of collected files to a text file."""
    with open(output_file, 'w', encoding='utf-8') as output:
        for file_path in collected_files:
            try:
                with open(file_path, 'r', encoding='utf-8') as file:
                    content = file.read()
                output.write(f"Path: {file_path}\n")
                output.write("Content:\n")
                output.write(content + "\n")
                output.write("=" * 80 + "\n")  # Separator
            except Exception as e:
                print(f"Error reading file {file_path}: {e}")

if __name__ == "__main__":
    # Specify paths
    yaml_config_path = "config.yaml"  # Path to the YAML configuration file
    project_directory = "."          # Directory of the project to scan
    output_file = "project_files.txt"  # Output text file
    
    # Load YAML configuration
    config = load_yaml_config(yaml_config_path)
    
    # Collect files based on the configuration
    files_to_include = collect_project_files(project_directory, config)
    
    # Write collected files and their contents to a text file
    write_files_to_text(files_to_include, output_file)
    print(f"Collected files written to {output_file}.")