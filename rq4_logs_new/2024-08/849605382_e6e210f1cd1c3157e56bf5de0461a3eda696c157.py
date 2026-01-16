import os
from pathlib import Path
from typing import Iterator

def get_file_content(file_path: Path) -> str:
    """Read and return the content of a file."""
    try:
        with file_path.open('r', encoding='utf-8') as file:
            return file.read()
    except UnicodeDecodeError:
        print(f"Warning: Unable to read {file_path} with UTF-8 encoding. Skipping.")
        return ""

def find_target_files(root_dir: Path) -> Iterator[Path]:
    """Generate paths of target files in the given directory and its subdirectories."""
    target_extensions = {'.jsx', '.js', '.html'}
    for path in root_dir.rglob('*'):
        if path.is_file() and path.suffix in target_extensions:
            yield path

def process_directory(root_dir: Path, output_file: Path) -> None:
    """Process all target files in the given directory and write their content to the output file."""
    with output_file.open('w', encoding='utf-8') as out_file:
        for file_path in find_target_files(root_dir):
            relative_path = file_path.relative_to(root_dir)
            content = get_file_content(file_path)
            if content:
                out_file.write(f"File: {relative_path}\n")
                out_file.write("=" * 50 + "\n")
                out_file.write(content)
                out_file.write("\n\n" + "=" * 50 + "\n\n")

def main() -> None:
    script_dir = Path(__file__).parent
    target_folder_name = input("Enter the name of the target folder: ").strip()
    root_directory = script_dir / target_folder_name
    output_file = script_dir / "output.txt"

    if not root_directory.is_dir():
        print(f"Error: {root_directory} is not a valid directory.")
        return

    try:
        process_directory(root_directory, output_file)
        print(f"Processing complete. Results saved to {output_file}")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()