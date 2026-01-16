import os
import random
import shutil
import argparse

def rename_text_files(directory_path):
    """
    Randomly rename all text files in a directory.
    
    Args:
        directory_path: Path to the directory containing text files to rename
        prefix: Prefix to use for the new filenames
    """
    # Ensure the directory exists
    if not os.path.exists(directory_path):
        print(f"Directory {directory_path} does not exist.")
        return

    # Get all text files in the directory (excluding subdirectories)
    text_files = [f for f in os.listdir(directory_path) 
                 if os.path.isfile(os.path.join(directory_path, f)) 
                 and f.lower().endswith('.txt')]
    
    if not text_files:
        print(f"No text files found in {directory_path}")
        return
        
    # Create a temporary directory for the renaming process
    temp_dir = os.path.join(directory_path, "temp_rename")
    os.makedirs(temp_dir, exist_ok=True)
    
    # Create a mapping for original files to new names
    mapping = {}
    shuffled_indices = list(range(len(text_files)))
    random.shuffle(shuffled_indices)
    
    # Determine how many digits we need for the numbering
    padding_length = len(str(len(text_files)))-1
    
    for i, idx in enumerate(shuffled_indices):
        original_file = text_files[idx]
        # Generate new filename with padded numbers
        padded_num = str(i).zfill(padding_length)
        new_filename = f"{padded_num}.txt"
        mapping[original_file] = new_filename
    
    # Phase 1: Move files to temporary directory with new names
    for orig_name, new_name in mapping.items():
        orig_path = os.path.join(directory_path, orig_name)
        temp_path = os.path.join(temp_dir, new_name)
        
        shutil.move(orig_path, temp_path)
    
    # Phase 2: Move files back from temporary directory
    for _, new_name in mapping.items():
        temp_path = os.path.join(temp_dir, new_name)
        final_path = os.path.join(directory_path, new_name)
        
        shutil.move(temp_path, final_path)
    
    # Remove temporary directory
    os.rmdir(temp_dir)
    
    print(f"Successfully renamed {len(text_files)} text files in {directory_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Randomly shuffle file names in a directory")
    parser.add_argument("directory", help="Directory containing text files to rename")
    
    args = parser.parse_args()
    
    rename_text_files(
        args.directory,
    )