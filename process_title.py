import pandas as pd
import os
from lingua import Language, LanguageDetectorBuilder
from concurrent.futures import ThreadPoolExecutor

# Define the path to the data folder
data_folder = 'data'
output_folder = 'output'
detector = LanguageDetectorBuilder.from_all_languages().build()

# Create the output folder if it doesn't exist
os.makedirs(output_folder, exist_ok=True)

# List all files in the data folder
files = [f for f in os.listdir(data_folder) if f.endswith('.csv')]

# Function to process a single file
def process_file(file):
    file_path = os.path.join(data_folder, file)
    df = pd.read_csv(file_path)
    print(f"Finished reading file: {file}")
    titles = df['issue_title'].tolist()
    
    # Initialize a list to store rows with non-English titles
    non_english_rows = []

    # Detect the language of the issue title
    for index, title in enumerate(titles):
        result = detector.detect_multiple_languages_of(title)
        for DetectionResult in result:
            print(DetectionResult.language.name)
            if DetectionResult.language.name != 'ENGLISH':
                non_english_rows.append(df.iloc[index])
    
    # Create a DataFrame from the non-English rows and save it to a new CSV file
    if non_english_rows:
        non_english_df = pd.DataFrame(non_english_rows)
        non_english_df.to_csv(os.path.join(output_folder, f'non_english_{file}'), index=False)

# Process files in parallel
with ThreadPoolExecutor(max_workers=10) as executor:
    executor.map(process_file, files[:10])
