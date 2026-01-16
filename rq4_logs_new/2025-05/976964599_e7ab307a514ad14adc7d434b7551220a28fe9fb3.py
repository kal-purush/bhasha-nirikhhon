import requests
import os
import json

# --- Configuration ---
# URL of your running FastAPI application's endpoint
API_URL = "http://127.0.0.1:8000/format/"

# --- !!! IMPORTANT !!! ---
# Create these files in the same directory as this script,
# or update the paths to point to your actual test files.

# Path to a sample input file (e.g., a dummy text or bib file)
INPUT_FILE_PATH = "testcases/transformers.bib"
# Path to a sample schema file (must be a valid JSON)
SCHEMA_FILE_PATH = "testcases/citations.json"



# --- Main Test Function ---
def test_format_endpoint():
    """Sends a request to the /format/ endpoint and prints the response."""


    print(f"Attempting to POST to: {API_URL}")
    print(f"Using input file: {INPUT_FILE_PATH}")
    print(f"Using schema file: {SCHEMA_FILE_PATH}")

    try:
        # Open the files in binary read mode ('rb')
        with open(INPUT_FILE_PATH, 'rb') as input_f, \
             open(SCHEMA_FILE_PATH, 'rb') as schema_f:

            # Prepare the files dictionary for the multipart/form-data request
            # The keys ('input_file', 'schema_file') MUST match the parameter
            # names in your FastAPI endpoint function (`create_format_job`).
            files_to_upload = {
                'input_file': (os.path.basename(INPUT_FILE_PATH), input_f, 'text/plain'), # Adjust mime type if needed
                'schema_file': (os.path.basename(SCHEMA_FILE_PATH), schema_f, 'application/json')
            }

            # Send the POST request
            response = requests.post(API_URL, files=files_to_upload)

            # Check the response status code
            print(f"\n--- Response ---")
            print(f"Status Code: {response.status_code}")

            # Try to print the JSON response body
            try:
                response_json = response.json()
                print("Response JSON:")
                # Pretty print the JSON
                print(json.dumps(response_json, indent=2))
            except json.JSONDecodeError:
                print("Response Body (Not JSON):")
                print(response.text)

    except FileNotFoundError:
        print(f"\nError: Make sure '{INPUT_FILE_PATH}' and '{SCHEMA_FILE_PATH}' exist.")
    except requests.exceptions.ConnectionError as e:
        print(f"\nConnection Error: Could not connect to the server at {API_URL}.")
        print("Please ensure your FastAPI server is running.")
        print(f"Details: {e}")
    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")

# --- Run the test ---
if __name__ == "__main__":
    test_format_endpoint()