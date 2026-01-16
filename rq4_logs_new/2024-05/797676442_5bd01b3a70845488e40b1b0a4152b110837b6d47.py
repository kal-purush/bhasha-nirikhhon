import os
import subprocess
import ray
from langchain_community.llms import Ollama


def model_exists_in_ollama_list(model_name):
    # Execute the `ollama list` command and capture the output
    result = subprocess.run(
        ["ollama", "list"], capture_output=True, text=True, check=True)
    if result.returncode == 0:
        # Check if the model name is in the output
        return model_name in result.stdout
    else:
        print(f"Failed to list models with ollama. Error: {result.stderr}")
        return False


def download_and_pull_model_if_missing(model_name):
    if not model_exists_in_ollama_list(model_name):
        print(
            f"Model {model_name} not found in ollama list. Attempting to pull...")
        # Use the ollama pull command to download the model
        try:
            subprocess.run(["ollama", "pull", model_name], check=True)
            print(f"Model {model_name} pulled successfully.")
        except subprocess.CalledProcessError as e:
            print(f"Failed to pull model {model_name}. Error: {e}")
    else:
        print(f"Model {model_name} found in ollama list.")


def load_and_invoke_model(prompt_text):
    # Initialize Ray
    ray.init()

    model_name = "dolphin-mixtral:8x22b"

    # Check and download or pull the model if not found in ollama list
    download_and_pull_model_if_missing(model_name)

    # Use vLLM to load the model from the specified path
    try:
        llm = Ollama(model=model_name)
        output = llm.invoke(prompt_text)
        return output
    except Exception as e:
        print(f"Failed to invoke model. Error: {e}")
        return None


def main():
    # Prompt text for model inference
    prompt_text = input("Enter the text prompt for the model: ")

    # Load and invoke the model using Ray and vLLM
    result = load_and_invoke_model(prompt_text)
    if result:
        print("Model response:", result)
    else:
        print("Check model files and configuration.")


if __name__ == "__main__":
    main()