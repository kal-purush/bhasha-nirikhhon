import os
import google.generativeai as genai
from dotenv import load_dotenv

# Debug information
print("Current directory:", os.getcwd())
print("Files in directory:", os.listdir())

# Load environment variables
load_dotenv()

# Configure the Gemini API
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')
print(f"API Key found: {'Yes' if GEMINI_API_KEY else 'No'}")
if GEMINI_API_KEY:
    print(f"API Key length: {len(GEMINI_API_KEY)}")
    print(f"API Key starts with: {GEMINI_API_KEY[:6]}...")

if not GEMINI_API_KEY:
    raise ValueError("Please set GEMINI_API_KEY in your .env file")

try:
    genai.configure(api_key=GEMINI_API_KEY)
    # List available models
    print("\nAvailable models:")
    for m in genai.list_models():
        if "generateContent" in m.supported_generation_methods:


            print(f"- {m.name}")
    
    # Initialize the model
    model = genai.GenerativeModel('gemini-1.5-pro-001')
    print("\nModel initialized successfully!")
except Exception as e:
    print(f"\nError during setup: {str(e)}")
    exit(1)

def chat_with_gemini(prompt):
    """
    Send a prompt to Gemini and get the response
    """
    try:
        response = model.generate_content(prompt)
        return response.text
    except Exception as e:
        return f"An error occurred: {str(e)}"

def main():
    print("\nWelcome to Gemini Chat!")
    print("Type 'quit' to exit")
    
    while True:
        user_input = input("\nYou: ")
        if user_input.lower() == 'quit':
            break
            
        response = chat_with_gemini(user_input)
        print("\nGemini:", response)

if __name__ == "__main__":
    main()