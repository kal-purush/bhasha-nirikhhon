import requests
import json
import os
import sys
import base64
import re
from typing import List, Dict, Any, Optional, Generator


class DashscopeClient:
    def __init__(self, api_key=None, base_url="https://dashscope.aliyuncs.com/compatible-mode/v1"):
        """Initialize the Dashscope client"""
        self.api_key = api_key or os.getenv("DASHSCOPE_API_KEY")
        if not self.api_key:
            raise ValueError("API key is required. Set it directly or via DASHSCOPE_API_KEY environment variable.")
        self.base_url = base_url

    def chat_completions_create(self, 
                               model: str,
                               messages: List[Dict[str, Any]], 
                               modalities: List[str] = ["text"],
                               stream: bool = True,
                               stream_options: Dict[str, Any] = {"include_usage": True}) -> Generator:
        """Create a chat completion with streaming support"""
        url = f"{self.base_url}/chat/completions"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_key}"
        }
        
        data = {
            "model": model,
            "messages": messages,
            "modalities": modalities,
            "stream": stream,
            "stream_options": stream_options
        }
        
        response = requests.post(url, headers=headers, json=data, stream=stream)
        
        if response.status_code != 200:
            raise Exception(f"Request failed with status code: {response.status_code}\n{response.text}")
        
        if stream:
            return self._process_stream(response)
        else:
            return response.json()
    
    def _process_stream(self, response):
        """Process streaming response"""
        for line in response.iter_lines():
            if line:
                if line.strip() == b'data: [DONE]':
                    break
                
                if line.startswith(b'data: '):
                    json_str = line[6:].decode('utf-8')
                    try:
                        chunk = json.loads(json_str)
                        yield chunk
                    except json.JSONDecodeError as e:
                        print(f"Error decoding JSON: {e}")


def encode_image_to_base64(image_path: str) -> Optional[str]:
    """Convert image to base64 encoding"""
    if image_path.startswith(('http://', 'https://')):
        # Return the URL directly for remote images
        return image_path
    
    try:
        # Normalize path (convert backslashes to forward slashes)
        normalized_path = os.path.normpath(image_path)
        
        if not os.path.exists(normalized_path):
            print(f"Warning: Image file not found: {normalized_path}")
            return None
        
        with open(normalized_path, "rb") as image_file:
            return f"data:image/jpeg;base64,{base64.b64encode(image_file.read()).decode('utf-8')}"
    except Exception as e:
        print(f"Error encoding image: {e}")
        return None


def process_user_input(text: str) -> List[Dict[str, Any]]:
    """Process user input text to handle images and text"""
    content = []
    # Use regex to find image markdown syntax
    image_pattern = r"!\[\]\((.+?)\)"
    
    # Find all image references
    image_matches = re.finditer(image_pattern, text)
    
    # Keep track of the last position processed
    last_end = 0
    for match in image_matches:
        # Add text before this image if there is any
        if match.start() > last_end:
            text_before = text[last_end:match.start()].strip()
            if text_before:
                content.append({"type": "text", "text": text_before})
        
        # Process the image
        image_path = match.group(1).strip()
        image_url = encode_image_to_base64(image_path)
        if image_url:
            # Image successfully processed
            content.append({
                "type": "image_url",
                "image_url": {"url": image_url}
            })
        else:
            # If image processing failed, add a note about it
            content.append({
                "type": "text", 
                "text": f"[Image processing failed for: {image_path}]"
            })
        
        last_end = match.end()
    
    # Add any remaining text after the last image
    if last_end < len(text):
        remaining_text = text[last_end:].strip()
        if remaining_text:
            content.append({"type": "text", "text": remaining_text})
    
    # If there were no images found, just return the text
    if not content:
        content.append({"type": "text", "text": text})
    
    return content


def main():
    # Initialize the client
    client = DashscopeClient()
    
    # Start with a system message
    messages = [
        {
            "role": "system",
            "content": [{"type": "text", "text": "You are a helpful assistant."}]
        }
    ]
    
    print("Type your message and end with '</end>' on a new line.")
    print("To include images, use the syntax: ![](path_or_url_to_image)")
    print("-" * 50)
    
    while True:
        print("\nUser: (end with '</end>' on a new line)")
        # Read multi-line input until </end> is encountered
        user_input_lines = []
        while True:
            try:
                line = sys.stdin.readline().rstrip("\n")
                if line == "</end>":
                    break
                user_input_lines.append(line)
            except KeyboardInterrupt:
                print("\nExiting program...")
                return
        
        user_input = "\n".join(user_input_lines)
        if not user_input.strip():
            continue
        
        # Process input to create content with text and images
        user_content = process_user_input(user_input)
        
        # Add to messages
        messages.append({
            "role": "user",
            "content": user_content
        })
        
        print("\nAssistant: ")
        
        try:
            # Call the API with streaming response
            full_response = ""
            completion = client.chat_completions_create(
                model="qwen-omni-turbo",
                messages=messages,
                modalities=["text"],
                stream=True,
                stream_options={"include_usage": True}
            )
            
            for chunk in completion:
                if "choices" in chunk and chunk["choices"]:
                    delta = chunk["choices"][0]["delta"]
                    if "content" in delta and delta["content"] is not None:  # Check for None
                        content = delta["content"]
                        print(content, end="", flush=True)
                        full_response += content
                elif "usage" in chunk:
                    print("\n\nUsage:", chunk["usage"])
            
            # Add the assistant's response to the conversation history
            if full_response:  # Only add if we got a response
                messages.append({
                    "role": "assistant",
                    "content": [{"type": "text", "text": full_response}]
                })
            
        except Exception as e:
            print(f"\nError: {str(e)}")
    

if __name__ == "__main__":
    main()