import os
import re
import requests
from bs4 import BeautifulSoup
from collections import defaultdict
from stop_words import STOP_WORDS

# Class representing a single node in the Trie
class IndexNode:
    def __init__(self):
        self.children = {}  # Dictionary to store child nodes
        self.files = defaultdict(int)  # Dictionary to store document frequencies (filename -> frequency)

# Class representing the Trie data structure
class Index:
    def __init__(self):
        self.root = IndexNode()  # Initialize the root node
    
    # Method to insert a word into the Index
    def add_word(self, word, filename):
        node = self.root
        for char in word:
            # Traverse or create child nodes for each character in the word
            if char not in node.children:
                node.children[char] = IndexNode()
            node = node.children[char]
        # Increment the frequency of the word in the given document
        node.files[filename] += 1

    # Method to search for a word in the Index
    def find_word(self, word):
        node = self.root
        for char in word:
            # Traverse the Index for each character in the word
            if char not in node.children:
                return {}  # Return empty if the word is not found
            node = node.children[char]
        # Return the dictionary of documents containing the word
        return dict(node.files)

# Function to clean and preprocess text
def preprocess_text(text):
    # Remove non-alphanumeric characters
    text = re.sub(r'[^a-zA-Z0-9\s]', '', text)
    # Convert text to lowercase and split into words
    words = text.lower().split()
    # Remove stop words
    return [word for word in words if word not in STOP_WORDS]

# Function to parse HTML content from a file or URL
def extract_text(source):
    if source.startswith("http"):
        # If the input is a URL, fetch the content using requests
        response = requests.get(source)
        soup = BeautifulSoup(response.content, "html.parser")
    else:
        # If the input is a file, read and parse its content
        with open(source, "r", encoding="utf-8") as f:
            soup = BeautifulSoup(f, "html.parser")
    # Extract and return the text content from the HTML
    return soup.get_text()

# Function to build the search index using an Index
def create_index(source_file):
    index = Index()  # Initialize the Index
    with open(source_file, "r") as f:
        # Read the list of sources (URLs or file paths) from the input file
        sources = f.read().splitlines()
    
    for src in sources:
        try:
            # Parse and clean the text from each source
            text = extract_text(src)
            words = preprocess_text(text)
            # Insert each word into the Index with the source as the document
            for word in words:
                index.add_word(word, src)
        except Exception as e:
            # Handle errors during processing
            print(f"Error processing {src}: {e}")
    return index

# Function to run the search engine
def search_engine():    
    source_file = "input.txt"  # File containing the list of sources
    result_file = "output.txt"  # File to write the search results
    index = create_index(source_file)  # Build the search index
    
    with open(result_file, "w") as out:
        # Prompt the user for search queries
        queries = input("Enter your search queries separated by commas: ").split(",")
        for query in queries:
            # Search for each query in the Index
            results = index.find_word(query)
            # Rank the results by frequency in descending order
            ranked = sorted(results.items(), key=lambda x: x[1], reverse=True)
            # Write the search results to the output file
            out.write(f"Search results for '{query}':\n")
            for doc, freq in ranked:
                out.write(f"  {doc} (frequency: {freq})\n")
            out.write("\n")

# Entry point of the program
if __name__ == "__main__":
    search_engine()