from langchain_google_genai import ChatGoogleGenerativeAI
from apify_client import ApifyClient
import json

class LangChainApifyAssistant:
    """
    LangChain LLM + Apify Actor integration with image-aware responses.
    
    This class demonstrates how to combine LangChain's language models with 
    Apify's web scraping capabilities to create an AI assistant that can:
    1. Analyze queries to determine if visual content is needed
    2. Search for real images from the web using Apify Actor
    3. Generate text responses that incorporate found image data
    
    Attributes:
        llm (ChatGoogleGenerativeAI): LangChain Gemini model instance
        apify_client (ApifyClient): Apify client for running actors
    """
    
    def __init__(self, gemini_key: str, apify_token: str):
        """
        Initialize the LangChain + Apify integration.
        
        Args:
            gemini_key (str): Google API key for Gemini model access
            apify_token (str): Apify API token for actor execution
            
        Raises:
            Exception: If API keys are invalid or services are unavailable
        """
        # Initialize LangChain LLM
        self.llm = ChatGoogleGenerativeAI(
            model="gemini-2.0-flash",
            google_api_key=gemini_key
        )
        
        # Initialize Apify client
        self.apify_client = ApifyClient(apify_token)
        print("✅ LangChain + Apify integration ready!")
    
    def should_search_images(self, query: str) -> bool:
        """
        Use LangChain LLM to intelligently decide if a query needs visual images.
        
        This method leverages the LLM's understanding to determine whether
        a user query would benefit from visual content to provide a complete answer.
        
        Args:
            query (str): User's input query to analyze
            
        Returns:
            bool: True if images would enhance the response, False otherwise
            
        Example:
            >>> assistant.should_search_images("What is Python?")
            False
            >>> assistant.should_search_images("Tesla Model Y interior")
            True
        """
        prompt = f"""
Does this query need visual images to answer properly?
Query: "{query}"

Answer only "YES" or "NO"
"""
        response = self.llm.invoke(prompt)
        return "YES" in response.content.upper()
    
    def get_text_response_with_images(self, query: str, images_data: dict = None) -> str:
        """
        Generate comprehensive text response using LangChain WITH image data context.
        
        This method creates two types of responses:
        1. Standard text-only response (when no images provided)
        2. Image-aware response that references and incorporates found visual content
        
        Args:
            query (str): Original user query
            images_data (dict, optional): Image search results from Apify Actor
                Expected structure:
                {
                    'images': [list of image objects],
                    'search_perspectives': [list of perspective objects],
                    'total_results': int
                }
                
        Returns:
            str: Generated text response, enhanced with image context if available
            
        Note:
            When images_data is provided, the response will reference specific
            images found and explain how they relate to the query.
        """
        if not images_data or not images_data.get('images'):
            # No images - regular text response
            prompt = f"Answer this query comprehensively: '{query}'"
        else:
            # WITH images - create rich context
            images = images_data.get('images', [])
            perspectives = images_data.get('search_perspectives', [])
            
            # Build image context for the LLM
            image_context = f"""
I found {len(images)} relevant images from {len(perspectives)} different search perspectives:

SEARCH PERSPECTIVES USED:
"""
            for p in perspectives:
                image_context += f"• {p.get('perspective_type', 'Unknown').title()}: '{p.get('query', 'N/A')}' ({p.get('images_found', 0)} images)\n"
            
            image_context += f"\nIMAGE DETAILS FOUND:\n"
            for i, img in enumerate(images[:5]):  # Include details of first 5 images
                image_context += f"{i+1}. Title: {img.get('title', 'Untitled')}\n"
                image_context += f"   Source: {img.get('display_link', 'N/A')}\n"
                image_context += f"   Perspective: {img.get('perspective_query', 'N/A')}\n"
                image_context += f"   Size: {img.get('width', 'N/A')}x{img.get('height', 'N/A')}\n\n"
            
            prompt = f"""
Answer this query comprehensively: "{query}"

IMPORTANT: I have found relevant images that provide visual context for this topic. Use this image information to enhance your response:

{image_context}

Guidelines:
1. Reference the specific images found and how they relate to your explanation
2. Mention the different perspectives covered by the image search
3. Explain how the visual content complements your text response
4. Provide a comprehensive answer that combines your knowledge with the visual evidence found

Provide a detailed response that integrates both textual explanation and references to the visual content discovered.
"""
        
        response = self.llm.invoke(prompt)
        return response.content
    
    def search_images(self, query: str, max_results: int = 10) -> dict:
        """
        Search for real images using the Apify Actor with multi-perspective approach.
        
        This method calls the AI Query Based Image Finder actor which:
        1. Decomposes the query into multiple search perspectives
        2. Searches Google Images from different angles
        3. Returns authentic, non-AI generated images with metadata
        
        Args:
            query (str): Search query for finding relevant images
            max_results (int, optional): Maximum number of images to return. Defaults to 10.
            
        Returns:
            dict: Search results containing:
                - images: List of image objects with URLs, titles, sources
                - search_perspectives: Different search angles used
                - total_results: Total number of images found
                - agent_response: Summary of search strategy
                
        Raises:
            Exception: If Apify Actor execution fails or times out
            
        Example:
            >>> results = assistant.search_images("Tesla dashboard", 5)
            >>> print(f"Found {results['total_results']} images")
        """
        run = self.apify_client.actor("loongnian714/ai-query-based-image-finder").call(
            run_input={
                "query": query,
                "maxResults": max_results
            }
        )
        
        results = list(self.apify_client.dataset(run["defaultDatasetId"]).iterate_items())
        return results[0] if results else {}
    
    def process_query(self, query: str) -> dict:
        """
        Main processing method: Complete LangChain + Apify workflow.
        
        This method orchestrates the entire process:
        1. LangChain analyzes if images are needed for the query
        2. If needed, Apify Actor searches for relevant images
        3. LangChain generates a response incorporating image context
        
        Args:
            query (str): User's input query to process
            
        Returns:
            dict: Complete response containing:
                - query: Original user query
                - text_response: Generated text (image-aware if applicable)
                - images: List of found images (empty if none needed)
                - total_images: Count of images found
                - perspectives: Search perspectives used
                - image_aware_response: Boolean indicating if images influenced response
                
        Example:
            >>> result = assistant.process_query("Tesla Model Y interior")
            >>> print(result['text_response'])  # Image-aware response
            >>> print(f"Found {result['total_images']} supporting images")
        """
        print(f"\n🔍 Processing: '{query}'")
        
        # Step 1: LangChain decides if images are needed
        needs_images = self.should_search_images(query)
        print(f"🧠 LangChain analysis: {'Images needed' if needs_images else 'Text only'}")
        
        # Step 2: Search images with Apify if needed
        images_data = {}
        if needs_images:
            print("🚀 Searching with Apify Actor...")
            images_data = self.search_images(query)
            print(f"✅ Found {images_data.get('total_results', 0)} images from {len(images_data.get('search_perspectives', []))} perspectives")
        
        # Step 3: LangChain generates response WITH image data context
        print("📝 Generating image-aware response with LangChain...")
        text_response = self.get_text_response_with_images(query, images_data)
        
        return {
            'query': query,
            'text_response': text_response,
            'images': images_data.get('images', []),
            'total_images': images_data.get('total_results', 0),
            'perspectives': images_data.get('search_perspectives', []),
            'image_aware_response': bool(images_data)
        }

def demo():
    """
    Demonstration function showing image-aware text generation capabilities.
    
    This demo showcases the difference between:
    1. Standard LLM responses (text-only queries)
    2. Image-aware LLM responses (queries enhanced with visual context)
    
    The demo processes two types of queries to illustrate how the system
    intelligently decides when to search for images and how those images
    enhance the final text response.
    
    Note:
        Requires valid API keys for both Google Gemini and Apify services.
        Replace "xxx" placeholders with actual API credentials.
    """
    # Setup
 
    assistant = LangChainApifyAssistant(
        gemini_key= "xxx",  # Replace with actual Google API key
        apify_token="xxx"  # Replace with actual Apify token
    )
    
    # Test queries demonstrating different response types
    queries = [
        "What is machine learning?",  # Text only - no images needed
        "Tesla Model Y interior dashboard",  # Text + Images - visual context enhances response
    ]
    
    for query in queries:
        result = assistant.process_query(query)
        
        print(f"\n📝 TEXT RESPONSE ({'Image-aware' if result['image_aware_response'] else 'Text-only'}):")
        print(result['text_response'])
        
        if result['images']:
            print(f"\n📸 IMAGES REFERENCED IN RESPONSE ({result['total_images']} found):")
            for i, img in enumerate(result['images'][:3]):
                print(f"  {i+1}. {img.get('title', 'Untitled')[:50]}...")
                print(f"     URL: {img.get('link', 'N/A')}")
        
        if result['perspectives']:
            print(f"\n🎯 PERSPECTIVES USED IN RESPONSE:")
            for p in result['perspectives']:
                print(f"  • {p.get('perspective_type', 'Unknown')}: {p.get('query', 'N/A')}")
        
        print("\n" + "="*60)
        input("Press Enter for next example...")

if __name__ == "__main__":
    demo()