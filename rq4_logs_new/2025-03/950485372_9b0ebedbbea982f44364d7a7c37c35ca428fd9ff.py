import asyncio
import logging
import os
from mcp import ClientSession
from mcp.client.sse import sse_client

# Configure logging
logging.basicConfig(level=logging.INFO)

async def main():
    # Get server URL from environment variable or use default
    server_url = os.getenv("SERVER_URL", "http://127.0.0.1:3001/sse")
    print(f"Connecting to MCP server at {server_url}...")
    
    try:
        # Use async with for the context manager
        async with sse_client(server_url) as streams:
            print("SSE client connection established")
            async with ClientSession(streams[0], streams[1]) as session:
                # Initialize connection
                print("Initializing connection...")
                await session.initialize()
                print("Connection initialized successfully!")
                
                # List available tools
                print("Listing tools...")
                response = await session.list_tools()
                tools = response.tools
                
                print(f"\nServer provides {len(tools)} tools:")
                for tool in tools:
                    print(f"  - {tool.name}: {tool.description}")
                
                # If we found the hello_world tool, test it
                if any(tool.name == "hello_world" for tool in tools):
                    print("\nTesting hello_world tool...")
                    result = await session.call_tool("hello_world", {"name": "MCP Tester"})
                    
                    print("Tool result:")
                    for content in result.content:
                        if content.type == "text":
                            print(f"  {content.text}")
    
    except Exception as e:
        print(f"\nError: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())