import os
import asyncio
import json
import crawl4ai
from pydantic import BaseModel, Field
from typing import List
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode
from crawl4ai.extraction_strategy import LLMExtractionStrategy

class Albums(BaseModel):
    title: str
    url: str
    year: int
    language: str

async def main():
    # 1. Define the LLM extraction strategy
    llm_strategy = LLMExtractionStrategy(
        provider="gemini/gemini-1.5-flash",            # e.g. "ollama/llama2"
        schema=Albums.model_json_schema(),            # Or use model_json_schema()
        extraction_type="schema",
        instruction="Extract all Titles, URLs, Years and Language from the tables in the page.",
        chunk_token_threshold=10000,
        overlap_rate=0.0,
        apply_chunking=True,
        input_format="html",   # or "html", "fit_markdown"
        extra_args={"temperature": 0.0, "max_tokens": 2000000}
    )

    # 2. Build the crawler config
    crawl_config = CrawlerRunConfig(
        extraction_strategy=llm_strategy,
        cache_mode=CacheMode.BYPASS
    )

    # 3. Create a browser config if needed
    browser_cfg = BrowserConfig(headless=True)

    async with AsyncWebCrawler(config=browser_cfg) as crawler:
        # 4. Let's say we want to crawl a single page
        result = await crawler.arun(
            url="https://en.wikipedia.org/wiki/A._R._Rahman_discography",
            config=crawl_config
        )

        if result.success:
            # 5. The extracted content is presumably JSON
            data = json.loads(result.extracted_content)
            print("Extracted items:", data)
            try:
                with open('data.json', 'w') as f:
                    json.dump(data, f, indent=4)
            except:
                print("Error writing to file")


            # 6. Show usage stats
            llm_strategy.show_usage()  # prints token usage
        else:
            print("Error:", result.error_message)

if __name__ == "__main__":
    asyncio.run(main())