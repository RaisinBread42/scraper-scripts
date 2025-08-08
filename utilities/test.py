# used to test getting markdown of a web page.
import re
import asyncio
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, DefaultMarkdownGenerator

async def main():
    # Create an instance of AsyncWebCrawler
    async with AsyncWebCrawler() as crawler:
        # Run the crawler on a URL

        cleaned_md_generator = DefaultMarkdownGenerator(
            content_source="cleaned_html",  # This is the default
        )

        config = CrawlerRunConfig(
            # e.g., first 30 items from Hacker News
            css_selector="div.main_property_wrap",
            markdown_generator = cleaned_md_generator,
            wait_for_images = True,
            scan_full_page = True,
            scroll_delay=0.5, 
        )

        urls = [
            "https://www.tridentproperties.ky/for-sale/idx_Y/filterby_N"
        ]

        results = await crawler.arun_many(urls=urls, config=config)

         # Loop through results and print markdown
        for result in results:
            print(f"Markdown for {result.url}:\n")
            print(result.markdown)
            print("\n" + "="*80 + "\n")

# Run the async main function
asyncio.run(main())