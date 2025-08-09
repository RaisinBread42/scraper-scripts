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
            css_selector="div#grid-view",
            markdown_generator = cleaned_md_generator,
            wait_for_images = True,
            scan_full_page = True,
            scroll_delay=0.5, 
        )

        # URLs collected from all Python scripts in the root directory
        urls = [
            "https://www.cireba.com/cayman-residential-property-for-sale/listingtype_14/filterby_N",
            "https://www.cireba.com/cayman-residential-property-for-sale/listingtype_14/filterby_N#2",
            "https://www.cireba.com/cayman-residential-property-for-sale/listingtype_4/filterby_N",
            "https://www.cireba.com/cayman-residential-property-for-sale/listingtype_4/filterby_N#2"
            "https://www.cireba.com/cayman-residential-property-for-sale/listingtype_5/filterby_N",
            "https://www.cireba.com/cayman-residential-property-for-sale/listingtype_5/filterby_N#2"

        ]

        results = await crawler.arun_many(urls=urls, config=config)

        # Save results to markdown file
        with open('crawl_results.md', 'w', encoding='utf-8') as f:
            for i, result in enumerate(results, 1):
                f.write(result.markdown)

                # Also print to console for immediate feedback
                print(f"Processed {i}/{len(results)}: {result.url}")

        print(f"\nResults saved to 'crawl_results.md'")

# Run the async main function
asyncio.run(main())