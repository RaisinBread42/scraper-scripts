# used to test getting markdown of a web page.
import re
import asyncio
from crawl4ai import AsyncWebCrawler, CacheMode, CrawlerRunConfig, DefaultMarkdownGenerator

async def main():
    # Create an instance of AsyncWebCrawler
    async with AsyncWebCrawler() as crawler:
        # Run the crawler on a URL

        cleaned_md_generator = DefaultMarkdownGenerator(
            content_source="raw_html",
        )

        config = CrawlerRunConfig(
            # e.g., first 30 items from Hacker News
            css_selector="div#listing-results",
            markdown_generator = cleaned_md_generator,
            wait_for_images = False,
            scan_full_page = True,
            scroll_delay=3, 

        )

        # URLs collected from all Python scripts in the root directory
        urls = [
        "https://ecaytrade.com/real-estate/for-sale?page=1&minprice=100000&type=apartments+condos+duplexes+houses+townhouses&location=Bodden%20Town/Breakers,East%20End/High%20Rock,George%20Town,North%20Side,Red%20Bay/Prospect,Rum%20Point/Kaibo,Savannah/Newlands,Seven%20Mile%20Beach,Seven%20Mile%20Beach%20Corridor,South%20Sound,Spotts,West%20Bay&sort=date-high"
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