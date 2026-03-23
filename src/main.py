import asyncio
import aiohttp
from bs4 import BeautifulSoup
from collections import deque
import json

class DistributedCrawler:
    def __init__(self, seeds, worker_count=10):
        self.seeds = seeds
        self.worker_count = worker_count
        self.queue = deque(seeds)
        self.visited = set()
        self.results = []

    async def crawl_page(self, url):
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                html = await response.text()
                soup = BeautifulSoup(html, 'html.parser')
                links = [link.get('href') for link in soup.find_all('a')]
                self.results.append({
                    'url': url,
                    'content': soup.get_text()
                })
                return links

    async def worker(self):
        while self.queue:
            url = self.queue.popleft()
            if url not in self.visited:
                self.visited.add(url)
                try:
                    new_links = await self.crawl_page(url)
                    self.queue.extend(new_links)
                except:
                    pass

    async def run(self):
        tasks = [asyncio.create_task(self.worker()) for _ in range(self.worker_count)]
        await asyncio.gather(*tasks)
        return self.results

if __name__ == '__main__':
    seeds = ['https://www.example.com', 'https://www.google.com', 'https://www.github.com']
    crawler = DistributedCrawler(seeds)
    results = asyncio.run(crawler.run())
    print(json.dumps(results, indent=2))