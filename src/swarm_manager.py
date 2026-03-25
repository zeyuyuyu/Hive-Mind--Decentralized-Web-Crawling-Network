import asyncio
from typing import List
from .crawler_node import CrawlerNode

class SwarmManager:
    def __init__(self, nodes: List[CrawlerNode]):
        self.nodes = nodes
        self.load_balancer = LoadBalancer(nodes)

    async def crawl(self, urls: List[str]):
        tasks = []
        for url in urls:
            node = self.load_balancer.get_least_loaded_node()
            task = asyncio.create_task(node.crawl(url))
            tasks.append(task)
        await asyncio.gather(*tasks)

class LoadBalancer:
    def __init__(self, nodes: List[CrawlerNode]):
        self.nodes = nodes

    def get_least_loaded_node(self) -> CrawlerNode:
        least_loaded_node = min(self.nodes, key=lambda node: node.get_load())
        return least_loaded_node
