import asyncio
import os
import sys
sys.path.append('src')
sys.path.append('scripts')

from config import config  
from telegram_scraper import TelegramScraper
from mongo import MongoDBClient
from preprocessing import TextProcessor

class Pipeline:

    def __init__(self, session_name):
        self.session_name = session_name
        self.mongo_client = MongoDBClient(config.get("mongo_config"))
        self.scraper = TelegramScraper(session_name, self.mongo_client)
        self.text_processor = TextProcessor(config.get("preprocessing", {}).get("text_processor", {}))
        # self.embedder = FeatureExtractor()
        # self.classifier = Sentiment

    def _load_channels(self):
        return config.get("channels", [])

    async def scrape_channel(self, channel_name, limit):
        await self.scraper.connect()
        try:
            messages = await self.scraper.parse_channel(channel_name, limit)
            if messages:
                self.scraper.save2mongodb(messages)
            return messages
        finally:
            await self.scraper.disconnect()

    def preprocess_messages(self, messages):
        for message in messages:
            if "message" in message:
                message["cleaned_message"] = self.text_processor(message["message"])
                message["is_processed"] = True
        return messages

    def classify_messages(self, messages):
        for message in messages:
            message["sentiment"] = None
            message["confidence"] = None
        return messages

    def save_results(self, messages):
        self.mongo_client.save_processed_messages(messages)

    async def run_full_pipeline(self, limit=10):
        all_messages = []
        for channel in self._load_channels():
            all_messages.extend(await self.scrape_channel(channel, limit))
        
        processed_messages = self.preprocess_messages(all_messages)
        classified_messages = self.classify_messages(processed_messages)
        self.save_results(classified_messages)
        return classified_messages

    def close(self):
        self.mongo_client.close()

if __name__ == "__main__":
    pipeline = Pipeline(os.getenv("SESSION_NAME", "default_session"))
    try:
        asyncio.run(pipeline.run_full_pipeline(limit=50))
    finally:
        pipeline.close()