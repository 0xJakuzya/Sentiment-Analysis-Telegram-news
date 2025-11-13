class SentimentAnalysisPipeline:

    def __init__(self):
        self.scraper = TelegramScraper()
        self.preprocessor = TextPreprocessor()
        self.feature_extractor = FeatureExtractor()
        self.model = SentimentModel()
        self.db = MongoDBClient()

    def run_full_pipeline(self):
        pass

    
