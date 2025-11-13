from pipeline import SentimentAnalysisPipeline

if __name__ == "__main__":

    pipeline = SentimentAnalysisPipeline()
    pipeline.run_full_pipeline()
    results = pipeline.run_inference()

    print(results)