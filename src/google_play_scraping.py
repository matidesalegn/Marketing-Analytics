import logging
import pandas as pd
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
import random
import time
import requests

class GooglePlayScraper:
    def __init__(self, app_ids):
        self.app_ids = app_ids

    def fetch_reviews(self, app_id, num_reviews=100, max_retries=5):
        """Fetch a limited number of reviews for a given app with retry mechanism."""
        all_reviews = []
        continuation_token = None
        retries = 0
    
        while len(all_reviews) < num_reviews and retries < max_retries:
            try:
                response = requests.get(
                    f"https://play.google.com/store/getreviews?id={app_id}&reviewSortOrder=0&reviewType=1&pageNum=0&hl=en&showAllReviews=true",
                    timeout=10  # Adjust timeout value as needed
                )
                response.raise_for_status()
                data = response.json()
                reviews_data = data[0][2]
                continuation_token = data[0][3][6][0]
                
                all_reviews.extend(reviews_data)
                if not continuation_token:
                    break
            except (requests.RequestException, requests.Timeout) as e:
                logger.error("Error while fetching reviews for app ID %s: %s", app_id, e)
                retries += 1
                sleep_time = (2 ** retries) + random.uniform(0, 1)
                logger.info("Retrying in %f seconds...", sleep_time)
                time.sleep(sleep_time)
        return all_reviews[:num_reviews]

    def fetch_app_details(self, app_id, max_retries=5):
        """Fetch app details to get download count and other metadata with retry mechanism."""
        retries = 0
        while retries < max_retries:
            try:
                details = app(app_id)
                download_count = details.get('installs', 'N/A')
                current_date = datetime.now().date()
                return {'downloads': download_count, 'date': current_date}
            except (HTTPError, URLError) as e:
                logger.error("Error while fetching details for app ID %s: %s", app_id, e)
                retries += 1
                sleep_time = (2 ** retries) + random.uniform(0, 1)
                logger.info("Retrying in %f seconds...", sleep_time)
                time.sleep(sleep_time)
        return {}

class ReviewTracker:
    def __init__(self, app_ids):
        self.scraper = GooglePlayScraper(app_ids)

    def fetch_data_for_bank(self, bank, app_id, duration_days=7, num_reviews=100):
        """Fetch data for a specific bank."""
        end_time = datetime.now()
        start_time = end_time - timedelta(days=duration_days)
        
        logger.info("Fetching data for %s", bank)
        
        # Fetch reviews
        reviews_data = self.scraper.fetch_reviews(app_id, num_reviews)
        if not reviews_data:
            logger.info("No reviews found for %s", bank)
            return [], []
        
        logger.info("Fetched %d reviews for %s", len(reviews_data), bank)
        
        # Filter reviews by time
        filtered_reviews = [review for review in reviews_data if datetime.fromtimestamp(review['at'].timestamp()) >= start_time]
        logger.info("Filtered down to %d reviews for %s", len(filtered_reviews), bank)
        
        # Fetch app details over the specified duration
        download_data_list = []
        current_date = start_time
        while current_date <= end_time:
            details = self.scraper.fetch_app_details(app_id)
            download_count = details.get('downloads', 'N/A')
            date = details.get('date', current_date)
            download_data = {
                'bank': bank,
                'appId': app_id,
                'date': date,
                'downloads': download_count
            }
            download_data_list.append(download_data)
            current_date += timedelta(days=1)
        
        all_reviews = []
        for review in filtered_reviews:
            review_data = {
                'bank': bank,
                'appId': app_id,
                'reviewId': review['reviewId'],
                'userName': review['userName'],
                'userImage': review['userImage'],
                'thumbsUpCount': review['thumbsUpCount'],
                'reviewCreatedVersion': review.get('reviewCreatedVersion'),
                'at': review['at'],
                'replyContent': review.get('replyContent', ''),
                'repliedAt': review.get('repliedAt', ''),
                'appVersion': review.get('appVersion', ''),
                'score': review['score'],
                'content': review['content'],
                'keywords': '',  # Placeholder for keywords
                'LDA_Category': '',  # Placeholder for LDA category
                'Sentiment': '',  # Placeholder for sentiment
                'Insight': ''  # Placeholder for insight
            }
            all_reviews.append(review_data)
        
        return all_reviews, download_data_list

    def track_reviews_and_downloads(self, duration_days=7, num_reviews=100):
        """Track reviews and download counts over a period of time."""
        all_reviews = []
        all_downloads = []
        
        with ThreadPoolExecutor(max_workers=len(self.scraper.app_ids)) as executor:
            futures = [executor.submit(self.fetch_data_for_bank, bank, app_id, duration_days, num_reviews) for bank, app_id in self.scraper.app_ids.items()]
            
            for future in futures:
                reviews, downloads = future.result()
                all_reviews.extend(reviews)
                all_downloads.extend(downloads)
        
        # Create DataFrames from the collected reviews and download counts
        df_reviews = pd.DataFrame(all_reviews)
        df_downloads = pd.DataFrame(all_downloads)
        
        # Print the heads of the DataFrames
        print("Reviews DataFrame head:")
        print(df_reviews.head())
        print("Downloads DataFrame head:")
        print(df_downloads.head())
        
        # Ensure the data directory exists
        os.makedirs('data', exist_ok=True)
        
        # Save the DataFrames to separate CSV files
        reviews_csv_file_path = '../data/google_play_reviews.csv'
        downloads_csv_file_path = '../data/google_play_downloads.csv'
        df_reviews.to_csv(reviews_csv_file_path, index=False)
        df_downloads.to_csv(downloads_csv_file_path, index=False)
        
        logger.info("Saved reviews data to %s", reviews_csv_file_path)
        logger.info("Saved downloads data to %s", downloads_csv_file_path)

if __name__ == "__main__":
    # Setup logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    # Define the app IDs for the banks
    app_ids = {
        'Abyssinia Bank': 'com.boa.boaMobileBanking',
        'Commercial Bank of Ethiopia': 'com.combanketh.mobilebanking',
    }

    tracker = ReviewTracker(app_ids)
    tracker.track_reviews_and_downloads(num_reviews=100)
