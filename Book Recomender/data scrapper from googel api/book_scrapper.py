import requests
import pandas as pd
import os
from dotenv import load_dotenv
import time

# === Load environment variables ===
load_dotenv()

# Get API key securely
API_KEY = os.getenv('GOOGLE_BOOKS_API_KEY')
if not API_KEY:
    raise ValueError("API key not found. Please set GOOGLE_BOOKS_API_KEY in your .env file.")

# === Queries to cover 10,000+ books ===
queries = [
    'machine learning', 'artificial intelligence', 'data science',
    'history', 'science fiction', 'fantasy', 'romance',
    'mystery', 'philosophy', 'psychology', 'self-help',
    'biography', 'business', 'technology', 'education',
    'health', 'travel', 'art', 'music', 'literature'
]

max_results = 40  # Google API limit per request
books = []

# === Retry helper ===
def fetch_with_retries(url, max_retries=3, backoff=2):
    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=15)
            response.raise_for_status()  # raises HTTPError for bad status
            return response
        except (requests.exceptions.ChunkedEncodingError, requests.exceptions.ConnectionError) as e:
            print(f"Network error: {e}. Retrying ({attempt + 1}/{max_retries})...")
            time.sleep(backoff * (attempt + 1))  # exponential backoff
        except requests.exceptions.RequestException as e:
            print(f"Failed with error: {e}. Not retrying.")
            break
    return None

# === Main loop ===
for query in queries:
    print(f"\n=== Fetching books for query: '{query}' ===")
    for start_index in range(0, 500, max_results):  # 500 books per query
        print(f"Fetching {query} books {start_index + 1} to {start_index + max_results}...")
        url = (
            f'https://www.googleapis.com/books/v1/volumes'
            f'?q={query}&startIndex={start_index}&maxResults={max_results}&key={API_KEY}'
        )

        response = fetch_with_retries(url)

        if response is None:
            print(f"Skipping {url} after multiple failures.")
            continue
        
        data = response.json()
        
        for item in data.get('items', []):
            volume_info = item.get('volumeInfo', {})
            books.append({
                'title': volume_info.get('title'),
                'authors': volume_info.get('authors'),
                'publishedDate': volume_info.get('publishedDate'),
                'categories': volume_info.get('categories'),
                'averageRating': volume_info.get('averageRating'),
                'ratingsCount': volume_info.get('ratingsCount'),
                'description': volume_info.get('description'),
                'pageCount': volume_info.get('pageCount'),
                'language': volume_info.get('language'),
                'previewLink': volume_info.get('previewLink'),
                'query': query
            })

        # Optional: small delay to be nice to the API
        time.sleep(0.5)

# === Save to CSV ===
print(f"\nTotal books fetched: {len(books)}")
df = pd.DataFrame(books)

# Optionally remove duplicates by title
df = df.drop_duplicates(subset='title')

df.to_csv('books_10000_data.csv', index=False)
print("✅ Books data saved to 'books_10000_data.csv'")
print(df.head())
