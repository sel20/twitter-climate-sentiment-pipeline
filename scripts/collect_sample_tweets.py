#!/usr/bin/env python3
"""
Collect sample tweets from Twitter API v2 for development and testing.

This script collects 1000+ tweets about climate events and saves them
to data/sample_tweets.json for use during development.
"""

import os
import json
import time
import requests
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

TWITTER_API_URL = "https://api.twitter.com/2/tweets/search/recent"
BEARER_TOKEN = os.getenv('TWITTER_BEARER_TOKEN')

# Search query for climate-related tweets
SEARCH_QUERY = "(storm OR cyclone OR flood OR heatwave OR wildfire OR #climatechange OR #climate OR #weather) -is:retweet lang:en"

def collect_tweets(max_tweets=1000):
    """
    Collect tweets from Twitter API v2.
    
    Args:
        max_tweets: Maximum number of tweets to collect
        
    Returns:
        List of tweet dicts
    """
    if not BEARER_TOKEN:
        print("Error: TWITTER_BEARER_TOKEN not set in .env file")
        return []
    
    all_tweets = []
    next_token = None
    
    headers = {
        'Authorization': f'Bearer {BEARER_TOKEN}'
    }
    
    print(f"Collecting up to {max_tweets} tweets...")
    print(f"Query: {SEARCH_QUERY}")
    print()
    
    while len(all_tweets) < max_tweets:
        # Build request parameters
        params = {
            'query': SEARCH_QUERY,
            'max_results': min(100, max_tweets - len(all_tweets)),
            'tweet.fields': 'created_at,author_id,lang,geo,entities,public_metrics',
            'expansions': 'geo.place_id,author_id',
            'place.fields': 'full_name,geo',
            'user.fields': 'location'
        }
        
        if next_token:
            params['next_token'] = next_token
        
        try:
            print(f"Fetching batch {len(all_tweets) // 100 + 1}...")
            response = requests.get(
                TWITTER_API_URL,
                headers=headers,
                params=params,
                timeout=30
            )
            
            if response.status_code == 429:
                print("Rate limit hit, waiting 15 minutes...")
                time.sleep(900)
                continue
            
            if response.status_code != 200:
                print(f"Error: {response.status_code} - {response.text}")
                break
            
            data = response.json()
            tweets = data.get('data', [])
            includes = data.get('includes', {})
            meta = data.get('meta', {})
            
            if not tweets:
                print("No more tweets found")
                break
            
            # Add place and user data to tweets
            places = {place['id']: place for place in includes.get('places', [])}
            users = {user['id']: user for user in includes.get('users', [])}
            
            for tweet in tweets:
                # Add place data
                if 'geo' in tweet and 'place_id' in tweet['geo']:
                    place_id = tweet['geo']['place_id']
                    if place_id in places:
                        tweet['place'] = places[place_id]
                
                # Add user location
                author_id = tweet.get('author_id')
                if author_id in users:
                    tweet['user_location'] = users[author_id].get('location')
            
            all_tweets.extend(tweets)
            print(f"  Collected {len(tweets)} tweets (total: {len(all_tweets)})")
            
            # Check for next page
            next_token = meta.get('next_token')
            if not next_token:
                print("No more pages available")
                break
            
            # Rate limiting: wait between requests
            time.sleep(2)
            
        except requests.exceptions.RequestException as e:
            print(f"Request error: {e}")
            break
        except Exception as e:
            print(f"Unexpected error: {e}")
            break
    
    return all_tweets

def save_tweets(tweets, output_file='data/sample_tweets.json'):
    """Save tweets to JSON file."""
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(tweets, f, indent=2, ensure_ascii=False)
    
    print(f"\nSaved {len(tweets)} tweets to {output_file}")

def analyze_tweets(tweets):
    """Print statistics about collected tweets."""
    if not tweets:
        return
    
    print("\n" + "=" * 60)
    print("Tweet Collection Statistics")
    print("=" * 60)
    
    # Language distribution
    langs = {}
    for tweet in tweets:
        lang = tweet.get('lang', 'unknown')
        langs[lang] = langs.get(lang, 0) + 1
    
    print(f"\nTotal tweets: {len(tweets)}")
    print(f"\nLanguage distribution:")
    for lang, count in sorted(langs.items(), key=lambda x: x[1], reverse=True)[:10]:
        print(f"  {lang}: {count} ({count/len(tweets)*100:.1f}%)")
    
    # Geolocation availability
    with_coords = sum(1 for t in tweets if 'geo' in t and t['geo'] and 'coordinates' in t['geo'])
    with_place = sum(1 for t in tweets if 'place' in t and t['place'])
    with_user_loc = sum(1 for t in tweets if 'user_location' in t and t['user_location'])
    
    print(f"\nGeolocation data:")
    print(f"  With coordinates: {with_coords} ({with_coords/len(tweets)*100:.1f}%)")
    print(f"  With place: {with_place} ({with_place/len(tweets)*100:.1f}%)")
    print(f"  With user location: {with_user_loc} ({with_user_loc/len(tweets)*100:.1f}%)")
    
    # Hashtag analysis
    hashtags = {}
    for tweet in tweets:
        for hashtag in tweet.get('entities', {}).get('hashtags', []):
            tag = hashtag.get('tag', '').lower()
            hashtags[tag] = hashtags.get(tag, 0) + 1
    
    print(f"\nTop hashtags:")
    for tag, count in sorted(hashtags.items(), key=lambda x: x[1], reverse=True)[:10]:
        print(f"  #{tag}: {count}")
    
    print("\n" + "=" * 60)

def main():
    print("=" * 60)
    print("Twitter Climate Tweet Collection")
    print("=" * 60)
    print()
    
    # Collect tweets
    tweets = collect_tweets(max_tweets=1000)
    
    if not tweets:
        print("\nNo tweets collected. Please check your API credentials.")
        return
    
    # Analyze tweets
    analyze_tweets(tweets)
    
    # Save tweets
    save_tweets(tweets)
    
    print("\nCollection complete!")
    print("You can now use data/sample_tweets.json for development")

if __name__ == "__main__":
    main()
