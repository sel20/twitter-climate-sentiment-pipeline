"""
WriteBolt - Writes processed tweets to Parquet files.

This bolt buffers tweets and writes them to partitioned Parquet files
with Snappy compression for efficient storage and querying.
"""

import os
import logging
import time
import json
from typing import Dict, Any, List
from datetime import datetime
import sys

# Parquet writing
try:
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
    PARQUET_AVAILABLE = True
except ImportError:
    PARQUET_AVAILABLE = False
    logging.warning("pyarrow/pandas not available, Parquet writing disabled")

# Storm imports
try:
    from streamparse.bolt import Bolt
except ImportError:
    class Bolt:
        def initialize(self, conf, ctx):
            pass
        def process(self, tup):
            pass

logger = logging.getLogger(__name__)


class WriteBolt(Bolt):
    """
    Storm bolt that writes processed tweets to Parquet files.
    
    Features:
    - Batch writing with configurable buffer size
    - Partitioning by processing_date (YYYY-MM-DD)
    - Snappy compression
    - Dictionary encoding for categorical columns
    - Separate raw JSON storage for audit
    - Automatic flushing (buffer full or time-based)
    """
 
   
    def initialize(self, conf, ctx):
        """Initialize the bolt."""
        self.conf = conf
        self.ctx = ctx
        
        # Configuration
        self.buffer_size = int(os.getenv('BUFFER_SIZE', '1000'))
        self.flush_interval = int(os.getenv('FLUSH_INTERVAL_SECONDS', '300'))
        
        # Storage paths
        self.use_local_storage = os.getenv('USE_LOCAL_STORAGE', 'false').lower() == 'true'
        
        if self.use_local_storage:
            self.storage_root = os.getenv('LOCAL_STORAGE_PATH', '/tmp/twitter_climate')
        else:
            hdfs_namenode = os.getenv('HDFS_NAMENODE', 'hdfs://localhost:9000')
            storage_root = os.getenv('STORAGE_ROOT', '/twitter_climate')
            self.storage_root = f"{hdfs_namenode}{storage_root}"
        
        self.processed_path = f"{self.storage_root}/processed_tweets"
        self.raw_path = f"{self.storage_root}/raw_tweets"
        
        # Create directories if local storage
        if self.use_local_storage:
            os.makedirs(self.processed_path, exist_ok=True)
            os.makedirs(self.raw_path, exist_ok=True)
        
        # Buffer
        self.buffer = []
        self.last_flush_time = time.time()
        
        # Statistics
        self.processed = 0
        self.records_written = 0
        self.flush_count = 0
        
        logger.info("WriteBolt initialized")
        logger.info(f"  Storage: {self.storage_root}")
        logger.info(f"  Buffer size: {self.buffer_size}")
        logger.info(f"  Flush interval: {self.flush_interval}s")
    
    def process(self, tup):
        """
        Process incoming tuple from SentimentBolt.
        
        Args:
            tup: Tuple containing fully processed tweet data
        """
        try:
            # Extract tweet data
            tweet_data = tup.values[0]
            
            # Add to buffer
            self.buffer.append(tweet_data)
            self.processed += 1
            
            # Check flush conditions
            buffer_full = len(self.buffer) >= self.buffer_size
            time_elapsed = (time.time() - self.last_flush_time) >= self.flush_interval
            
            if buffer_full or time_elapsed:
                self._flush_buffer()
            
            # Ack tuple
            self.ack(tup)
        
        except Exception as e:
            logger.error(f"Error processing tuple: {e}", exc_info=True)
            self.fail(tup)
    
    def _flush_buffer(self):
        """Flush buffer to Parquet files."""
        if not self.buffer:
            return
        
        if not PARQUET_AVAILABLE:
            logger.error("Parquet writing not available")
            self.buffer.clear()
            return
        
        try:
            logger.info(f"Flushing {len(self.buffer)} records to Parquet...")
            
            # Convert to DataFrame
            df = pd.DataFrame(self.buffer)
            
            # Add processing metadata
            processing_date = datetime.utcnow().strftime('%Y-%m-%d')
            df['processing_date'] = processing_date
            
            # Write Parquet
            self._write_parquet(df, processing_date)
            
            # Write raw JSON
            self._write_raw_json(self.buffer, processing_date)
            
            # Update statistics
            self.records_written += len(self.buffer)
            self.flush_count += 1
            
            logger.info(f"Flushed {len(self.buffer)} records (total: {self.records_written})")
            
            # Clear buffer
            self.buffer.clear()
            self.last_flush_time = time.time()
        
        except Exception as e:
            logger.error(f"Error flushing buffer: {e}", exc_info=True)
            # Keep buffer for retry
    
    def _write_parquet(self, df: pd.DataFrame, processing_date: str):
        """Write DataFrame to Parquet with partitioning."""
        try:
            # Partition path
            partition_path = f"{self.processed_path}/processing_date={processing_date}"
            
            # Generate filename with timestamp
            timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
            filename = f"tweets_{timestamp}.parquet"
            filepath = f"{partition_path}/{filename}"
            
            # Create directory if local
            if self.use_local_storage:
                os.makedirs(partition_path, exist_ok=True)
            
            # Write Parquet
            df.to_parquet(
                filepath,
                engine='pyarrow',
                compression='snappy',
                index=False
            )
            
            logger.debug(f"Wrote Parquet file: {filepath}")
        
        except Exception as e:
            logger.error(f"Error writing Parquet: {e}")
            raise
    
    def _write_raw_json(self, records: List[Dict], processing_date: str):
        """Write raw JSON for audit."""
        try:
            # Partition path
            partition_path = f"{self.raw_path}/{processing_date}"
            
            # Generate filename
            timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
            filename = f"tweets_{timestamp}.json"
            filepath = f"{partition_path}/{filename}"
            
            # Create directory if local
            if self.use_local_storage:
                os.makedirs(partition_path, exist_ok=True)
            
            # Write JSON
            with open(filepath, 'w', encoding='utf-8') as f:
                for record in records:
                    json.dump(record, f, ensure_ascii=False)
                    f.write('\n')
            
            logger.debug(f"Wrote raw JSON: {filepath}")
        
        except Exception as e:
            logger.error(f"Error writing raw JSON: {e}")
            # Non-critical, don't raise


if __name__ == "__main__":
    # For local testing
    
    os.environ['USE_LOCAL_STORAGE'] = 'true'
    os.environ['LOCAL_STORAGE_PATH'] = '/tmp/twitter_climate_test'
    
    bolt = WriteBolt()
    bolt.initialize({}, {})
    
    # Test data
    test_tweets = [
        {
            'tweet_id': '123',
            'text_clean': 'Test tweet 1',
            'sentiment_score': 0.8,
            'sentiment_label': 'positive',
            'geo_lat': 29.76,
            'geo_lon': -95.37
        },
        {
            'tweet_id': '124',
            'text_clean': 'Test tweet 2',
            'sentiment_score': -0.5,
            'sentiment_label': 'negative',
            'geo_lat': 25.78,
            'geo_lon': -80.25
        }
    ]
    
    print("Testing WriteBolt:\n")
    
    # Add to buffer
    for tweet in test_tweets:
        bolt.buffer.append(tweet)
    
    print(f"Buffer size: {len(bolt.buffer)}")
    
    # Flush
    bolt._flush_buffer()
    
    print(f"Records written: {bolt.records_written}")
    print(f"Flush count: {bolt.flush_count}")
    print(f"\nCheck output at: {bolt.storage_root}")
