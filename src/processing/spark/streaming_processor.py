#!/usr/bin/env python3
"""
Simplified Spark Streaming Music Recommendation System
Focuses on basic functionality without complex UDFs
"""

import json
import logging
from collections import defaultdict
import pandas as pd
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, collect_list, current_timestamp, 
    to_timestamp, size, when, lit, count, first, last
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, TimestampType
)
from pymongo import MongoClient
import argparse

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SimpleMusicRecommender:
    """Simplified music recommendation system"""
    
    def __init__(self, 
                 kafka_servers: str = "localhost:9092",
                 kafka_topic: str = "user-tracks",
                 mongo_uri: str = "mongodb://localhost:27017",
                 mongo_db: str = "recommendations",
                 mongo_collection: str = "user_sessions",
                 checkpoint_location: str = "/tmp/checkpoints"):
        
        self.kafka_servers = kafka_servers
        self.kafka_topic = kafka_topic
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
        self.mongo_collection = mongo_collection
        self.checkpoint_location = checkpoint_location
        
        # Create directories
        os.makedirs(self.checkpoint_location, exist_ok=True)
        os.makedirs(f"{self.checkpoint_location}/main", exist_ok=True)
        
        # Initialize Spark
        self.spark = self._create_spark_session()
        self.mongo_client = None
        
        logger.info("Simplified recommender initialized")
    
    def _create_spark_session(self) -> SparkSession:
        """Create minimal Spark session"""
        return SparkSession.builder \
            .appName("SimpleMusicRecommender") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.streaming.stopGracefullyOnShutdown", "true") \
            .getOrCreate()
    
    def _get_schema(self) -> StructType:
        """Simple event schema"""
        return StructType([
            StructField("user_id", StringType(), True),
            StructField("track_id", StringType(), True),
            StructField("session_id", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("artist", StringType(), True),
            StructField("song_title", StringType(), True)
        ])
    
    def _create_stream(self):
        """Create simple Kafka stream"""
        return self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_servers) \
            .option("subscribe", self.kafka_topic) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
    
    def _process_events(self, kafka_df):
        """Simple event processing without complex UDFs"""
        schema = self._get_schema()
        
        # Parse JSON
        events = kafka_df.select(
            from_json(col("value").cast("string"), schema).alias("data")
        ).select("data.*")
        
        # Convert timestamp
        events = events.withColumn(
            "event_time",
            to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS")
        ).filter(col("event_time").isNotNull() & (col("event_type") == "play"))
        
        # Simple session aggregation - no complex recommendations
        sessions = events \
            .withWatermark("event_time", "1 minute") \
            .groupBy(
                window(col("event_time"), "2 minutes"),
                col("session_id"),
                col("user_id")
            ).agg(
                collect_list(col("track_id")).alias("tracks"),
                count("*").alias("track_count"),
                first(col("artist")).alias("first_artist"),
                last(col("track_id")).alias("last_track")
            ).withColumn(
                "processing_time", current_timestamp()
            ).withColumn(
                "has_multiple_tracks", 
                when(col("track_count") > 1, True).otherwise(False)
            )
        
        return sessions
    
    def _write_to_mongo(self, df, epoch_id):
        """Simple MongoDB writer"""
        try:
            pandas_df = df.toPandas()
            
            if pandas_df.empty:
                logger.info(f"Epoch {epoch_id}: No data")
                return
            
            if not self.mongo_client:
                self.mongo_client = MongoClient(self.mongo_uri)
            
            db = self.mongo_client[self.mongo_db]
            collection = db[self.mongo_collection]
            
            # Simple conversion
            records = []
            for _, row in pandas_df.iterrows():
                record = {
                    "session_id": row.get("session_id"),
                    "user_id": row.get("user_id"),
                    "track_count": int(row.get("track_count", 0)),
                    "tracks": row.get("tracks", []) if isinstance(row.get("tracks"), list) else [],
                    "last_track": row.get("last_track"),
                    "first_artist": row.get("first_artist"),
                    "window_start": str(row.get("window.start")) if row.get("window") else None,
                    "window_end": str(row.get("window.end")) if row.get("window") else None,
                    "processing_time": str(row.get("processing_time")),
                    "has_multiple_tracks": bool(row.get("has_multiple_tracks", False))
                }
                records.append(record)
            
            if records:
                collection.insert_many(records)
                logger.info(f"Epoch {epoch_id}: Inserted {len(records)} records")
                
        except Exception as e:
            logger.error(f"MongoDB write error epoch {epoch_id}: {e}")
    
    def start(self):
        """Start simple streaming"""
        try:
            # Create stream
            kafka_stream = self._create_stream()
            logger.info("Created Kafka stream")
            
            # Process events
            processed = self._process_events(kafka_stream)
            logger.info("Processing events...")
            
            # Write to MongoDB
            query = processed.writeStream \
                .outputMode("update") \
                .foreachBatch(self._write_to_mongo) \
                .option("checkpointLocation", f"{self.checkpoint_location}/main") \
                .trigger(processingTime='10 seconds') \
                .start()
            
            # Console output for monitoring
            console = processed.select(
                col("session_id"), 
                col("user_id"), 
                col("track_count"),
                col("last_track")
            ).writeStream \
                .outputMode("update") \
                .format("console") \
                .option("numRows", 5) \
                .trigger(processingTime='30 seconds') \
                .start()
            
            logger.info("Streaming started - waiting for data...")
            query.awaitTermination()
            
        except Exception as e:
            logger.error(f"Streaming failed: {e}")
            raise
        finally:
            if self.mongo_client:
                self.mongo_client.close()
            self.spark.stop()

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Simple Music Recommender')
    parser.add_argument('--kafka-servers', default='localhost:9092')
    parser.add_argument('--kafka-topic', default='user-tracks')
    parser.add_argument('--mongo-uri', default='mongodb://localhost:27017')
    parser.add_argument('--mongo-db', default='recommendations')
    parser.add_argument('--mongo-collection', default='user_sessions')
    parser.add_argument('--checkpoint-location', default='/tmp/simple_checkpoints')
    
    args = parser.parse_args()
    
    try:
        recommender = SimpleMusicRecommender(
            kafka_servers=args.kafka_servers,
            kafka_topic=args.kafka_topic,
            mongo_uri=args.mongo_uri,
            mongo_db=args.mongo_db,
            mongo_collection=args.mongo_collection,
            checkpoint_location=args.checkpoint_location
        )
        
        logger.info("Starting Simple Music Recommender...")
        recommender.start()
        
    except KeyboardInterrupt:
        logger.info("Stopped by user")
    except Exception as e:
        logger.error(f"Failed: {e}")
        raise

if __name__ == "__main__":
    main()