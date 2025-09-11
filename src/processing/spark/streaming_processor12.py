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

# Setup logging with more detailed format
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(name)s: %(message)s'
)
logger = logging.getLogger(__name__)

class SimpleMusicRecommender:
    """Simplified music recommendation system"""
    
    def __init__(self, 
                 kafka_servers: str = "localhost:9092",
                 kafka_topic: str = "user-tracks",
                 mongo_uri: str = "mongodb://localhost:27017",
                 mongo_db: str = "recommendations",
                 mongo_collection: str = "user_sessions"):
        
        self.kafka_servers = kafka_servers
        self.kafka_topic = kafka_topic
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
        self.mongo_collection = mongo_collection
        
        # Initialize Spark
        self.spark = self._create_spark_session()
        self.mongo_client = None
        
        logger.info("Simplified recommender initialized with kafka_servers=%s, topic=%s, mongo_uri=%s", 
                    kafka_servers, kafka_topic, mongo_uri)
    
    def _create_spark_session(self) -> SparkSession:
        """Create minimal Spark session with reduced logging"""
        spark = SparkSession.builder \
            .appName("SimpleMusicRecommender") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.streaming.stopGracefullyOnShutdown", "true") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .config("spark.log.level", "WARN").getOrCreate()
        spark.sparkContext.setLogLevel("WARN")  # Change from DEBUG to WARN
        logger.info("Spark session created with appName=SimpleMusicRecommender")
        return spark
    
    def _get_schema(self) -> StructType:
        """Simple event schema"""
        schema = StructType([
            StructField("user_id", StringType(), True),
            StructField("track_id", StringType(), True),
            StructField("session_id", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("artist", StringType(), True),
            StructField("song_title", StringType(), True)
        ])
        logger.info("Defined schema: %s", schema.json())
        return schema
    
    def _create_stream(self):
        """Create simple Kafka stream"""
        kafka_stream = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_servers) \
            .option("subscribe", self.kafka_topic) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
        logger.info("Created Kafka stream for topic=%s, servers=%s", self.kafka_topic, self.kafka_servers)
        return kafka_stream
    
    def _process_events(self, kafka_df):
        """Simple event processing without complex UDFs"""
        schema = self._get_schema()
        
        # Parse JSON
        events = kafka_df.select(
            from_json(col("value").cast("string"), schema).alias("data")
        ).select("data.*")
        
        # Log event count and sample events
        def log_events(batch_df, batch_id):
            event_count = batch_df.count()
            logger.info("Batch %s: Consumed %d events from Kafka", batch_id, event_count)
            if event_count > 0:
                sample_events = batch_df.limit(3).collect()  # Log up to 3 events
                for i, event in enumerate(sample_events, 1):
                    logger.info("Batch %s: Sample event %d: user_id=%s, track_id=%s, session_id=%s, event_type=%s, artist=%s, song_title=%s, timestamp=%s", 
                                 batch_id, i, event.user_id, event.track_id, event.session_id, 
                                 event.event_type, event.artist, event.song_title, event.timestamp)
        
        # Apply logging to each micro-batch
        events.writeStream \
            .foreachBatch(log_events) \
            .trigger(processingTime='5 seconds') \
            .start()  # Start a separate query for logging
        
        # Convert timestamp and filter
        events = events.withColumn(
            "event_time",
            to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS")
        ).filter(col("event_time").isNotNull() & (col("event_type") == "play"))
        
        # Log filtered events
        def log_filtered_events(batch_df, batch_id):
            filtered_count = batch_df.count()
            logger.info("Batch %s: Processed %d play events after filtering", batch_id, filtered_count)
        
        events.writeStream \
            .foreachBatch(log_filtered_events) \
            .trigger(processingTime='10 seconds') \
            .start()  # Start a separate query for logging filtered events
        
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
        
        logger.info("Completed event processing setup")
        return sessions
    
    def _write_to_mongodb(self, df, epoch_id):
        try:
            pandas_df = df.toPandas()
            if pandas_df.empty:
                return
                
            if not self.mongo_client:
                self.mongo_client = MongoClient(self.mongo_uri, maxPoolSize=10)
            
            collection = self.mongo_client[self.mongo_db][self.mongo_collection]
            
            chunk_size = 100
            total_records = 0
            
            for i in range(0, len(pandas_df), chunk_size):
                chunk = pandas_df.iloc[i:i+chunk_size]
                records = chunk.to_dict('records')
                
                if records:
                    collection.insert_many(records, ordered=False)  # Faster unordered inserts
                    total_records += len(records)
            
            logger.info(f"Epoch {epoch_id}: Inserted {total_records} records")
        
        except Exception as e:
            logger.error(f"MongoDB error epoch {epoch_id}: {e}")
    
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
                .foreachBatch(self._write_to_mongodb) \
                .trigger(processingTime='20 seconds') \
                .start()
            logger.info("Started MongoDB write stream: %s", query.id)
            
            # Console output for monitoring
            console = processed.select(
                col("session_id"), 
                col("user_id"), 
                col("track_count"),
                col("last_track")
            ).writeStream \
                .outputMode("update") \
                .format("console") \
                .option("numRows", 50) \
                .trigger(processingTime='10 seconds') \
                .start()
            logger.info("Started console output stream: %s", console.id)
            
            logger.info("Streaming started - waiting for data...")
            query.awaitTermination()
            
        except Exception as e:
            logger.error(f"Streaming failed: {e}")
            raise
        finally:
            if self.mongo_client:
                self.mongo_client.close()
            self.spark.stop()
            logger.info("Spark streaming stopped")

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Simple Music Recommender')
    parser.add_argument('--kafka-servers', default='localhost:9092')
    parser.add_argument('--kafka-topic', default='user-tracks')
    parser.add_argument('--mongo-uri', default='mongodb://localhost:27017')
    parser.add_argument('--mongo-db', default='recommendations')
    parser.add_argument('--mongo-collection', default='user_sessions')
    
    args = parser.parse_args()
    
    try:
        recommender = SimpleMusicRecommender(
            kafka_servers=args.kafka_servers,
            kafka_topic=args.kafka_topic,
            mongo_uri=args.mongo_uri,
            mongo_db=args.mongo_db,
            mongo_collection=args.mongo_collection
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
