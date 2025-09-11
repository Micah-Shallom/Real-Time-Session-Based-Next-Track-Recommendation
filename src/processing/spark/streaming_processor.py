#!/usr/bin/env python3
"""
Simplified Spark Streaming Music Recommendation System
Focuses on basic functionality without complex UDFs
Now includes integrated recommendation engine
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

# Import recommendation engine
from recommendation_engine import generate_recommendations

# Setup logging with more detailed format
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(name)s: %(message)s'
)
logger = logging.getLogger(__name__)

class SimpleMusicRecommender:
    """Simplified music recommendation system with integrated recommendation engine"""
    
    def __init__(self, 
             kafka_servers: str = "localhost:9092",
             kafka_topic: str = "user-tracks",
             mongo_uri: str = "mongodb://localhost:27017",
             mongo_db: str = "recommendations",
             mongo_sessions_collection: str = "user_sessions",
             mongo_recommendations_collection: str = "user_recommendations"):
    
        self.kafka_servers = kafka_servers
        self.kafka_topic = kafka_topic
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
        self.mongo_sessions_collection = mongo_sessions_collection
        self.mongo_recommendations_collection = mongo_recommendations_collection
        
        # Initialize Spark
        self.spark = self._create_spark_session()
        self.mongo_client = None
        
        # NEW: Initialize variable to store enriched sessions
        self.current_enriched_sessions = None
        
        logger.info("Simplified recommender initialized with kafka_servers=%s, topic=%s, mongo_uri=%s", 
                    kafka_servers, kafka_topic, mongo_uri)
        logger.info("MongoDB collections: sessions=%s, recommendations=%s", 
                    mongo_sessions_collection, mongo_recommendations_collection)
    
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
        """
        Enhanced event processing with recommendation generation
        Pipeline: Kafka -> Parse Events -> Generate Recommendations -> Store Both
        """
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
        return events, sessions
    
    def _write_sessions_to_mongodb(self, df, epoch_id):
        """Write session data to MongoDB (original functionality)"""
        try:
            pandas_df = df.toPandas()
            if pandas_df.empty:
                return
                
            if not self.mongo_client:
                self.mongo_client = MongoClient(self.mongo_uri, maxPoolSize=10)
            
            collection = self.mongo_client[self.mongo_db][self.mongo_sessions_collection]
            
            chunk_size = 100
            total_records = 0
            
            for i in range(0, len(pandas_df), chunk_size):
                chunk = pandas_df.iloc[i:i+chunk_size]
                records = chunk.to_dict('records')
                
                if records:
                    collection.insert_many(records, ordered=False)  # Faster unordered inserts
                    total_records += len(records)
            
            logger.info(f"Sessions Epoch {epoch_id}: Inserted {total_records} session records")
        
        except Exception as e:
            logger.error(f"MongoDB sessions write error epoch {epoch_id}: {e}")
    
    def _generate_and_write_recommendations(self, events_df, epoch_id):
        """
        ⚡ RECOMMENDATION ENGINE INTEGRATION POINT ⚡
        Generate recommendations from events and write to MongoDB
        """
        try:
            logger.info(f"Recommendations Epoch {epoch_id}: Starting recommendation generation for {events_df.count()} events")
            
            # 🎯 CALL RECOMMENDATION ENGINE HERE
            recommendations_df = generate_recommendations(events_df, self.spark)
            
            rec_count = recommendations_df.count()
            if rec_count == 0:
                logger.info(f"Recommendations Epoch {epoch_id}: No recommendations generated")
                return
            
            logger.info(f"Recommendations Epoch {epoch_id}: Generated {rec_count} recommendations")
            
            # Convert to pandas for MongoDB write
            pandas_df = recommendations_df.toPandas()
            
            if not self.mongo_client:
                self.mongo_client = MongoClient(self.mongo_uri, maxPoolSize=10)
            
            collection = self.mongo_client[self.mongo_db][self.mongo_recommendations_collection]
            
            # Write recommendations in chunks (append mode)
            chunk_size = 50  # Smaller chunks for recommendation data
            total_records = 0
            
            for i in range(0, len(pandas_df), chunk_size):
                chunk = pandas_df.iloc[i:i+chunk_size]
                records = chunk.to_dict('records')
                
                if records:
                    # Use insert_many for append mode (no upserts)
                    collection.insert_many(records, ordered=False)
                    total_records += len(records)
            
            logger.info(f"Recommendations Epoch {epoch_id}: Successfully stored {total_records} recommendations in MongoDB")
            
            # Log sample recommendations for monitoring
            if rec_count > 0:
                sample = recommendations_df.limit(1).collect()[0]
                logger.info(f"Recommendations Epoch {epoch_id}: Sample - User: {sample.user_id}, "
                           f"Next track: {sample.next_track_recommendation.track if sample.next_track_recommendation else 'None'}, "
                           f"Strategy: {sample.next_track_recommendation.strategy if sample.next_track_recommendation else 'None'}")
        
        except Exception as e:
            logger.error(f"Recommendations Epoch {epoch_id}: Error generating/storing recommendations: {e}")
    
    # def _combined_processing(self, events_df, epoch_id):
    #     """
    #     Combined processing function that handles both sessions and recommendations
    #     This is called by foreachBatch to process each micro-batch
    #     """
    #     try:
    #         # Generate session aggregations (existing logic)
    #         sessions = events_df \
    #             .withWatermark("event_time", "1 minute") \
    #             .groupBy(
    #                 window(col("event_time"), "2 minutes"),
    #                 col("session_id"),
    #                 col("user_id")
    #             ).agg(
    #                 collect_list(col("track_id")).alias("tracks"),
    #                 count("*").alias("track_count"),
    #                 first(col("artist")).alias("first_artist"),
    #                 last(col("track_id")).alias("last_track")
    #             ).withColumn(
    #                 "processing_time", current_timestamp()
    #             ).withColumn(
    #                 "has_multiple_tracks", 
    #                 when(col("track_count") > 1, True).otherwise(False)
    #             )
            
    #         # Write sessions to MongoDB (original functionality)
    #         self._write_sessions_to_mongodb(sessions, epoch_id)
            
    #         # Generate and write recommendations (new functionality)
    #         self._generate_and_write_recommendations(events_df, epoch_id)
            
    #     except Exception as e:
    #         logger.error(f"Combined processing error epoch {epoch_id}: {e}")

    def _combined_processing(self, events_df, epoch_id):
        """
        Combined processing function that handles both sessions and recommendations
        This is called by foreachBatch to process each micro-batch
        """
        try:
            # Generate session aggregations (existing logic)
            sessions = events_df \
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
            
            # Write sessions to MongoDB (original functionality)
            self._write_sessions_to_mongodb(sessions, epoch_id)
            
            # 🎯 NEW: Generate recommendations and JOIN with sessions
            recommendations_df = generate_recommendations(events_df, self.spark)
            
            # Create enriched sessions with recommendations for console display
            if recommendations_df.count() > 0:
                # Join sessions with recommendations on user_id
                enriched_sessions = sessions.join(
                    recommendations_df.select(
                        col("user_id"),
                        col("next_track_recommendation.track").alias("recommended_track"),
                        col("next_track_recommendation.strategy").alias("rec_strategy")
                    ),
                    on="user_id",
                    how="left"  # Left join to keep all sessions
                )
                
                # Store enriched sessions in a class variable for console output
                self.current_enriched_sessions = enriched_sessions
            else:
                # If no recommendations, add null columns for consistency
                enriched_sessions = sessions.withColumn("recommended_track", lit(None).cast(StringType())) \
                                        .withColumn("rec_strategy", lit(None).cast(StringType()))
                self.current_enriched_sessions = enriched_sessions
            
            # Write recommendations to MongoDB (existing functionality)
            if recommendations_df.count() > 0:
                pandas_df = recommendations_df.toPandas()
                
                if not self.mongo_client:
                    self.mongo_client = MongoClient(self.mongo_uri, maxPoolSize=10)
                
                collection = self.mongo_client[self.mongo_db][self.mongo_recommendations_collection]
                
                chunk_size = 50
                total_records = 0
                
                for i in range(0, len(pandas_df), chunk_size):
                    chunk = pandas_df.iloc[i:i+chunk_size]
                    records = chunk.to_dict('records')
                    
                    if records:
                        collection.insert_many(records, ordered=False)
                        total_records += len(records)
                
                logger.info(f"Recommendations Epoch {epoch_id}: Successfully stored {total_records} recommendations in MongoDB")
            
        except Exception as e:
            logger.error(f"Combined processing error epoch {epoch_id}: {e}")

    
    def start(self):
        """Start streaming with integrated recommendations"""
        try:
            # Create stream
            kafka_stream = self._create_stream()
            logger.info("Created Kafka stream")
            
            # Process events and get both events and sessions
            events, sessions = self._process_events(kafka_stream)
            logger.info("Processing events...")
            
            # 🚀 MAIN PIPELINE: Process events with recommendations
            main_query = events.writeStream \
                .outputMode("append") \
                .foreachBatch(self._combined_processing) \
                .trigger(processingTime='20 seconds') \
                .start()
            logger.info("Started main processing stream (sessions + recommendations): %s", main_query.id)
            
            # Console output for monitoring (sessions only)
            console = sessions.select(
                col("session_id"), 
                col("user_id"), 
                col("track_count"),
                col("last_track")
            ).writeStream \
                .outputMode("update") \
                .foreachBatch(self._write_enriched_to_console) \
                .trigger(processingTime='15 seconds') \
                .start()
            logger.info("Started console output stream: %s", console.id)
            
            logger.info("🎵 Streaming started with recommendations - waiting for data...")
            logger.info("📊 Writing sessions to: %s.%s", self.mongo_db, self.mongo_sessions_collection)
            logger.info("🎯 Writing recommendations to: %s.%s", self.mongo_db, self.mongo_recommendations_collection)
            
            main_query.awaitTermination()
            
        except Exception as e:
            logger.error(f"Streaming failed: {e}")
            raise
        finally:
            if self.mongo_client:
                self.mongo_client.close()
            self.spark.stop()
            logger.info("Spark streaming stopped")

    def _write_enriched_to_console(self, batch_df, batch_id):
        """Write enriched session data (with recommendations) to console"""
        try:
            if self.current_enriched_sessions is not None:
                # Show enriched data with recommendations
                enriched_display = self.current_enriched_sessions.select(
                    col("session_id"),
                    col("user_id"), 
                    col("track_count"),
                    col("last_track"),
                    col("recommended_track").alias("next_recommendation"),
                    col("rec_strategy").alias("strategy")
                )
                
                print(f"\n=== BATCH {batch_id} - Sessions with Recommendations ===")
                enriched_display.show(50, truncate=False)
            else:
                # Fallback to regular session data
                print(f"\n=== BATCH {batch_id} - Sessions (No Recommendations Yet) ===")
                batch_df.show(50, truncate=False)
                
        except Exception as e:
            logger.error(f"Console output error batch {batch_id}: {e}")
            # Fallback to basic display
            batch_df.show(50, truncate=False)

    


def main():
    """Main function with updated parameters"""
    parser = argparse.ArgumentParser(description='Simple Music Recommender with Recommendations')
    parser.add_argument('--kafka-servers', default='localhost:9092')
    parser.add_argument('--kafka-topic', default='user-tracks')
    parser.add_argument('--mongo-uri', default='mongodb://localhost:27017')
    parser.add_argument('--mongo-db', default='recommendations')
    parser.add_argument('--mongo-sessions-collection', default='user_sessions')
    parser.add_argument('--mongo-recommendations-collection', default='user_recommendations')
    
    args = parser.parse_args()
    
    try:
        recommender = SimpleMusicRecommender(
            kafka_servers=args.kafka_servers,
            kafka_topic=args.kafka_topic,
            mongo_uri=args.mongo_uri,
            mongo_db=args.mongo_db,
            mongo_sessions_collection=args.mongo_sessions_collection,
            mongo_recommendations_collection=args.mongo_recommendations_collection
        )
        
        logger.info("Starting Simple Music Recommender with Integrated Recommendations...")
        recommender.start()
        
    except KeyboardInterrupt:
        logger.info("Stopped by user")
    except Exception as e:
        logger.error(f"Failed: {e}")
        raise

if __name__ == "__main__":
    main()