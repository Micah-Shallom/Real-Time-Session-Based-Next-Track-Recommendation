#!/usr/bin/env python3
"""
Music Recommendation Engine for Spark Streaming
Implements popularity-based and co-occurrence-based recommendations
"""

import logging
from typing import Dict, List, Tuple
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, count, collect_list, explode, struct, array, lit, 
    max as spark_max, sum as spark_sum, when, size, 
    row_number, desc, asc, current_timestamp, last
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    ArrayType, TimestampType
)
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)

class MusicRecommendationEngine:
    """
    Lightweight music recommendation engine using Spark DataFrames
    Implements popularity-based and co-occurrence-based strategies
    """
    
    def __init__(self, top_n: int = 5):
        self.top_n = top_n
        logger.info(f"Initialized MusicRecommendationEngine with top_n={top_n}")
    
    def _calculate_popularity_recommendations(self, events_df: DataFrame) -> DataFrame:
        """
        Calculate popularity-based recommendations
        Returns: DataFrame with columns [track_id, popularity_weight]
        """
        logger.info("Calculating popularity-based recommendations")
        
        # Count total plays per track across all events
        track_counts = events_df.groupBy("track_id") \
            .agg(count("*").alias("play_count"))
        
        # Calculate total events for normalization
        total_events = events_df.count()
        
        if total_events == 0:
            logger.warning("No events found for popularity calculation")
            return events_df.spark.createDataFrame([], 
                StructType([
                    StructField("track_id", StringType(), False),
                    StructField("popularity_weight", DoubleType(), False)
                ])
            )
        
        # Calculate normalized popularity weights
        popularity_df = track_counts.withColumn(
            "popularity_weight", 
            col("play_count") / lit(total_events)
        ).select("track_id", "popularity_weight")
        
        # Get top N most popular tracks
        window = Window.orderBy(desc("popularity_weight"), asc("track_id"))
        top_popular = popularity_df \
            .withColumn("rank", row_number().over(window)) \
            .filter(col("rank") <= self.top_n) \
            .drop("rank")
        
        logger.info(f"Calculated popularity recommendations for {top_popular.count()} tracks")
        return top_popular
    
    def _calculate_cooccurrence_recommendations(self, events_df: DataFrame) -> DataFrame:
        """
        Calculate co-occurrence-based recommendations
        Returns: DataFrame with columns [last_track, track_id, cooccurrence_weight]
        """
        logger.info("Calculating co-occurrence-based recommendations")
        
        # Group events by session to get track sequences
        session_tracks = events_df.groupBy("session_id", "user_id") \
            .agg(
                collect_list("track_id").alias("tracks"),
                last("track_id").alias("last_track")
            ).filter(size(col("tracks")) > 1)  # Only sessions with multiple tracks
        
        if session_tracks.count() == 0:
            logger.warning("No multi-track sessions found for co-occurrence calculation")
            return events_df.spark.createDataFrame([], 
                StructType([
                    StructField("last_track", StringType(), False),
                    StructField("track_id", StringType(), False),
                    StructField("cooccurrence_weight", DoubleType(), False)
                ])
            )
        
        # Create pairs of tracks that co-occurred in the same session
        # This creates a cartesian product of tracks within each session
        track_pairs = session_tracks.select(
            col("last_track"),
            explode(col("tracks")).alias("track_id")
        ).filter(col("last_track") != col("track_id"))  # Remove self-pairs
        
        # Count co-occurrences for each track pair
        cooccurrence_counts = track_pairs.groupBy("last_track", "track_id") \
            .agg(count("*").alias("cooccurrence_count"))
        
        # Calculate total co-occurrences per last_track for normalization
        total_cooccurrences = cooccurrence_counts.groupBy("last_track") \
            .agg(spark_sum("cooccurrence_count").alias("total_count"))
        
        # Join and calculate normalized weights
        cooccurrence_weights = cooccurrence_counts.join(
            total_cooccurrences, "last_track"
        ).withColumn(
            "cooccurrence_weight",
            col("cooccurrence_count") / col("total_count")
        ).select("last_track", "track_id", "cooccurrence_weight")
        
        # Get top N co-occurring tracks for each last_track
        window = Window.partitionBy("last_track") \
            .orderBy(desc("cooccurrence_weight"), asc("track_id"))
        
        top_cooccurrence = cooccurrence_weights \
            .withColumn("rank", row_number().over(window)) \
            .filter(col("rank") <= self.top_n) \
            .drop("rank")
        
        logger.info(f"Calculated co-occurrence recommendations for {top_cooccurrence.select('last_track').distinct().count()} tracks")
        return top_cooccurrence
    
    def _combine_recommendations(self, events_df: DataFrame, 
                               popularity_df: DataFrame, 
                               cooccurrence_df: DataFrame) -> DataFrame:
        """
        Combine recommendations and generate final output per user/session
        """
        logger.info("Combining recommendations")
        
        # Get user sessions with their last track
        user_sessions = events_df.groupBy("user_id", "session_id") \
            .agg(last("track_id").alias("last_track"))
        
        # Create popularity recommendations array for each session
        popularity_recs = popularity_df.select(
            struct(
                col("track_id").alias("track"),
                col("popularity_weight").alias("weight")
            ).alias("rec")
        ).agg(collect_list("rec").alias("popularity_recs"))
        
        # For each user session, get co-occurrence recommendations based on last track
        session_cooccurrence = user_sessions.join(
            cooccurrence_df,
            user_sessions.last_track == cooccurrence_df.last_track,
            "left"
        ).groupBy("user_id", "session_id", user_sessions.last_track) \
         .agg(
             collect_list(
                 struct(
                     col("track_id").alias("track"),
                     col("cooccurrence_weight").alias("weight")
                 )
             ).alias("cooccurrence_recs")
         )
        
        # Cross join with popularity to add to all sessions
        final_recommendations = session_cooccurrence.crossJoin(popularity_recs)
        
        # Determine next track recommendation (prioritize co-occurrence over popularity)
        final_recommendations = final_recommendations.withColumn(
            "next_track_recommendation",
            when(size(col("cooccurrence_recs")) > 0,
                 struct(
                     col("cooccurrence_recs")[0]["track"].alias("track"),
                     lit("cooccurrence").alias("strategy"),
                     col("cooccurrence_recs")[0]["weight"].alias("weight")
                 )
            ).otherwise(
                when(size(col("popularity_recs")) > 0,
                     struct(
                         col("popularity_recs")[0]["track"].alias("track"),
                         lit("popularity").alias("strategy"),
                         col("popularity_recs")[0]["weight"].alias("weight")
                     )
                ).otherwise(lit(None))
            )
        )
        
        # Create final recommendations structure
        recommendations_df = final_recommendations.select(
            col("user_id"),
            col("session_id"),
            col("last_track"),
            struct(
                col("popularity_recs").alias("popularity"),
                col("cooccurrence_recs").alias("cooccurrence")
            ).alias("recommendations"),
            col("next_track_recommendation"),
            current_timestamp().alias("timestamp")
        ).filter(col("next_track_recommendation").isNotNull())  # Only return sessions with recommendations
        
        logger.info(f"Generated combined recommendations for {recommendations_df.count()} user sessions")
        return recommendations_df
    
    def generate_recommendations(self, events_df: DataFrame, spark: SparkSession) -> DataFrame:
        """
        Main function to generate recommendations for a batch of events
        
        Args:
            events_df: DataFrame with schema [user_id, track_id, session_id, event_time, artist, song_title]
            spark: SparkSession instance
            
        Returns:
            DataFrame with recommendations schema as specified
        """
        logger.info(f"Starting recommendation generation for {events_df.count()} events")
        
        # Handle empty DataFrame
        if events_df.count() == 0:
            logger.info("No events to process, returning empty recommendations")
            return spark.createDataFrame([], self._get_output_schema())
        
        try:
            # Calculate popularity-based recommendations
            popularity_df = self._calculate_popularity_recommendations(events_df)
            
            # Calculate co-occurrence-based recommendations
            cooccurrence_df = self._calculate_cooccurrence_recommendations(events_df)
            
            # Combine recommendations
            final_df = self._combine_recommendations(events_df, popularity_df, cooccurrence_df)
            
            logger.info("Recommendation generation completed successfully")
            return final_df
            
        except Exception as e:
            logger.error(f"Error generating recommendations: {e}")
            # Return empty DataFrame with correct schema on error
            return spark.createDataFrame([], self._get_output_schema())
    
    def _get_output_schema(self) -> StructType:
        """Define the output schema for recommendations"""
        return StructType([
            StructField("user_id", StringType(), False),
            StructField("session_id", StringType(), False),
            StructField("last_track", StringType(), False),
            StructField("recommendations", StructType([
                StructField("popularity", ArrayType(StructType([
                    StructField("track", StringType(), False),
                    StructField("weight", DoubleType(), False)
                ])), True),
                StructField("cooccurrence", ArrayType(StructType([
                    StructField("track", StringType(), False),
                    StructField("weight", DoubleType(), False)
                ])), True)
            ]), False),
            StructField("next_track_recommendation", StructType([
                StructField("track", StringType(), False),
                StructField("strategy", StringType(), False),
                StructField("weight", DoubleType(), False)
            ]), True),
            StructField("timestamp", TimestampType(), False)
        ])


# Convenience function for easy import
def generate_recommendations(events_df: DataFrame, spark: SparkSession, top_n: int = 5) -> DataFrame:
    """
    Generate music recommendations for a batch of listening events
    
    Args:
        events_df: DataFrame with listening events
        spark: SparkSession instance
        top_n: Number of recommendations per strategy (default: 5)
        
    Returns:
        DataFrame with recommendations following the specified schema
    """
    engine = MusicRecommendationEngine(top_n=top_n)
    return engine.generate_recommendations(events_df, spark)


# Additional utility functions for monitoring and debugging
def get_recommendation_stats(recommendations_df: DataFrame) -> Dict[str, int]:
    """
    Get statistics about recommendations generated
    
    Args:
        recommendations_df: Output from generate_recommendations
        
    Returns:
        Dictionary with recommendation statistics
    """
    total_sessions = recommendations_df.count()
    
    popularity_sessions = recommendations_df.filter(
        col("next_track_recommendation.strategy") == "popularity"
    ).count()
    
    cooccurrence_sessions = recommendations_df.filter(
        col("next_track_recommendation.strategy") == "cooccurrence"
    ).count()
    
    return {
        "total_sessions": total_sessions,
        "popularity_recommendations": popularity_sessions,
        "cooccurrence_recommendations": cooccurrence_sessions,
        "coverage_rate": round((total_sessions / recommendations_df.count() if recommendations_df.count() > 0 else 0) * 100, 2)
    }


def print_sample_recommendations(recommendations_df: DataFrame, num_samples: int = 3):
    """
    Print sample recommendations for debugging/monitoring
    
    Args:
        recommendations_df: Output from generate_recommendations
        num_samples: Number of sample recommendations to print
    """
    samples = recommendations_df.limit(num_samples).collect()
    
    print(f"\n=== Sample Recommendations ({len(samples)} sessions) ===")
    for i, rec in enumerate(samples, 1):
        print(f"\nSession {i}:")
        print(f"  User: {rec.user_id}")
        print(f"  Session: {rec.session_id}")
        print(f"  Last Track: {rec.last_track}")
        print(f"  Next Recommendation: {rec.next_track_recommendation.track} ({rec.next_track_recommendation.strategy}, weight: {rec.next_track_recommendation.weight:.3f})")
        print(f"  Popularity Recs: {len(rec.recommendations.popularity)} tracks")
        print(f"  Co-occurrence Recs: {len(rec.recommendations.cooccurrence)} tracks")