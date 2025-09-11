#!/usr/bin/env python3


import json
import random
import time
import uuid
from datetime import datetime
from typing import Dict, List, Optional
from dataclasses import dataclass, asdict
import logging
from pathlib import Path
from kafka import KafkaProducer
import argparse

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class ListeningEvent:
    """Simplified listening event with SpotifyTrack alignment"""
    event_id: str
    user_id: str
    track_id: str
    artist: str
    song_title: str
    timestamp: str
    session_id: str
    event_type: str  # 'play' or 'skip'
    duration_played: int  # seconds
    genre: str
    emotion: str
    device_type: str
    
    def to_json(self) -> str:
        """Convert to JSON string for Kafka"""
        return json.dumps(asdict(self))

@dataclass 
class UserProfile:
    """Simplified user profile"""
    user_id: str
    preferred_genres: List[str]
    preferred_emotions: List[str]
    skip_rate: float  # 0.0 to 1.0
    recent_tracks: List[str]  # Last 5 tracks to avoid repetition

class SimpleKafkaProducer:
    """Simplified Kafka producer integrated with Spotify 900K dataset"""
    
    def __init__(self, 
                 bootstrap_servers: str = 'localhost:9092',
                 topic: str = 'user-tracks',
                 data_path: str = './data/processed',
                 max_tracks: int = 500000):
        
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.data_path = Path(data_path)
        self.max_tracks = max_tracks
        
        # Simple Kafka producer configuration
        self.producer = KafkaProducer(
            bootstrap_servers=[bootstrap_servers],
            value_serializer=lambda v: v.encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            retries=3,
            acks=1,
            batch_size=65536,
            linger_ms=5
        )
        
        # Load tracks from multiple chunks
        self.tracks_data = self.load_sampled_tracks()
        
        # Load genre and emotion mappings for user profiles
        self.genre_mapping, self.emotion_mapping = self.load_mappings()
        
        # Generate user profiles based on dataset
        self.user_profiles = self.generate_realistic_users(20)
        
        # Statistics
        self.events_sent = 0
        
        logger.info(f"Initialized with {len(self.tracks_data)} tracks and {len(self.user_profiles)} users")
    
    def load_mappings(self) -> tuple[Dict, Dict]:
        """Load genre and emotion mappings from processed data"""
        mappings_file = self.data_path / 'feature_mappings.json'
        if mappings_file.exists():
            with open(mappings_file, 'r') as f:
                data = json.load(f)
                return data['genres'], data['emotions']
        return {}, {}
    
    def load_sampled_tracks(self) -> List[Dict]:
        """Load sampled tracks from all chunks up to max_tracks"""
        tracks = []
        chunk_files = list(self.data_path.glob('tracks_chunk_*.json'))
        
        if not chunk_files:
            logger.warning("No chunk files found, creating sample data")
            return self.create_sample_tracks(self.max_tracks)
        
        # Sample across chunks
        random.shuffle(chunk_files)
        tracks_per_chunk = self.max_tracks // len(chunk_files) + 1
        
        for chunk_file in chunk_files:
            try:
                with open(chunk_file, 'r') as f:
                    chunk_tracks = json.load(f)
                    sampled = random.sample(chunk_tracks, min(tracks_per_chunk, len(chunk_tracks)))
                    tracks.extend(sampled)
                    if len(tracks) >= self.max_tracks:
                        break
            except Exception as e:
                logger.warning(f"Could not load chunk {chunk_file}: {e}")
        
        # Trim to max_tracks if exceeded
        if len(tracks) > self.max_tracks:
            tracks = tracks[:self.max_tracks]
        
        logger.info(f"Loaded {len(tracks)} tracks from {len(chunk_files)} chunks")
        return tracks
    
    def create_sample_tracks(self, num_tracks: int) -> List[Dict]:
        """Create sample tracks if no data files available"""
        genres = ['pop', 'rock', 'hip-hop', 'electronic', 'jazz', 'classical']  # Match your data
        emotions = ['happy', 'sad', 'energetic', 'calm']  # Match your data
        
        tracks = []
        for i in range(num_tracks):
            track = {
                'track_id': f'track_{uuid.uuid4().hex[:8]}',
                'artist': f'Artist {i % 5}',
                'song_title': f'Song {i+1}',
                'genre': random.choice(genres),
                'emotion': random.choice(emotions),
                'duration_seconds': random.randint(120, 300)
            }
            tracks.append(track)
        
        logger.info(f"Created {len(tracks)} sample tracks")
        return tracks
    
    def generate_realistic_users(self, no_users) -> List[UserProfile]:
        """Generate user profiles based on dataset distribution"""
        if not self.tracks_data:
            raise ValueError("No tracks loaded to derive user profiles.")
        
        # Derive from track popularity or stats if available
        stats_file = self.data_path / 'dataset_stats.json'
        profiles = []
        all_genres = list(self.genre_mapping.keys())
        all_emotions = list(self.emotion_mapping.keys())
        
        if stats_file.exists():
            with open(stats_file, 'r') as f:
                stats = json.load(f)
                # Assume stats include average skip rate or genre popularity
                avg_skip_rate = stats.get('average_skip_rate', 0.2)  # Default if not present
        else:
            avg_skip_rate = 0.2  # Fallback if no stats
        
        # Create pseudo-users based on genre/emotion distribution
        for i in range(no_users):  
            user_id = f"user_{i+1:03d}"
            # Sample genres and emotions weighted by frequency in tracks_data
            genre_counts = {}
            emotion_counts = {}
            for track in self.tracks_data:
                genre_counts[track['genre']] = genre_counts.get(track['genre'], 0) + 1
                emotion_counts[track['emotion']] = emotion_counts.get(track['emotion'], 0) + 1
            
            total_genres = sum(genre_counts.values())
            total_emotions = sum(emotion_counts.values())
            preferred_genres = random.choices(all_genres, weights=[genre_counts.get(g, 1) for g in all_genres], k=2)
            preferred_emotions = random.choices(all_emotions, weights=[emotion_counts.get(e, 1) for e in all_emotions], k=1)
            
            profile = UserProfile(
                user_id=user_id,
                preferred_genres=preferred_genres,
                preferred_emotions=preferred_emotions,
                skip_rate=avg_skip_rate,  # Use dataset-derived rate
                recent_tracks=[]  # Initialize empty; populate from data if available
            )
            profiles.append(profile)
        
        return profiles
    
    def select_track_for_user(self, user_profile: UserProfile) -> Dict:
        """Select track based on genre and emotion preferences"""
        # Filter by preferred genres and emotions
        preferred_tracks = [
            track for track in self.tracks_data 
            if track['genre'] in user_profile.preferred_genres or track['emotion'] in user_profile.preferred_emotions
        ]
        
        # If no preferred tracks, use all tracks
        if not preferred_tracks:
            preferred_tracks = self.tracks_data
        
        # Avoid recently played tracks
        available_tracks = [
            track for track in preferred_tracks 
            if track['track_id'] not in user_profile.recent_tracks
        ]
        
        # If all preferred tracks were recent, allow recent tracks
        if not available_tracks:
            available_tracks = preferred_tracks
        
        return random.choice(available_tracks)
    
    def simulate_listening_event(self, user_profile: UserProfile, session_id: str) -> ListeningEvent:
        """Simulate a listening event using dataset-derived behavior"""
        track = self.select_track_for_user(user_profile)
        
        # Derive skip behavior from track or user stats if available
        stats_file = self.data_path / 'dataset_stats.json'
        if stats_file.exists():
            with open(stats_file, 'r') as f:
                stats = json.load(f)
                avg_skip_rate = stats.get('average_skip_rate', user_profile.skip_rate)
                avg_play_duration = stats.get('average_play_duration', track.get('duration_seconds', 180))
        else:
            avg_skip_rate = user_profile.skip_rate
            avg_play_duration = track.get('duration_seconds', 180)
        
        will_skip = random.random() < avg_skip_rate
        
        if will_skip:
            duration_played = random.randint(5, min(30, avg_play_duration))  # Short skip
            event_type = 'skip'
        else:
            duration_played = random.randint(int(avg_play_duration * 0.7), avg_play_duration)  # 70%-100% play
            event_type = 'play'
        
        user_profile.recent_tracks.append(track['track_id'])
        if len(user_profile.recent_tracks) > 5:
            user_profile.recent_tracks = user_profile.recent_tracks[-5:]
        
        device_types = ['mobile', 'desktop', 'tablet']  # Could expand with dataset info
        device_type = random.choice(device_types)
        
        event = ListeningEvent(
            event_id=f"event_{uuid.uuid4().hex[:8]}",
            user_id=user_profile.user_id,
            track_id=track['track_id'],
            artist=track['artist'],
            song_title=track['song_title'],
            timestamp=datetime.now().isoformat(),
            session_id=session_id,
            event_type=event_type,
            duration_played=duration_played,
            genre=track['genre'],
            emotion=track.get('emotion', 'neutral'),
            device_type=device_type
        )
        
        return event
    
    def send_event(self, event: ListeningEvent):
        """Send single event to Kafka"""
        try:
            self.producer.send(
                self.topic,
                key=event.user_id,
                value=event.to_json()
            )
            
            self.events_sent += 1
            
            if self.events_sent % 50 == 0:
                logger.info(f"Sent {self.events_sent} events")
                
        except Exception as e:
            logger.error(f"Failed to send event {event.event_id}: {e}")
    
    def start_streaming(self, 
                       events_per_second: int = 100,
                       duration_minutes: int = None):
        """Start simple streaming"""
        
        logger.info(f"Starting streaming: {events_per_second} events/sec")
        
        start_time = datetime.now()
        sleep_interval = 1.0 / events_per_second
        
        try:
            while True:
                # Check duration limit
                if duration_minutes:
                    elapsed = (datetime.now() - start_time).total_seconds()
                    if elapsed > duration_minutes * 60:
                        logger.info("Duration reached, stopping...")
                        break
                
                # Pick random user and generate event
                user_profile = random.choice(self.user_profiles)
                session_id = f"session_{uuid.uuid4().hex[:8]}"
                
                event = self.simulate_listening_event(user_profile, session_id)
                self.send_event(event)
                
                # Wait before next event
                time.sleep(sleep_interval)
                
        except KeyboardInterrupt:
            logger.info("Stopping due to keyboard interrupt...")
        except Exception as e:
            logger.error(f"Streaming error: {e}")
        finally:
            self.producer.flush()
            self.producer.close()
            logger.info(f"Streaming completed. Total events: {self.events_sent}")
    
    def send_batch(self, num_events: int = 100):
        """Send a batch of events for testing"""
        logger.info(f"Sending batch of {num_events} events...")
        
        for i in range(num_events):
            user_profile = random.choice(self.user_profiles)
            session_id = f"session_{uuid.uuid4().hex[:8]}"
            
            event = self.simulate_listening_event(user_profile, session_id)
            self.send_event(event)
            
            if i % 10 == 0:
                time.sleep(0.1)
        
        self.producer.flush()
        logger.info(f"Batch completed: {num_events} events sent")

def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(description='Simplified Spotify Kafka Producer')
    parser.add_argument('--bootstrap-servers', default='localhost:9092', 
                       help='Kafka bootstrap servers')
    parser.add_argument('--topic', default='user-tracks', 
                       help='Kafka topic name')
    parser.add_argument('--data-path', default='./data/processed', 
                       help='Path to processed data directory')
    parser.add_argument('--events-per-second', type=int, default=10,
                       help='Target events per second')
    parser.add_argument('--duration', type=int, default=None,
                       help='Streaming duration in minutes')
    parser.add_argument('--batch-mode', action='store_true',
                       help='Send batch instead of streaming')
    parser.add_argument('--batch-size', type=int, default=100,
                       help='Batch size')
    parser.add_argument('--max-tracks', type=int, default=500000,
                       help='Maximum number of tracks to load')
    
    args = parser.parse_args()
    
    try:
        producer = SimpleKafkaProducer(
            bootstrap_servers=args.bootstrap_servers,
            topic=args.topic,
            data_path=args.data_path,
            max_tracks=args.max_tracks
        )
        
        if args.batch_mode:
            producer.send_batch(args.batch_size)
        else:
            producer.start_streaming(
                events_per_second=args.events_per_second,
                duration_minutes=args.duration
            )
        
        print(f"\nTotal events sent: {producer.events_sent}")
        
    except Exception as e:
        logger.error(f"Producer failed: {e}")
        raise

if __name__ == "__main__":
    main()