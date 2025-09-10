#!/usr/bin/env python3
"""
Spotify 900K Dataset Processor and Bootstrap
Handles preprocessing of the 900K Spotify dataset (JSON) for real-time recommendations
"""

import pandas as pd
import json
import hashlib
import logging
import os
from typing import Dict, List, Optional
from dataclasses import dataclass, asdict
from pathlib import Path
import re

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class SpotifyTrack:
    """Data class for Spotify track representation"""
    track_id: str
    artist: str
    song_title: str
    emotion: str
    genre: str
    tempo: float
    key: int
    loudness: float
    contextual_tags: List[str]
    similar_tracks: List[Dict[str, float]]  # [{"track_id": "...", "similarity": 0.85}]
    audio_features: Dict[str, float]
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON serialization"""
        return asdict(self)

class SpotifyDataProcessor:
    """Main processor for Spotify 900K dataset (JSON)"""
    
    def __init__(self, data_path: str = "./data", chunk_size: int = 10000):
        self.data_path = Path(data_path)
        self.raw_path = self.data_path / "raw"
        self.processed_path = self.data_path / "processed"
        self.chunk_size = chunk_size
        
        # Create directories
        self.raw_path.mkdir(parents=True, exist_ok=True)
        self.processed_path.mkdir(parents=True, exist_ok=True)
        
        # Track metadata cache
        self.track_cache = {}
        self.genre_mapping = {}
        self.emotion_mapping = {}
        
        # Key mapping (MIDI standard: C=0, C#=1, ..., B=11, Maj/Min as offset)
        self.key_mapping = {
            'C': 0, 'C#': 1, 'Db': 1, 'D': 2, 'D#': 3, 'Eb': 3,
            'E': 4, 'F': 5, 'F#': 6, 'Gb': 6, 'G': 7, 'G#': 8,
            'Ab': 8, 'A': 9, 'A#': 10, 'Bb': 10, 'B': 11
        }
    
    def generate_track_id(self, artist: str, song_title: str) -> str:
        """Generate consistent track ID from artist and song title"""
        clean_artist = re.sub(r'[^\w\s-]', '', str(artist).lower().strip())
        clean_title = re.sub(r'[^\w\s-]', '', str(song_title).lower().strip())
        combined = f"{clean_artist}||{clean_title}"
        return f"track_{hashlib.md5(combined.encode()).hexdigest()[:12]}"
    
    def parse_contextual_tags(self, tags) -> List[str]:
        """Parse contextual tags from string, list, or Series"""
        # Handle None or empty cases first
        if isinstance(tags, (list, tuple)):
            return [str(tag).strip().lower() for tag in tags if pd.notna(tag) and str(tag).strip()]

        if isinstance(tags, (pd.Series, pd.DataFrame)):
            tags = tags.dropna().tolist()
            return [str(tag).strip().lower() for tag in tags if str(tag).strip()]

        
        # Handle pandas Series
        if isinstance(tags, pd.Series):
            if tags.empty:
                return []
            tags = tags.iloc[0]
        
        # Handle list case
        if isinstance(tags, list):
            if not tags:  # Empty list
                return []
            return [str(tag).strip().lower() for tag in tags if tag]
        
        # Handle string case
        if isinstance(tags, str):
            if not tags.strip():
                return []
            return [tag.strip().lower() for tag in re.split(r'[,;|]', tags) if tag.strip()]
        
        # Default case
        return []
    
    def parse_similar_tracks(self, similar_data: Optional[List[Dict]]) -> List[Dict[str, float]]:
        """Parse similar tracks from JSON list of dicts or Series"""
        if similar_data is None:
            return []
        if isinstance(similar_data, (list, dict)) and len(similar_data) == 0:
            return []
        if isinstance(similar_data, (pd.Series, pd.DataFrame)) and similar_data.empty:
            return []

        if isinstance(similar_data, pd.Series):
            similar_data = similar_data.iloc[0] if not similar_data.empty else []
        similar_tracks = []
        for item in similar_data[:3]:  # Limit to 3
            if isinstance(item, dict) and 'track_id' in item and 'similarity' in item:
                similar_id = self.generate_track_id("unknown", item['track_id'])
                similar_tracks.append({
                    "track_id": similar_id,
                    "similarity": float(item['similarity'])
                })
            elif isinstance(item, str):
                similar_id = self.generate_track_id("unknown", item.strip())
                similar_tracks.append({"track_id": similar_id, "similarity": 0.8})
        return similar_tracks
    
    def extract_audio_features(self, row: Dict) -> Dict[str, float]:
        """Extract audio features from row"""
        audio_features = {}
        feature_columns = ['Tempo', 'Key', 'Loudness (db)', 'Energy', 'Danceability']
        for feature in feature_columns:
            if feature in row and row[feature] is not None:
                try:
                    if feature == 'Key':
                        key_str = str(row[feature]).strip()
                        key_base = re.match(r'([A-G]#?|b)', key_str, re.IGNORECASE)
                        key_mod = re.search(r'(Maj|min)', key_str, re.IGNORECASE)
                        key_value = 0
                        if key_base:
                            base = key_base.group(0).upper().replace('B', 'Bb')
                            key_value = self.key_mapping.get(base, 0)
                            if key_mod and 'min' in key_mod.group(0):
                                key_value += 12  # Arbitrary offset for minor (optional)
                        audio_features['key'] = key_value
                    else:
                        audio_features[feature.lower().replace(' ', '_').replace('(', '').replace(')', '')] = float(row[feature])
                except (ValueError, TypeError, AttributeError):
                    continue
        return audio_features
    
    def load_and_validate_dataset(self, json_file_path: str) -> pd.DataFrame:
        """Load and validate the Spotify dataset (JSON)"""
        logger.info(f"Loading dataset from {json_file_path}")
        try:
            # Load JSON with chunks for large files
            df = pd.read_json(json_file_path, lines=True, chunksize=self.chunk_size)
            combined_df = pd.concat(df, ignore_index=True)
            
            logger.info(f"Dataset loaded: {len(combined_df)} rows, {len(combined_df.columns)} columns")
            logger.info(f"Columns: {combined_df.columns.tolist()}")
            
            # Map actual column names to expected ones
            column_mapping = {
                'Artist(s)': 'artist',
                'song': 'song_title',
                'emotion': 'emotion',
                'Genre': 'genre',
                'Tempo': 'tempo',
                'Key': 'key',
                'Loudness (db)': 'loudness'
            }
            df_renamed = combined_df.rename(columns=column_mapping)
            
            # Collect contextual tags
            contextual_cols = [col for col in combined_df.columns if col.startswith('Good for')]
            df_renamed['contextual_tags'] = combined_df[contextual_cols].apply(
                lambda row: [col.split('Good for ')[1].lower().replace(' ', '_') for col in contextual_cols if row[col]], axis=1
            ).tolist()  # Convert Series to list
            
            # Handle Similar Songs (assuming it's a list or string)
            df_renamed['similar_songs'] = combined_df['Similar Songs'].apply(
                lambda x: json.loads(x) if isinstance(x, str) and x.startswith('[') else x
            ).tolist()  # Convert Series to list
            
            required_cols = ['artist', 'song_title']
            missing_cols = [col for col in required_cols if col not in df_renamed.columns]
            if missing_cols:
                raise ValueError(f"Missing required columns after mapping: {missing_cols}")
            
            df_renamed = df_renamed.dropna(subset=required_cols)
            logger.info(f"After cleaning: {len(df_renamed)} rows remain")
            return df_renamed
            
        except Exception as e:
            logger.error(f"Error loading dataset: {e}")
            raise
    
    def process_chunk(self, chunk_df: pd.DataFrame) -> List[SpotifyTrack]:
        """Process a chunk of the dataset into SpotifyTrack objects"""
        tracks = []
        for _, row in chunk_df.iterrows():
            try:
                track_id = self.generate_track_id(row['artist'], row['song_title'])
                
                # Ensure contextual_tags and similar_songs are lists
                contextual_tags = self.parse_contextual_tags(row.get('contextual_tags'))
                similar_tracks = self.parse_similar_tracks(row.get('similar_songs'))
                
                track = SpotifyTrack(
                    track_id=track_id,
                    artist=str(row['artist']).strip(),
                    song_title=str(row['song_title']).strip(),
                    emotion=str(row.get('emotion', 'neutral')).lower(),
                    genre=str(row.get('genre', 'unknown')).lower(),
                    tempo=float(row.get('tempo', 120.0)),
                    key=int(self.extract_audio_features(row).get('key', 0)),  # Use parsed key
                    loudness=float(row.get('loudness', -10.0)),
                    contextual_tags=contextual_tags,
                    similar_tracks=similar_tracks,
                    audio_features=self.extract_audio_features(row)
                )
                tracks.append(track)
                self.track_cache[track_id] = track
            except Exception as e:
                logger.warning(f"Error processing row: {e}")
                continue
        return tracks
    
    def create_genre_emotion_mappings(self, df: pd.DataFrame):
        """Create mappings for genres and emotions"""
        if 'genre' in df.columns:
            self.genre_mapping = {genre: i for i, genre in enumerate(df['genre'].dropna().unique())}
        if 'emotion' in df.columns:
            self.emotion_mapping = {emotion: i for i, emotion in enumerate(df['emotion'].dropna().unique())}
        with open(self.processed_path / 'feature_mappings.json', 'w') as f:
            json.dump({'genres': self.genre_mapping, 'emotions': self.emotion_mapping}, f, indent=2)
        logger.info(f"Created mappings: {len(self.genre_mapping)} genres, {len(self.emotion_mapping)} emotions")
    
    def bootstrap_dataset(self, json_file_path: str) -> Dict:
        """Main bootstrap function to process the entire dataset"""
        logger.info("Starting Spotify dataset bootstrap process...")
        
        df = self.load_and_validate_dataset(json_file_path)
        self.create_genre_emotion_mappings(df)
        
        total_chunks = len(df) // self.chunk_size + (1 if len(df) % self.chunk_size else 0)
        all_tracks = []
        
        logger.info(f"Processing {total_chunks} chunks...")
        for i in range(0, len(df), self.chunk_size):
            chunk_df = df.iloc[i:i + self.chunk_size]
            chunk_tracks = self.process_chunk(chunk_df)
            all_tracks.extend(chunk_tracks)
            chunk_num = i // self.chunk_size + 1
            with open(self.processed_path / f'tracks_chunk_{chunk_num:04d}.json', 'w') as f:
                json.dump([track.to_dict() for track in chunk_tracks], f, indent=2)
            logger.info(f"Processed chunk {chunk_num}/{total_chunks}: {len(chunk_tracks)} tracks")
        
        track_index = {track.track_id: {'artist': track.artist, 'song_title': track.song_title, 
                                       'genre': track.genre, 'emotion': track.emotion,
                                       'chunk_file': f'tracks_chunk_{(i // self.chunk_size + 1):04d}.json'}
                       for i, track in enumerate(all_tracks)}
        with open(self.processed_path / 'track_index.json', 'w') as f:
            json.dump(track_index, f, indent=2)
        
        stats = self.generate_dataset_stats(all_tracks)
        with open(self.processed_path / 'dataset_stats.json', 'w') as f:
            json.dump(stats, f, indent=2)
        logger.info("Bootstrap process completed successfully!")
        return stats
    
    def generate_dataset_stats(self, tracks: List[SpotifyTrack]) -> Dict:
        """Generate dataset statistics"""
        stats = {
            'total_tracks': len(tracks),
            'unique_artists': len(set(track.artist for track in tracks)),
            'genre_distribution': {genre: sum(1 for t in tracks if t.genre == genre) for genre in set(t.genre for t in tracks)},
            'emotion_distribution': {emotion: sum(1 for t in tracks if t.emotion == emotion) for emotion in set(t.emotion for t in tracks)},
            'similarity_stats': {'tracks_with_similar': len([t for t in tracks if t.similar_tracks])}
        }
        return stats
    
    def create_sample_dataset(self, sample_size: int = 1000) -> str:
        """Create a smaller sample dataset for testing"""
        sample_file = self.processed_path / 'sample_tracks.json'
        chunk_files = list(self.processed_path.glob('tracks_chunk_*.json'))
        if chunk_files:
            with open(chunk_files[0], 'r') as f:
                all_tracks = json.load(f)
            sample_tracks = [SpotifyTrack(**t) for t in all_tracks[:sample_size]]
            with open(sample_file, 'w') as f:
                json.dump([track.to_dict() for track in sample_tracks], f, indent=2)
            logger.info(f"Created sample dataset with {len(sample_tracks)} tracks")
            return str(sample_file)
        return None

def main():
    """Main execution function"""
    import argparse
    parser = argparse.ArgumentParser(description='Bootstrap Spotify 900K dataset')
    parser.add_argument('--json-file', required=True, help='Path to Spotify JSON file')
    parser.add_argument('--data-path', default='./data', help='Data directory path')
    parser.add_argument('--chunk-size', type=int, default=10000, help='Processing chunk size')
    parser.add_argument('--sample-size', type=int, default=1000, help='Sample dataset size')
    args = parser.parse_args()
    
    processor = SpotifyDataProcessor(data_path=args.data_path, chunk_size=args.chunk_size)
    try:
        stats = processor.bootstrap_dataset(args.json_file)
        sample_file = processor.create_sample_dataset(args.sample_size)
        
        print("\n" + "="*50)
        print("BOOTSTRAP COMPLETED SUCCESSFULLY!")
        print("="*50)
        print(f"Total tracks processed: {stats['total_tracks']:,}")
        print(f"Unique artists: {stats['unique_artists']:,}")
        print(f"Genres found: {len(stats['genre_distribution'])}")
        print(f"Tracks with similar songs: {stats['similarity_stats']['tracks_with_similar']:,}")
        if sample_file:
            print(f"Sample dataset created: {sample_file}")
        print("\nFiles created:")
        print(f"- Processed chunks: {args.data_path}/processed/tracks_chunk_*.json")
        print(f"- Track index: {args.data_path}/processed/track_index.json")
        print(f"- Dataset stats: {args.data_path}/processed/dataset_stats.json")
    except Exception as e:
        logger.error(f"Bootstrap failed: {e}")
        raise

if __name__ == "__main__":
    main()