# 🎧 Real-Time Session-Based Next-Track Recommendation System

This project builds a real-time music recommendation engine using session data from the Last.fm-1K dataset. It simulates user listening sessions, processes them in real-time using Apache Kafka and Spark, and recommends the next track using a Markov model. Results are stored in MongoDB and visualized in a live Streamlit UI.

## 🧱 Architecture

![Pipeline Diagram](architecture/pipeline_diagram.png)

## 🛠️ Technologies Used

- **Apache Kafka**: Event streaming
- **Apache Spark**: Real-time processing
- **MongoDB**: Data storage
- **Streamlit**: UI for live recommendations
- **Apache Airflow**: Pipeline orchestration
- **Prometheus + Grafana** *(optional)*: Monitoring
- 
## Quick Start Guide

### Prerequisites
- Docker & Docker Compose installed
- Python 3.8+ (for development)
- Git
- 16GB+ RAM recommended
- 20GB+ free disk space

### Initial Setup
1. **Clone Repository**
   ```bash
   git clone <your-repo-url>
   cd realtime-music-recommender
   ```

2. **Environment Setup**
   ```bash
   cp .env.example .env
   # Edit .env with your configurations
   ```

3. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Start Services**
   ```bash
   docker-compose up -d
   ```

5. **Verify Setup**
   ```bash
   ./scripts/deployment/health_check.sh
   ```

## Architecture Overview

This system implements a real-time music recommendation pipeline using:

- **Kafka**: Stream ingestion and message queuing
- **Spark Streaming**: Real-time data processing
- **MongoDB**: Session storage and recommendation cache
- **Streamlit**: Live dashboard and user interface
- **Airflow**: Pipeline orchestration and scheduling
- **Prometheus/Grafana**: Monitoring and alerting

## Key Features

- **Real-time Processing**: Sub-10-second recommendation latency
- **Session-based Learning**: Markov chain model for sequential predictions
- **Scalable Architecture**: Handle 100+ events/second
- **Live Dashboard**: Real-time "Up Next" playlist visualization
- **Fault Tolerance**: Automatic recovery and data persistence

## Quick Commands

```bash
# Start all services
docker-compose up -d

# View logs
docker-compose logs -f [service-name]

# Stop services
docker-compose down

# Reset environment
docker-compose down -v
docker system prune -f

# Run tests
python -m pytest tests/

# Start development server
streamlit run src/ui/streamlit_app.py
```
