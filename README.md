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

## 📁 Project Structure
```
realtime-music-recommender/
├── README.md
├── .gitignore
├── docker-compose.yml
├── .env.example
├── requirements.txt
│
├── docs/
│   ├── architecture/
│   │   ├── system-architecture.md
│   │   ├── data-flow-diagram.md
│   │   └── component-interactions.md
│   ├── api/
│   │   ├── kafka-api.md
│   │   ├── spark-api.md
│   │   └── recommendation-api.md
│   ├── deployment/
│   │   ├── local-setup.md
│   │   ├── docker-guide.md
│   │   └── troubleshooting.md
│   └── data/
│       ├── schema-definitions.md
│       ├── sample-data.md
│       └── data-pipeline.md
│
├── src/
│   ├── __init__.py
│   ├── config/
│   │   ├── __init__.py
│   │   ├── settings.py
│   │   └── logging_config.py
│   │
│   ├── data/
│   │   ├── __init__.py
│   │   ├── preprocessing/
│   │   │   ├── __init__.py
│   │   │   ├── lastfm_processor.py
│   │   │   └── session_analyzer.py
│   │   └── streaming/
│   │       ├── __init__.py
│   │       ├── kafka_producer.py
│   │       └── data_simulator.py
│   │
│   ├── processing/
│   │   ├── __init__.py
│   │   ├── spark/
│   │   │   ├── __init__.py
│   │   │   ├── streaming_processor.py
│   │   │   ├── session_extractor.py
│   │   │   └── batch_processor.py
│   │   └── models/
│   │       ├── __init__.py
│   │       ├── markov_chain.py
│   │       ├── recommendation_engine.py
│   │       └── model_evaluator.py
│   │
│   ├── storage/
│   │   ├── __init__.py
│   │   ├── mongodb/
│   │   │   ├── __init__.py
│   │   │   ├── connection.py
│   │   │   ├── schemas.py
│   │   │   └── operations.py
│   │   └── cache/
│   │       ├── __init__.py
│   │       └── redis_client.py
│   │
│   ├── api/
│   │   ├── __init__.py
│   │   ├── recommendation_service.py
│   │   ├── health_check.py
│   │   └── middleware.py
│   │
│   ├── ui/
│   │   ├── __init__.py
│   │   ├── streamlit_app.py
│   │   ├── components/
│   │   │   ├── __init__.py
│   │   │   ├── live_dashboard.py
│   │   │   ├── user_analytics.py
│   │   │   └── system_metrics.py
│   │   └── utils/
│   │       ├── __init__.py
│   │       └── ui_helpers.py
│   │
│   └── orchestration/
│       ├── __init__.py
│       ├── airflow/
│       │   ├── dags/
│       │   │   ├── __init__.py
│       │   │   ├── main_pipeline_dag.py
│       │   │   └── model_training_dag.py
│       │   └── plugins/
│       │       ├── __init__.py
│       │       └── custom_operators.py
│       └── monitoring/
│           ├── __init__.py
│           ├── prometheus_config.py
│           └── grafana_dashboards/
│
├── tests/
│   ├── __init__.py
│   ├── unit/
│   │   ├── __init__.py
│   │   ├── test_data_processing.py
│   │   ├── test_models.py
│   │   └── test_api.py
│   ├── integration/
│   │   ├── __init__.py
│   │   ├── test_pipeline.py
│   │   └── test_end_to_end.py
│   └── fixtures/
│       ├── sample_data.json
│       └── test_configs.py
│
├── data/
│   ├── raw/
│   │   └── .gitkeep
│   ├── processed/
│   │   └── .gitkeep
│   ├── models/
│   │   └── .gitkeep
│   └── samples/
│       └── lastfm_sample.json
│
├── docker/
│   ├── kafka/
│   │   └── Dockerfile
│   ├── spark/
│   │   └── Dockerfile
│   ├── mongodb/
│   │   └── init-scripts/
│   ├── airflow/
│   │   └── Dockerfile
│   └── streamlit/
│       └── Dockerfile
│
├── scripts/
│   ├── setup/
│   │   ├── install_dependencies.sh
│   │   ├── setup_environment.sh
│   │   └── download_data.sh
│   ├── deployment/
│   │   ├── start_services.sh
│   │   ├── stop_services.sh
│   │   └── health_check.sh
│   └── utils/
│       ├── cleanup.sh
│       └── backup_data.sh
│
├── monitoring/
│   ├── prometheus/
│   │   └── prometheus.yml
│   ├── grafana/
│   │   ├── dashboards/
│   │   └── provisioning/
│   └── logs/
│       └── .gitkeep
│
└── notebooks/
    ├── data_exploration.ipynb
    ├── model_development.ipynb
    ├── performance_analysis.ipynb
    └── results_visualization.ipynb
```

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
