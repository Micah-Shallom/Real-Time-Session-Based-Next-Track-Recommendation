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
