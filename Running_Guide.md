Quick Start / Deployment Guide

## Step-by-Step Execution Guide

Follow these instructions to activate the data pipeline infrastructure, streaming workers, and tracking interfaces concurrently on your local machine.

### Prerequisites

- Docker Desktop installed and running.
- Python 3.10 environment with access to create a virtual environment (`venv`).
- A Snowflake Account with target connection privileges.

---

### Step 1: Environment Setup & Activation

Clone the repository, initialize your isolated virtual environment, and install the required streaming and data engineering dependencies:

# Navigate into your local repository workspace

cd stream_nexus

# Create a local Python virtual environment

python -m venv venv

# Activate the environment (Windows Command Prompt / PowerShell)

.\venv\Scripts\activate

To deactivate the environment (Windows Command Prompt / PowerShell)

deactivate

# Install pipeline, visualization, and cloud warehouse libraries

pip install kafka-python streamlit snowflake-connector-python python-dotenv plotly apache-airflow

# Snowflake credentials

Make sure your configuration parameters (Snowflake credentials, database names, and cluster ports) are populated inside your local .env file at the root of the project directory before moving forward.

### Step 2: Spin Up Infrastructure Containers

Launch the isolated Apache Kafka broker and AKHQ topic-monitoring interface using Docker Compose:

docker compose up -d

To verify everything launched successfully, open your browser and navigate to http://localhost:8086 to access the AKHQ management console dashboard.

### Step 3: Launch Live Streaming Workers

Open separate terminal tabs (ensuring your (venv) is active in each) to spin up the continuous background streaming applications.

Terminal A: The Advanced Simulation Producer

# Runs the simulation loop generating rush hour traffic patterns and surge pricing

python src/producer.py

Terminal B: The Micro-Batch Consumer Ingestion Engine

# Polls message streams from Kafka partitions and flushes them to Snowflake every 10 seconds

python src/consumer.py

### Step 4: Access Your Real-Time Operations Interfaces

With data successfully streaming from Kafka into your Snowflake warehouse tables, open your monitoring and business intelligence interfaces:

1. Live Stream Operations UI (Streamlit)
   Launch your real-time performance tracking dashboard canvas by running this command in a new terminal pane:

streamlit run src/dashboard.py

The dashboard will automatically open inside your web browser at http://localhost:8501, showing dynamic revenue trackers, surge metrics, and ride status graphs.

2. Infrastructure Auditing & Orchestration Control (Apache Airflow)
   Launch the Apache Airflow interface using Docker Compose, navigate to airflow and run:

docker compose up -d

Ensure your Airflow container framework is running on port 8085. Access the control panel at:

http://localhost:8085

Locate the ride_pipeline_monitor DAG, toggle it to On, and trigger a manual run to test the infrastructure health audit and error recovery logic.
