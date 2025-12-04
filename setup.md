
# Setup Guide: Job Crawler & Search Pipeline

This guide covers the installation and configuration of the environment required to run the Hybrid JD-CV Matching System, Web Crawler, and Flink Data Pipeline.

## 1\. Prerequisites

Before starting, ensure you have the following installed:

  * **Docker Desktop** (for Elasticsearch & Kibana)
  * **Java 17 (JDK)**: Required for Apache Flink.
      * *Note:* Flink runs best on Java 11. Ensure `JAVA_HOME` is set in your system environment variables.

-----

## 2\. Python Setup (Version 3.10.11)

This project is strictly tested on **Python 3.10.11**. Using newer versions (3.11+) or older versions may cause conflicts with `apache-beam` and `pyflink`.

1.  **Download Python 3.10.11**:

      * [Installer](https://www.python.org/downloads/release/python-31011/)

2.  **Create a Virtual Environment**:
    Open your terminal/command prompt in the project root:

    ```bash
    # Windows
    py -3.10 -m venv .venv

    # Mac/Linux
    python3.10 -m venv venv
    ```

3.  **Activate the Environment**:

    ```bash
    # Windows
    .\venv\Scripts\activate

    # Mac/Linux
    source venv/bin/activate
    ```

-----

## 3\. Install Python Dependencies

**Install the packages:**

```bash
pip install -r requirements.txt
```

-----

## 4\. Apache Flink JAR Dependencies

The Flink pipeline requires a specific JAR connector to talk to Kafka.

1.  Create a folder named `jars` in the project root.
2.  **Download**: `flink-sql-connector-kafka` (matches Flink 1.19).
      * **File Name**: `flink-sql-connector-kafka-3.2.0-1.19.jar`
      * **Download Link**: [Maven Central Repository](https://www.google.com/search?q=https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.2.0-1.19/flink-sql-connector-kafka-3.2.0-1.19.jar)
3.  Place the downloaded file inside the `jars/` folder.

**Project Structure should look like this:**

```text
project_root/
├── jars/
│   └── flink-sql-connector-kafka-3.2.0-1.19.jar
├── venv/
├── docker-compose.yml
├── requirements.txt
├── .env
├── main.py (or your script names)
└── ...
```

-----

## 5\. Infrastructure (Docker Compose)

> **⚠️ Windows User Note**: The `volumes` path below is set to `D:/elasticsearch/data`. Update this path if your D: drive does not exist or if you are on Mac/Linux.

```yaml
services:
  elasticsearch:
    image: elasticsearch:9.2.1
    container_name: elasticsearch
    environment:
      - node.name=elasticsearch
      - cluster.name=datasearch
      - "ES_JAVA_OPTS=-Xms512m -Xmx512m"
      - cluster.initial_master_nodes=elasticsearch
      - xpack.security.enabled=false
    ports:
      - "9200:9200"
    volumes:
      - D:/elasticsearch/data:/usr/share/elasticsearch/data

  kibana:
    image: kibana:9.2.1
    container_name: kibana
    ports:
      - "5601:5601"
    environment:
      ELASTICSEARCH_HOSTS: "http://elasticsearch:9200"
    depends_on:
      - elasticsearch
```

**Start the services:**

```bash
docker-compose up -d
```

-----

## 6\. Environment Configuration

Create a `.env` file in the root directory to store secrets. **Do not commit this file to Git.**

```ini
# OpenAI Config
OPENAI_API_KEY=sk-your-key-here
EMBEDDING_MODEL=text-embedding-3-small

# Elasticsearch Config
ES_HOST=http://localhost:9200
INDEX_NAME=job_postings

# Algorithm Config
NUM_PERM=128

# VietnamWorks Credentials (for Crawler)
VNW_GMAIL=your_email@example.com
VNW_PASSWORD=your_password
```

*Note: You must create a VietNamWorks account using Google, and your Google account should turn off 2FA or any security methods.*

-----

## 7\. Verification

1.  **Verify Elasticsearch**: Open `http://localhost:9200` in your browser. You should see a JSON response.
2.  **Verify Kibana**: Open `http://localhost:5601`.
3.  **Run the Pipeline**:

    ```bash
    # Ensure you are in the venv
    python stream_processing.py # Run Flink so it reads from Kafka
    python vietnamwork_crawlerv3.py  # Enable job scraping
    ```
4.  **Verification**: After crawl all jobs from pages, run the following command to search the job matching with CV sample.

    ```bash
    python search.py 
    ```

*Note: Each command above must be run with separate terminals*

## Troubleshooting

  * **`Process died` error (Flink on Windows)**: This usually happens if Python is not found. Ensure `python.client.executable` in the code points to your `venv` python path.
  * **Selenium Errors**: Ensure Chrome is installed. The `webdriver-manager` package will handle the driver download automatically.
  * **Java Error**: Run `java -version` in your terminal. If it is not version 17, Flink may fail to start.