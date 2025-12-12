import os
import sys
import json
from typing import List
from pyflink.common import SimpleStringSchema, WatermarkStrategy, Configuration
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import FlatMapFunction, RuntimeContext
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from elasticsearch import Elasticsearch, helpers
from kafka_service import KafkaService
from datasketch import MinHash
from openai import OpenAI
import re
import time
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration Constants
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
NUM_PERMUTATIONS = int(os.getenv("NUM_PERM", 128))
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "text-embedding-3-small")
BATCH_SIZE = 5  # Number of documents to buffer before writing to Elasticsearch
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")

class ElasticWriter(FlatMapFunction):
    """
    Flink FlatMapFunction that processes stream elements and writes to Elasticsearch using Bulk API.
    """
    def __init__(self, es_host: str, openai_api_key: str, batch_size: int):
        self.es_host = es_host
        self.openai_api_key = openai_api_key
        self.batch_size = batch_size
        self.buffer = []  # Local buffer for batch processing

    @staticmethod
    def clean_location(text):
        """Cleaning logic: Removes specific Vietnamese keywords like 'Expired' or 'Views'."""
        if not isinstance(text, str): return ""
        lines = text.split('\n')
        # Filter out lines containing "Hết hạn" (Expired) or "lượt xem" (views)
        clean_lines = [line.strip() for line in lines if "Hết hạn" not in line and "lượt xem" not in line]
        return ", ".join(clean_lines)

    @staticmethod
    def clean_description(text):
        """Cleaning logic: Removes headers and normalizes whitespace."""
        if not isinstance(text, str): return ""
        text = re.sub(r'(?i)mô tả công việc[:\s]*', '', text)
        text = re.sub(r'\n+', '\n', text).strip()
        return text
    
    @staticmethod
    def clean_min_experience(text):
        """Extracts integer years of experience from string input."""
        if isinstance(text, (int, float)): return int(text)
        if not isinstance(text, str): return 0
        match = re.search(r'\d+', text)
        return int(match.group()) if match else 0
    
    @staticmethod
    def generate_minhash_signature(tokens: List[str], num_perm: int = NUM_PERMUTATIONS) -> List[str]:
        """
        Generate MinHash signature from list of tokens (skills).
        CRITICAL: Only use this for skill lists/sets, not full text descriptions.
        """
        tokens_set = set(t.lower().strip() for t in tokens if t.strip())
        m = MinHash(num_perm=num_perm)
        for t in tokens_set:
            m.update(t.encode("utf-8"))
        # Return hash values as strings for storage
        return [f"hash_{v}" for v in m.hashvalues]
    
    def embed_text(self, text: str) -> List[float]:
        """Generate embedding vector via OpenAI API."""
        resp = self.openai_client.embeddings.create(model=EMBEDDING_MODEL, input=text)
        return resp.data[0].embedding

    def open(self, runtime_context: RuntimeContext):
        """
        Lifecycle method: Called once during initialization of the parallel instance.
        Sets up connections to Elasticsearch and OpenAI.
        """
        print(f">>> [ES Writer] Connecting to {self.es_host}...")
        try:
            self.es = Elasticsearch(
                self.es_host,
                request_timeout=30,
                max_retries=3,
                retry_on_timeout=True
            )
            self.openai_client = OpenAI(api_key=self.openai_api_key)
            if self.es.ping():
                print(">>> [ES Writer] Connected successfully!")
            else:
                print("!!! [ES Writer] Ping failed!")
        except Exception as e:
            print(f"!!! [ES Writer] Connection Error: {e}")
    
    def flush_buffer(self):
        """
        Helper function to perform Bulk Indexing to Elasticsearch.
        UPDATED: Handle errors gracefully and log details without crashing.
        """
        if not self.buffer:
            return

        try:
            # raise_on_error=False: Ngăn chương trình bị crash khi ES từ chối data
            # stats_only=False: Trả về danh sách chi tiết các item bị lỗi
            success, failed = helpers.bulk(
                self.es, 
                self.buffer, 
                stats_only=False, 
                raise_on_error=False
            )
            
            print(f">>> [Bulk Saved] {success} docs saved.")

            # Nếu có documents bị lỗi, in chi tiết ra để sửa
            if failed:
                print(f">>> [ERROR] {len(failed)} documents failed to index. Details below:")
                for item in failed:
                    # In ra lỗi cụ thể (ví dụ: mapper_parsing_exception)
                    error_detail = item.get('index', {}).get('error', {})
                    print(f"    - ID: {item.get('index', {}).get('_id')}")
                    print(f"    - Reason: {error_detail.get('reason')}")
                    # print(f"    - Full Error: {item}") # Bỏ comment nếu muốn xem full log

        except Exception as e:
            print(f">>> [CRITICAL ERROR] Bulk write failed completely: {str(e)}")
        
        # Reset buffer để tiếp tục xử lý lô tiếp theo
        self.buffer = []

    def flat_map(self, value):
        """
        Main processing logic for each Kafka message.
        """
        # 1. Skip empty messages
        if not value:
            return

        
        # 2. Deserialize & Prepare Data
        raw_data = json.loads(value)

        title = raw_data.get("title", "Unknown")
        location = self.clean_location(raw_data.get("location", ""))
        description = self.clean_description(raw_data.get("description", ""))
        job_requirements = raw_data.get("job_requirements", "")
        
        # Generate Embeddings (Costly operation)
        description_embedding = self.embed_text(description) if description else None
        job_requirements_embedding = self.embed_text(job_requirements) if job_requirements else None


        # Process Skills & MinHash
        skills = raw_data.get("required_skills", "")
        skills_arr = [s.strip() for s in skills.split(',')] if skills else []
        skills_text = " ".join(skills_arr)
        skills_signature = self.generate_minhash_signature(skills_arr) 

        min_experience = self.clean_min_experience(raw_data.get("min_experience_years", "0"))
        link = raw_data.get("link", "")

        # Construct Document
        document = {
            "doc_type": "job",
            "title": title,
            "skills": skills_arr,
            "skills_signature": skills_signature,
            "skills_text": skills_text,
            "description": description,
            "description_vector": description_embedding,
            "experience": job_requirements,      # Optional fields commented out
            "experience_vector": job_requirements_embedding,
            "location": location,
            "experience_years": min_experience,
            "link": link,
            "metadata": {
                "source": "job_description",
                "indexed_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            }
        }

        # Prepare Bulk Action
        action = {
            "_index": "job_postings",
            "_source": document
        }
        self.buffer.append(action)

        # 3. Check Buffer Size -> Flush if full
        if len(self.buffer) >= self.batch_size:
            self.flush_buffer()
            yield f">>> [Bulk Saved] {self.batch_size} documents indexed."
        else:
            yield f">>> [Buffered] {len(self.buffer)}/{self.batch_size} documents. [Saved OK] {title[:40]}..."
            
        

    def close(self):
        """
        Lifecycle method: Called when the job stops or finishes.
        Ensures any remaining items in the buffer are written to ES.
        """
        if self.buffer:
            print(f">>> [Closing] Flushing remaining {len(self.buffer)} docs...")
            self.flush_buffer()
        if hasattr(self, 'es'):
            self.es.close()

class JobConsumerPipeline:
    def __init__(self, jar_paths, kafka_bootstrap="localhost:9092", es_host="http://127.0.0.1:9200"):
        self.kafka_bootstrap = kafka_bootstrap
        self.es_host = es_host
        
        config = Configuration()
        # Prevent memory leaks in long-running jobs
        config.set_string("classloader.check-leaked-classloader", "false")
        
        # Configure Python executable path to avoid 'Process died' errors on Windows
        current_python = sys.executable
        config.set_string("python.client.executable", current_python)
        config.set_string("python.executable", current_python)

        self.env = StreamExecutionEnvironment.get_execution_environment(configuration=config)
        self.env.set_parallelism(1)

        # Handle JAR path formatting for Windows/Unix compatibility
        if isinstance(jar_paths, str): jar_paths = [jar_paths]
        formatted_jars = []
        for path in jar_paths:
            abs_path = os.path.abspath(path)
            unix_style = abs_path.replace("\\", "/")
            # Ensure correct file protocol for Flink classloader
            if not unix_style.startswith("file:///"):
                if unix_style.startswith("file:"): unix_style = unix_style.replace("file:", "file:///")
                else: unix_style = f"file:///{unix_style}"
            formatted_jars.append(unix_style)
        
        print(f"--- Loading JARs: {formatted_jars}")
        separator = ";" if os.name == 'nt' else ":"
        self.env.add_jars(separator.join(formatted_jars))

    def run(self, topic="job_postings", group_id="job_group_final_v5"):
        print(f"--- [Hybrid Mode] Reading '{topic}' -> Writing to ES (Python Client) ---")
        
        # Build Kafka Source
        kafka_source = KafkaSource.builder() \
            .set_bootstrap_servers(self.kafka_bootstrap) \
            .set_topics(topic) \
            .set_group_id(group_id) \
            .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
            .set_value_only_deserializer(SimpleStringSchema(charset="utf-8")) \
            .build()
            
        stream = self.env.from_source(kafka_source, WatermarkStrategy.no_watermarks(), "Kafka Source")
        
        # Initialize custom writer
        es_writer = ElasticWriter(self.es_host, openai_api_key=OPENAI_API_KEY, batch_size=BATCH_SIZE)
        
        # Execute Stream
        stream.flat_map(es_writer).print()
        
        print("Pipeline Running... (Waiting for data)")
        try:
            self.env.execute("Job Postings Hybrid Pipeline")
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":

    # Ensure Topic Exists
    kafka_service = KafkaService(bootstrap_servers=BOOTSTRAP_SERVERS)
    kafka_service.create_topic("job_postings", num_partitions=3, replication_factor=1)
   
    # Point to the Kafka Connector JAR
    jar_path = ["jars/flink-sql-connector-kafka-3.2.0-1.19.jar"]
    pipeline = JobConsumerPipeline(jar_paths=jar_path)
    
    # Run Pipeline
    pipeline.run(topic="job_postings", group_id="job_group_es9_v1")