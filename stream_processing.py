import os
import sys
import json
from typing import List
from pyflink.common import SimpleStringSchema, WatermarkStrategy, Configuration
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import FlatMapFunction, RuntimeContext
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from elasticsearch import Elasticsearch
from datasketch import MinHash
from openai import OpenAI
import re
import time
from dotenv import load_dotenv
load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
NUM_PERMUTATIONS = int(os.getenv("NUM_PERM", 128))
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "text-embedding-3-small")

class ElasticWriter(FlatMapFunction):
    def __init__(self, es_host: str, openai_api_key: str):
        self.es_host = es_host
        self.openai_api_key = openai_api_key

    @staticmethod
    def clean_location(text):
        if not isinstance(text, str): return ""
        lines = text.split('\n')
        clean_lines = [line.strip() for line in lines if "Hết hạn" not in line and "lượt xem" not in line]
        return ", ".join(clean_lines)

    @staticmethod
    def clean_description(text):
        if not isinstance(text, str): return ""
        text = re.sub(r'(?i)mô tả công việc[:\s]*', '', text)
        text = re.sub(r'\n+', '\n', text).strip()
        return text
    
    @staticmethod
    def clean_min_experience(text):
        if isinstance(text, (int, float)): return int(text)
        if not isinstance(text, str): return 0
        match = re.search(r'\d+', text)
        return int(match.group()) if match else 0
    
    @staticmethod
    def generate_minhash_signature(tokens: List[str], num_perm: int = NUM_PERMUTATIONS) -> List[str]:
        """
        Generate MinHash signature from list of tokens (skills).
        CRITICAL: Only use this for skill lists, not full text.
        """
        tokens_set = set(t.lower().strip() for t in tokens if t.strip())
        m = MinHash(num_perm=num_perm)
        for t in tokens_set:
            m.update(t.encode("utf-8"))
        return [f"hash_{v}" for v in m.hashvalues]
    
    def embed_text(self, text: str) -> List[float]:
        """Generate embedding vector via OpenAI API."""
        resp = self.openai_client.embeddings.create(model=EMBEDDING_MODEL, input=text)
        return resp.data[0].embedding

    def open(self, runtime_context: RuntimeContext):
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

    def flat_map(self, value):
        # 1. Nếu tin nhắn rỗng, không làm gì (tương đương return)
        if not value:
            return

        try:
            # 2. Deserialize & Ghi ES
            raw_data = json.loads(value)

            title = raw_data.get("title", "Unknown")
            location = self.clean_location(raw_data.get("location", ""))
            description = self.clean_description(raw_data.get("description", ""))
            description_embedding = self.embed_text(description) if description else []

            skills = raw_data.get("required_skills", "")
            skills_arr = [s.strip() for s in skills.split(',')] if skills else []
            skills_text = " ".join(skills_arr)
            skills_signature = self.generate_minhash_signature([s.strip() for s in raw_data.get("required_skills", "").split(',')] if raw_data.get("required_skills") else []) 

            min_experience = self.clean_min_experience(raw_data.get("min_experience_years", "0"))

            link = raw_data.get("link", "")

            document = {
                "doc_type": "job",
                "title": title,
                "skills": skills_arr,
                "skills_signature": skills_signature,
                "skills_text": skills_text,
                "description": description,
                "description_vector": description_embedding,
                # "experience": min_experience,
                # "experience_vector": experience_vector,
                "location": location,
                # "job_type": raw_data.get("job_type", ""),
                "experience_years": min_experience,
                "link": link,
                "metadata": {
                    "source": "job_description",
                    "indexed_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                }
            }

            self.es.index(index="job_postings", document=document)
            
            title = document.get('title', 'Unknown')
            
            yield f">>> [Saved OK] {title[:40]}..."
            
        except Exception as e:
            print(f"!!! [Skip] Error: {e}")

    def close(self):
        if hasattr(self, 'es'):
            self.es.close()

class JobConsumerPipeline:
    def __init__(self, jar_paths, kafka_bootstrap="localhost:9092", es_host="http://127.0.0.1:9200"):
        self.kafka_bootstrap = kafka_bootstrap
        self.es_host = es_host
        
        config = Configuration()
        config.set_string("classloader.check-leaked-classloader", "false")
        
        # Cấu hình Python Path để tránh lỗi 'Process died' trên Windows
        current_python = sys.executable
        config.set_string("python.client.executable", current_python)
        config.set_string("python.executable", current_python)

        self.env = StreamExecutionEnvironment.get_execution_environment(configuration=config)
        self.env.set_parallelism(1)

        if isinstance(jar_paths, str): jar_paths = [jar_paths]
        formatted_jars = []
        for path in jar_paths:
            abs_path = os.path.abspath(path)
            unix_style = abs_path.replace("\\", "/")
            if not unix_style.startswith("file:///"):
                if unix_style.startswith("file:"): unix_style = unix_style.replace("file:", "file:///")
                else: unix_style = f"file:///{unix_style}"
            formatted_jars.append(unix_style)
        
        print(f"--- Loading JARs: {formatted_jars}")
        separator = ";" if os.name == 'nt' else ":"
        self.env.add_jars(separator.join(formatted_jars))

    def run(self, topic="job_postings", group_id="job_group_final_v5"):
        print(f"--- [Hybrid Mode] Reading '{topic}' -> Writing to ES (Python Client) ---")
        
        kafka_source = KafkaSource.builder() \
            .set_bootstrap_servers(self.kafka_bootstrap) \
            .set_topics(topic) \
            .set_group_id(group_id) \
            .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
            .set_value_only_deserializer(SimpleStringSchema()) \
            .build()
            
        stream = self.env.from_source(kafka_source, WatermarkStrategy.no_watermarks(), "Kafka Source")
        
        es_writer = ElasticWriter(self.es_host, openai_api_key=OPENAI_API_KEY)
        
        # Chạy
        stream.flat_map(es_writer).print()
        
        print("Pipeline Running... (Waiting for data)")
        try:
            self.env.execute("Job Postings Hybrid Pipeline")
        except Exception as e:
            print(f"❌ Error: {e}")

if __name__ == "__main__":
   
    jar_path = ["jars/flink-sql-connector-kafka-3.2.0-1.19.jar"]
    pipeline = JobConsumerPipeline(jar_paths=jar_path)
    pipeline.run(topic="job_postings", group_id="job_group_es9_v1")