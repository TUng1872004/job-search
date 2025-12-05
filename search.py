#!/usr/bin/env python3
"""
Hybrid JD-CV Matching System (Mock Data Demo)
- Features:
  - Drag & Drop PDF support
  - Auto-ingest Mock Data (Vectors + MinHash)
  - Hybrid Search (Semantic + MinHash + BM25)
"""

import os
import sys
import json
import time
from typing import List, Dict, Any
import tkinter as tk
from tkinter import filedialog
from collections import defaultdict

from dotenv import load_dotenv
from datasketch import MinHash
from openai import OpenAI
from elasticsearch import Elasticsearch, helpers

# Import your extractor (Ensure extract.py is in the same folder)
try:
    from extract import ResumeExtractor
except ImportError:
    print("WARNING: 'extract.py' not found. Ensure ResumeExtractor is available.")
    sys.exit(1)

# ------------------------------------------------------------
# Config & Constants
# ------------------------------------------------------------
load_dotenv()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    raise RuntimeError("OPENAI_API_KEY missing. Put it in .env or environment.")

ES_HOST = os.getenv("ES_HOST", "http://localhost:9200")

# FORCE MOCK INDEX NAME
# We are using a hardcoded name to prevent environment variable formatting errors.
# Original: INDEX_NAME = os.getenv("INDEX_NAME", "job_postings_demo").strip()
INDEX_NAME = "mock_job_index"

EMBEDDING_MODEL = "text-embedding-3-small"
NUM_PERM = 128

# Fusion params
TOP_K_PER_SOURCE = 10
FINAL_K = 5
RRF_K = 60

# ------------------------------------------------------------
# Clients
# ------------------------------------------------------------
client = OpenAI(api_key=OPENAI_API_KEY)
es = Elasticsearch(
    hosts=[ES_HOST],
    verify_certs=False,
    ssl_show_warn=False
)


# ------------------------------------------------------------
# Class Definition
# ------------------------------------------------------------
class HybridJDCVMatching:
    def __init__(self, es_client: Elasticsearch, openai_client: OpenAI):
        self.es = es_client
        self.client = openai_client

    # --- Helpers ---
    def embed_text(self, text: str) -> List[float]:
        """Generate embedding vector via OpenAI API."""
        if not text: return [0.0] * 1536
        resp = self.client.embeddings.create(model=EMBEDDING_MODEL, input=text)
        return resp.data[0].embedding

    def generate_minhash_signature(self, tokens: List[str], num_perm: int = NUM_PERM) -> List[str]:
        """Generate MinHash signature from list of tokens (skills)."""
        if not tokens: return []
        tokens_set = set(t.lower().strip() for t in tokens if t.strip())
        m = MinHash(num_perm=num_perm)
        for t in tokens_set:
            m.update(t.encode("utf-8"))
        return [f"hash_{v}" for v in m.hashvalues]

    # --- Index Management ---
    def create_index(self, index_name: str = INDEX_NAME):
        """Creates the index with correct mappings if it does not exist."""
        try:
            if self.es.indices.exists(index=index_name):
                print(f"INFO: Index '{index_name}' already exists.")
                return
        except Exception as e:
            print(f"WARNING: Check exists failed ({e}). Attempting to create index anyway...")

        mapping = {
            "mappings": {
                "properties": {
                    "doc_type": {"type": "keyword"},
                    "title": {
                        "type": "text",
                        "analyzer": "standard",
                        "fields": {"keyword": {"type": "keyword"}}
                    },
                    "skills": {"type": "keyword"},
                    "skills_signature": {"type": "keyword"},  # For MinHash exact matches
                    "description": {"type": "text"},
                    "description_vector": {
                        "type": "dense_vector",
                        "dims": 1536,
                        "index": True,
                        "similarity": "cosine"
                    },
                    "experience": {"type": "text"},
                    "experience_vector": {
                        "type": "dense_vector",
                        "dims": 1536,
                        "index": True,
                        "similarity": "cosine"
                    }
                }
            }
        }

        try:
            self.es.indices.create(index=index_name, body=mapping)
            print(f"INFO: Index '{index_name}' created successfully.")
        except Exception as e:
            print(f"ERROR: Failed to create index '{index_name}': {e}")
            sys.exit(1)

    # --- Mock Data Ingestion ---
    def ingest_mock_data(self):
        """Generates embeddings and hashes for mock jobs and inserts them into ES."""
        # Check if data already exists to avoid re-billing OpenAI
        try:
            count = self.es.count(index=INDEX_NAME, body={"query": {"match_all": {}}})
            if count['count'] > 0:
                print(f"INFO: Index already contains {count['count']} documents. Skipping mock ingestion.")
                return
        except Exception:
            pass  # Index might be empty

        print("INFO: Generating mock data (Embeddings & Hashes)... This may take a few seconds.")

        # --- MOCK DATA SOURCE ---
        mock_jobs = [
            {
                "title": "Senior Python Backend Engineer",
                "skills": ["Python", "Django", "FastAPI", "PostgreSQL", "Docker", "AWS", "Redis"],
                "description": "We are looking for a Python expert to build scalable APIs. Experience with microservices and cloud infrastructure is required.",
                "experience": "5+ years of backend development experience."
            },
            {
                "title": "Sales Manager (B2B)",
                "skills": ["Sales", "Negotiation", "CRM", "Salesforce", "Lead Generation", "Communication"],
                "description": "Drive revenue growth by managing sales teams and closing B2B deals. Must have a strong network in the technology sector.",
                "experience": "7 years in sales, specifically B2B software."
            },
            {
                "title": "Data Analyst",
                "skills": ["SQL", "Tableau", "Python", "Excel", "Data Visualization", "Statistics"],
                "description": "Analyze large datasets to provide actionable insights for the business. Proficiency in SQL and dashboarding tools is a must.",
                "experience": "3 years of data analysis experience."
            }
        ]
        # ------------------------

        actions = []
        for i, job in enumerate(mock_jobs):
            # Generate Vectors
            desc_vec = self.embed_text(job['description'])
            exp_vec = self.embed_text(job['experience'])

            # Generate MinHash
            sig = self.generate_minhash_signature(job['skills'])

            doc = {
                "_index": INDEX_NAME,
                "_id": f"mock_job_{i}",
                "_source": {
                    "doc_type": "job",
                    "title": job['title'],
                    "skills": job['skills'],
                    "skills_signature": sig,
                    "description": job['description'],
                    "description_vector": desc_vec,
                    "experience": job['experience'],
                    "experience_vector": exp_vec
                }
            }
            actions.append(doc)

        helpers.bulk(self.es, actions)
        print(f"SUCCESS: Ingested {len(actions)} mock jobs into Elasticsearch.")
        time.sleep(1)  # Allow ES to refresh

    # --- Search Methods ---
    def semantic_search(self, vector: List[float], field: str, top_k: int) -> List[Dict]:
        body = {
            "size": top_k,
            "query": {
                "script_score": {
                    "query": {"term": {"doc_type": "job"}},
                    "script": {
                        "source": f"cosineSimilarity(params.query_vector, '{field}') + 1.0",
                        "params": {"query_vector": vector}
                    }
                }
            }
        }
        resp = self.es.search(index=INDEX_NAME, body=body)
        return [{"id": h["_id"], "score": h["_score"], "source": f"sem_{field}", **h["_source"]} for h in
                resp["hits"]["hits"]]

    def minhash_search(self, signature: List[str], top_k: int) -> List[Dict]:
        body = {
            "size": top_k,
            "query": {
                "script_score": {
                    "query": {"term": {"doc_type": "job"}},
                    "script": {
                        "source": "int m=0; for(int i=0;i<params.h.length;++i){if(doc['skills_signature'].contains(params.h[i])){m++;}} return m;",
                        "params": {"h": signature}
                    }
                }
            }
        }
        resp = self.es.search(index=INDEX_NAME, body=body)
        return [{"id": h["_id"], "score": h["_score"], "source": "minhash", **h["_source"]} for h in
                resp["hits"]["hits"]]

    def bm25_search(self, text: str, top_k: int) -> List[Dict]:
        body = {
            "size": top_k,
            "query": {
                "bool": {
                    "must": [
                        {"term": {"doc_type": "job"}},
                        {"match": {"title": {"query": text, "boost": 2.0}}}
                    ]
                }
            }
        }
        resp = self.es.search(index=INDEX_NAME, body=body)
        return [{"id": h["_id"], "score": h["_score"], "source": "bm25", **h["_source"]} for h in resp["hits"]["hits"]]

    # --- RRF & Main Search ---
    def reciprocal_rank_fusion(self, results_list: List[List[Dict]], weights: List[float]) -> List[Dict]:
        scores = defaultdict(float)
        meta = {}

        for idx, res_set in enumerate(results_list):
            w = weights[idx]
            for rank, item in enumerate(res_set, 1):
                did = item["id"]
                scores[did] += w * (1.0 / (RRF_K + rank))
                if did not in meta:
                    meta[did] = {"title": item.get("title"), "skills": item.get("skills"), "sources": {}}
                meta[did]["sources"][item["source"]] = {"rank": rank, "raw_score": item["score"]}

        final = []
        for did, score in scores.items():
            final.append({
                "id": did,
                "rrf_score": score,
                "title": meta[did]["title"],
                "skills": meta[did]["skills"],
                "sources": meta[did]["sources"]
            })

        return sorted(final, key=lambda x: x["rrf_score"], reverse=True)

    def hybrid_search(self, cv_data: Dict) -> List[Dict]:
        print("\n--- Running Hybrid Search Components ---")

        # 1. Semantic Description
        desc_vec = self.embed_text(cv_data.get("description", ""))
        sem_res = self.semantic_search(desc_vec, "description_vector", TOP_K_PER_SOURCE)
        print(f"1. Semantic (Desc): Found {len(sem_res)} candidates")

        # 2. MinHash Skills
        sig = self.generate_minhash_signature(cv_data.get("skills", []))
        mh_res = self.minhash_search(sig, TOP_K_PER_SOURCE)
        print(f"2. MinHash (Skills): Found {len(mh_res)} candidates")

        # 3. BM25 Title
        bm_res = self.bm25_search(cv_data.get("title", ""), TOP_K_PER_SOURCE)
        print(f"3. BM25 (Title): Found {len(bm_res)} candidates")

        # Fusion
        # Weights: Semantic=1.2, MinHash=1.5 (High precision on skills), BM25=1.0
        return self.reciprocal_rank_fusion([sem_res, mh_res, bm_res], [1.2, 1.5, 1.0])


# ------------------------------------------------------------
# Helper: Get File Path
# ------------------------------------------------------------
def get_pdf_path():
    if len(sys.argv) > 1:
        return sys.argv[1]

    print(">>> Opening file selector...")

    root = tk.Tk()
    root.withdraw()

    path = filedialog.askopenfilename(
        title="Select your CV (PDF)",
        filetypes=[("PDF Files", "*.pdf"), ("All Files", "*.*")]
    )

    root.destroy()

    if not path:
        print("[!] No file selected. Exiting.")
        sys.exit(0)

    print(f">>> Selected: {path}")
    return path


# ------------------------------------------------------------
# Main Execution
# ------------------------------------------------------------
if __name__ == "__main__":
    # 1. Setup
    matcher = HybridJDCVMatching(es, client)
    matcher.create_index()
    matcher.ingest_mock_data()  # <--- INJECT MOCK DATA ONLY

    # 2. Get CV File
    pdf_path = get_pdf_path()

    if not os.path.exists(pdf_path):
        print(f"ERROR: File does not exist: {pdf_path}")
        sys.exit(1)

    try:
        # 3. Extract
        print(f"INFO: Extracting from: {os.path.basename(pdf_path)}")
        extractor = ResumeExtractor(test=0)
        cv_data = extractor.extract(pdf_path)

        print(f"   Detected Title: {cv_data.get('title')}")
        print(f"   Detected Skills: {cv_data.get('skills')[:5]}...")  # Show first 5

        # 4. Search
        results = matcher.hybrid_search(cv_data)

        # 5. Display
        print("\n" + "=" * 60)
        print("FINAL MATCHING RESULTS (Top Jobs)")
        print("=" * 60)

        if not results:
            print("No matches found.")

        for i, r in enumerate(results[:FINAL_K], 1):
            print(f"\n{i}. {r['title']}")
            print(f"   ID: {r['id']} | RRF Score: {r['rrf_score']:.5f}")
            print(f"   Skills in JD: {r['skills']}")
            print(f"   Why matched?: {json.dumps(r['sources'], indent=0)}")

    except Exception as e:
        print(f"\nCRITICAL ERROR: {e}")
        import traceback

        traceback.print_exc()