#!/usr/bin/env python3
"""
Hybrid JD-CV Matching System
- Features:
  - Drag & Drop PDF support (GUI)
  - Auto-ingest Mock Data
  - Hybrid Search:
    1. Semantic Search on Description
    2. Semantic Search on Experience
    3. MinHash LSH for Skills (Exact Jaccard verify)
    4. BM25 for Title
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
from datasketch import MinHash, MinHashLSH
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
INDEX_NAME = os.getenv("ES_INDEX", "job_postings")

EMBEDDING_MODEL = "text-embedding-3-small"
NUM_PERM = 128
LSH_THRESHOLD = 0.05

# Fusion params
TOP_K_PER_SOURCE = 10
FINAL_K = 5
RRF_K = 60

# Weights from Script B
WEIGHT_SEMANTIC_DESC = 1.2
WEIGHT_SEMANTIC_EXP = 0.8
WEIGHT_MINHASH = 1.5
WEIGHT_BM25 = 1.0

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
        
        # Initialize LSH Index in memory for skills
        self.lsh = MinHashLSH(threshold=LSH_THRESHOLD, num_perm=NUM_PERM)
        self.local_minhashes = {} 

    # --- Helpers ---
    def embed_text(self, text: str) -> List[float]:
        """Generate embedding vector via OpenAI API."""
        if not text: return [0.0] * 1536
        try:
            resp = self.client.embeddings.create(model=EMBEDDING_MODEL, input=text)
            return resp.data[0].embedding
        except Exception as e:
            print(f"Embedding error: {e}")
            return [0.0] * 1536

    def generate_minhash(self, tokens: List[str], num_perm: int = NUM_PERM) -> MinHash:
        """Generate MinHash object from list of tokens (skills)."""
        m = MinHash(num_perm=num_perm)
        if not tokens: 
            return m
        # Normalize logic
        tokens_set = set(t.lower().strip() for t in tokens if t.strip())
        for t in tokens_set:
            m.update(t.encode("utf-8"))
        return m

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
        """Generates embeddings (Desc + Exp) and hashes, inserts to ES and LSH."""
        print("INFO: Generating mock data...")

        mock_jobs = [
            {
                "title": "Senior Python Backend Engineer",
                "skills": ["Python", "Django", "FastAPI", "PostgreSQL", "Docker", "AWS", "Redis"],
                "description": "We are looking for a Python expert to build scalable APIs. Experience with microservices.",
                "experience": "5+ years of backend development experience. Proven track record in high-load systems."
            },
            {
                "title": "Sales Manager (B2B)",
                "skills": ["Sales", "Negotiation", "CRM", "Salesforce", "Lead Generation"],
                "description": "Drive revenue growth by managing sales teams and closing B2B deals.",
                "experience": "7 years in sales, specifically B2B software. Management experience required."
            },
            {
                "title": "Data Analyst",
                "skills": ["SQL", "Tableau", "Python", "Excel", "Data Visualization"],
                "description": "Analyze large datasets to provide actionable insights for the business.",
                "experience": "3 years of data analysis experience in a fintech environment."
            },
            {
                "title": "Frontend React Developer",
                "skills": ["Javascript", "React", "Redux", "HTML", "CSS", "TypeScript"],
                "description": "Build responsive UI components. Strong understanding of DOM.",
                "experience": "3 years frontend development. Experience with React Hooks and State Management."
            }
        ]

        actions = []
        for i, job in enumerate(mock_jobs):
            doc_id = f"mock_job_{i}"
            
            # 1. Generate Vectors (Desc + Exp)
            desc_vec = self.embed_text(job.get('description', ''))
            exp_vec = self.embed_text(job.get('experience', '')) # NEW

            # 2. Generate MinHash Object
            m = self.generate_minhash(job.get('skills', []))
            
            # 3. Insert into LSH (In-Memory)
            self.lsh.insert(doc_id, m)
            self.local_minhashes[doc_id] = m

            # 4. Prepare ES Document
            doc = {
                "_index": INDEX_NAME,
                "_id": doc_id,
                "_source": {
                    "doc_type": "job",
                    "title": job['title'],
                    "skills": job['skills'],
                    "description": job['description'],
                    "description_vector": desc_vec,
                    "experience": job['experience'],       # NEW
                    "experience_vector": exp_vec           # NEW
                }
            }
            actions.append(doc)

        helpers.bulk(self.es, actions)
        print(f"SUCCESS: Ingested {len(actions)} jobs.")
        time.sleep(1)

    # --- Search Methods ---
    def semantic_search(self, vector: List[float], field: str, top_k: int) -> List[Dict]:
        """Generic semantic search for any vector field."""
        if not vector or all(v == 0.0 for v in vector):
            return []
            
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
        return [{"id": h["_id"], "score": h["_score"], "source": f"sem_{field}", **h["_source"]} 
                for h in resp["hits"]["hits"]]

    def lsh_search(self, cv_skills: List[str], top_k: int) -> List[Dict]:
        """Uses MinHashLSH to find candidates, then computes exact Jaccard."""
        query_m = self.generate_minhash(cv_skills)
        candidate_ids = self.lsh.query(query_m)

        print(f"LSH found {len(candidate_ids)} candidates for skills: {cv_skills}")
        
        if not candidate_ids: return []

        scored_candidates = []
        for doc_id in candidate_ids:
            if doc_id in self.local_minhashes:
                candidate_m = self.local_minhashes[doc_id]
                jaccard_score = query_m.jaccard(candidate_m)
                scored_candidates.append((doc_id, jaccard_score))
        
        scored_candidates.sort(key=lambda x: x[1], reverse=True)
        top_candidates = scored_candidates[:top_k]
        
        if not top_candidates: return []

        ids_only = [x[0] for x in top_candidates]
        scores_map = {x[0]: x[1] for x in top_candidates}
        
        body = {"query": {"ids": {"values": ids_only}}}
        resp = self.es.search(index=INDEX_NAME, body=body)
        
        results = []
        for h in resp["hits"]["hits"]:
            did = h["_id"]
            results.append({
                "id": did,
                "score": scores_map.get(did, 0.0),
                "source": "lsh_minhash",
                **h["_source"]
            })
        return sorted(results, key=lambda x: x['score'], reverse=True)

    def bm25_search(self, text: str, top_k: int) -> List[Dict]:
        if not text: return []
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
                    meta[did] = {
                        "title": item.get("title"), 
                        "skills": item.get("skills"), 
                        "experience": item.get("experience"),
                        "sources": {}
                    }
                meta[did]["sources"][item["source"]] = {"rank": rank, "raw_score": item["score"]}

        final = []
        for did, score in scores.items():
            final.append({
                "id": did,
                "rrf_score": score,
                "title": meta[did]["title"],
                "skills": meta[did]["skills"],
                "experience": meta[did]["experience"],
                "sources": meta[did]["sources"]
            })

        return sorted(final, key=lambda x: x["rrf_score"], reverse=True)

    def hybrid_search(self, cv_data: Dict) -> List[Dict]:
        print("\n--- Running Hybrid Search Components ---")

        # 1. Semantic Description
        desc_vec = self.embed_text(cv_data.get("description", ""))
        sem_desc_res = self.semantic_search(desc_vec, "description_vector", TOP_K_PER_SOURCE)
        print(f"1. Semantic (Desc): Found {len(sem_desc_res)} candidates")

        # 2. Semantic Experience
        exp_vec = self.embed_text(cv_data.get("experience", ""))
        sem_exp_res = self.semantic_search(exp_vec, "experience_vector", TOP_K_PER_SOURCE)
        print(f"2. Semantic (Exp): Found {len(sem_exp_res)} candidates")

        # 3. LSH MinHash Skills
        mh_res = self.lsh_search(cv_data.get("skills", []), TOP_K_PER_SOURCE)
        print(f"3. LSH (Skills): Found {len(mh_res)} candidates")

        # 4. BM25 Title
        bm_res = self.bm25_search(cv_data.get("title", ""), TOP_K_PER_SOURCE)
        print(f"4. BM25 (Title): Found {len(bm_res)} candidates")

        # Fusion
        # Order: [Desc, Exp, MinHash, BM25]
        return self.reciprocal_rank_fusion(
            [sem_desc_res, sem_exp_res, mh_res, bm_res], 
            [WEIGHT_SEMANTIC_DESC, WEIGHT_SEMANTIC_EXP, WEIGHT_MINHASH, WEIGHT_BM25]
        )
    
    def load_skills_to_lsh(self):
        data = self.es.search(index=INDEX_NAME, body={
            "query": {
                "term": {
                    "doc_type": "job"
                }
            },
            "size": 1000
        })
        for hit in data['hits']['hits']:
            doc_id = hit['_id']
            skills = hit['_source'].get('skills', [])
            m = self.generate_minhash(skills)
            self.lsh.insert(doc_id, m)
            self.local_minhashes[doc_id] = m

        print("Number of query from elasticsearch:", len(data['hits']['hits']))

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
    return path



# ------------------------------------------------------------
# Main Execution
# ------------------------------------------------------------
if __name__ == "__main__":
    # 1. Setup
    matcher = HybridJDCVMatching(es, client)
    matcher.load_skills_to_lsh()
    
    # # Run once to setup index with new schema
    matcher.create_index()
    # matcher.ingest_mock_data()

    # 2. Get CV File
    pdf_path = get_pdf_path()
    if not pdf_path or not os.path.exists(pdf_path):
        print("No file selected or file not found.")
        sys.exit(0)

    try:
        # 3. Extract
        print(f"INFO: Extracting from: {os.path.basename(pdf_path)}")
        extractor = ResumeExtractor(test=0)
        cv_data = extractor.extract(pdf_path)

        print(f"   Title: {cv_data.get('title')}")
        print(f"   Skills: {cv_data.get('skills')}...")

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
            print(f"   Experience in JD: {r['experience'][:100]}...")
            print(f"   Why matched?: {json.dumps(r['sources'], indent=2)}")

    except Exception as e:
        print(f"\nCRITICAL ERROR: {e}")
        import traceback
        traceback.print_exc()