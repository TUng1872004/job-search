#!/usr/bin/env python3
"""
Hybrid JD-CV Matching System (Refactored to Class)
- Unified schema for JD and CV with matchable fields
- MinHash for skills matching (hard skills)
- Semantic for description/experience (context understanding)
- BM25 for title matching (exact match)
- Reciprocal Rank Fusion (RRF)
"""

import os
import time
from typing import List, Dict, Any
from collections import defaultdict
from pprint import pprint

from dotenv import load_dotenv
from datasketch import MinHash
from openai import OpenAI
from elasticsearch import Elasticsearch, helpers

# ------------------------------------------------------------
# Config & Constants (Giữ nguyên bên ngoài)
# ------------------------------------------------------------
load_dotenv()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    raise RuntimeError("OPENAI_API_KEY missing. Put it in .env or environment.")

ES_HOST = os.getenv("ES_HOST", "http://localhost:9200")
INDEX_NAME = os.getenv("INDEX_NAME", "job_postings")
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "text-embedding-3-small")
NUM_PERM = int(os.getenv("NUM_PERM", "128"))

# Fusion params (Giữ nguyên)
TOP_K_PER_SOURCE = 10
FINAL_K = 10
RRF_K = 60
WEIGHT_SEMANTIC = 1.2  # Tăng trọng số semantic (context quan trọng)
WEIGHT_MINHASH = 1.5   # Tăng trọng số MinHash (skills là then chốt)
WEIGHT_BM25 = 1.0      # BM25 cho title matching

# ------------------------------------------------------------
# Clients (Giữ nguyên khởi tạo bên ngoài)
# ------------------------------------------------------------
client = OpenAI(api_key=OPENAI_API_KEY)
es = Elasticsearch(ES_HOST)

print("Elasticsearch info:")
try:
    info = es.info()
    pprint(info.body if hasattr(info, "body") else info)
except Exception as e:
    raise RuntimeError(f"Cannot connect to Elasticsearch at {ES_HOST}: {e}")


# ------------------------------------------------------------
# Class Definition
# ------------------------------------------------------------
class HybridJDCVMatching:
    def __init__(self, es_client: Elasticsearch, openai_client: OpenAI):
        """
        Inject dependencies để class hoạt động.
        Các biến config khác (INDEX_NAME, NUM_PERM...) vẫn lấy từ global constant 
        để đảm bảo không thay đổi logic mặc định.
        """
        self.es = es_client
        self.client = openai_client

    # ------------------------------------------------------------
    # Helpers: Embedding + MinHash
    # ------------------------------------------------------------
    def embed_text(self, text: str) -> List[float]:
        """Generate embedding vector via OpenAI API."""
        resp = self.client.embeddings.create(model=EMBEDDING_MODEL, input=text)
        return resp.data[0].embedding

    def generate_minhash_signature(self, tokens: List[str], num_perm: int = NUM_PERM) -> List[str]:
        """
        Generate MinHash signature from list of tokens (skills).
        CRITICAL: Only use this for skill lists, not full text.
        """
        tokens_set = set(t.lower().strip() for t in tokens if t.strip())
        m = MinHash(num_perm=num_perm)
        for t in tokens_set:
            m.update(t.encode("utf-8"))
        return [f"hash_{v}" for v in m.hashvalues]

    # ------------------------------------------------------------
    # Unified Index Schema (for both JD and CV)
    # ------------------------------------------------------------
    def create_index(self, index_name: str = INDEX_NAME):
        """
        Create unified index schema following Best Practice:
        - title: text (BM25)
        - skills: keyword array + MinHash signature
        - description_vector: dense_vector (Semantic)
        - experience_vector: dense_vector (Semantic for work experience)
        - location, type: keyword (hard filter)
        """
        if self.es.indices.exists(index=index_name):
            print(f"Index '{index_name}' already exists. Skipping creation.")
            return

        mapping = {
            "mappings": {
                "properties": {
                    "doc_type": {"type": "keyword"},  # "job" or "cv"

                    # Title (BM25 with high boost)
                    "title": {
                        "type": "text",
                        "analyzer": "standard",
                        "fields": {
                            "keyword": {"type": "keyword"}
                        }
                    },

                    # Skills (MinHash + BM25 backup)
                    "skills": {"type": "keyword"},  # Array of skills
                    "skills_signature": {"type": "keyword"},  # MinHash signature
                    "skills_text": {"type": "text"},  # For BM25 backup

                    # Description/Responsibilities (Semantic)
                    "description": {"type": "text"},
                    "description_vector": {
                        "type": "dense_vector",
                        "dims": 1536,
                        "index": True,
                        "similarity": "cosine"
                    },

                    # Work Experience (Semantic)
                    "experience": {"type": "text"},
                    "experience_vector": {
                        "type": "dense_vector",
                        "dims": 1536,
                        "index": True,
                        "similarity": "cosine"
                    },

                    # Education/Certifications (BM25)
                    "education": {"type": "text"},

                    # Hard filters
                    "location": {"type": "keyword"},
                    "job_type": {"type": "keyword"},  # Full-time, Remote, etc.
                    "experience_years": {"type": "integer"},

                    # Metadata
                    "metadata": {"type": "object"}
                }
            }
        }

        self.es.indices.create(index=index_name, body=mapping)
        print(f"Index '{index_name}' created with unified schema.")

    # ------------------------------------------------------------
    # Indexing Functions
    # ------------------------------------------------------------
    def index_job_description(self, jd: Dict[str, Any], index_name: str = INDEX_NAME):
        """
        Index a Job Description with proper field mapping.
        """
        doc_id = jd["id"]

        # Extract fields
        title = jd.get("title", "")
        skills = jd.get("required_skills", [])
        description = jd.get("description", "")
        experience = jd.get("work_experience", "")

        # Generate embeddings (Semantic)
        description_vector = self.embed_text(description) if description else [0.0] * 1536
        experience_vector = self.embed_text(experience) if experience else [0.0] * 1536

        # Generate MinHash signature (from skills only!)
        skills_signature = self.generate_minhash_signature(skills) if skills else []
        skills_text = " ".join(skills)  # For BM25 backup

        document = {
            "doc_type": "job",
            "title": title,
            "skills": skills,
            "skills_signature": skills_signature,
            "skills_text": skills_text,
            "description": description,
            "description_vector": description_vector,
            "experience": experience,
            "experience_vector": experience_vector,
            "location": jd.get("location", ""),
            "job_type": jd.get("job_type", ""),
            "experience_years": jd.get("min_experience_years", 0),
            "metadata": {
                "source": "job_description",
                "indexed_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            }
        }

        self.es.index(index=index_name, id=doc_id, document=document)
        print(f"Indexed JD: {doc_id} - {title}")

    def index_jobs_bulk(self, jobs: List[Dict[str, Any]], index_name: str = INDEX_NAME):
        """Bulk index multiple job descriptions."""
        actions = []
        for jd in jobs:
            doc_id = jd["id"]
            title = jd.get("title", "")
            skills = jd.get("required_skills", [])
            description = jd.get("description", "")
            experience = jd.get("work_experience", "")

            # Embeddings
            description_vector = self.embed_text(description) if description else [0.0] * 1536
            experience_vector = self.embed_text(experience) if experience else [0.0] * 1536

            # MinHash from skills only
            skills_signature = self.generate_minhash_signature(skills) if skills else []
            skills_text = " ".join(skills)

            document = {
                "doc_type": "job",
                "title": title,
                "skills": skills,
                "skills_signature": skills_signature,
                "skills_text": skills_text,
                "description": description,
                "description_vector": description_vector,
                "experience": experience,
                "experience_vector": experience_vector,
                "location": jd.get("location", ""),
                "job_type": jd.get("job_type", ""),
                "experience_years": jd.get("min_experience_years", 0),
                "metadata": {"source": "job_description", "indexed_at": time.strftime("%Y-%m-%dT%H:%M:%SZ")}
            }

            actions.append({
                "_op_type": "index",
                "_index": index_name,
                "_id": doc_id,
                "_source": document
            })

        helpers.bulk(self.es, actions)
        print(f"Bulk indexed {len(actions)} job descriptions.")

    # ------------------------------------------------------------
    # Component Searches
    # ------------------------------------------------------------
    def semantic_search_by_field(self, query_vector: List[float],
                                 field: str = "description_vector",
                                 top_k: int = TOP_K_PER_SOURCE) -> List[Dict[str, Any]]:
        """
        Semantic search using specific vector field.
        field can be: "description_vector" or "experience_vector"
        """
        body = {
            "size": top_k,
            "query": {
                "script_score": {
                    "query": {"term": {"doc_type": "job"}},
                    "script": {
                        "source": f"cosineSimilarity(params.query_vector, '{field}') + 1.0",
                        "params": {"query_vector": query_vector}
                    }
                }
            }
        }
        resp = self.es.search(index=INDEX_NAME, body=body)
        results = []
        for hit in resp["hits"]["hits"]:
            results.append({
                "id": hit["_id"],
                "score": hit["_score"],
                "source": f"semantic_{field}",
                "title": hit["_source"].get("title"),
                "skills": hit["_source"].get("skills"),
                "description": hit["_source"].get("description")
            })
        return results

    def minhash_search(self, query_signature: List[str], top_k: int = TOP_K_PER_SOURCE) -> List[Dict[str, Any]]:
        """
        MinHash search: count matching hash tokens.
        This is THE KEY for skills matching!
        """
        body = {
            "size": top_k,
            "query": {
                "script_score": {
                    "query": {"term": {"doc_type": "job"}},
                    "script": {
                        "source": """
                            int matches = 0;
                            for (int i = 0; i < params.hashes.length; ++i) {
                                if (doc['skills_signature'].contains(params.hashes[i])) {
                                    matches += 1;
                                }
                            }
                            return matches;
                        """,
                        "params": {"hashes": query_signature}
                    }
                }
            }
        }
        resp = self.es.search(index=INDEX_NAME, body=body)
        results = []
        for hit in resp["hits"]["hits"]:
            results.append({
                "id": hit["_id"],
                "score": hit["_score"],
                "source": "minhash",
                "title": hit["_source"].get("title"),
                "skills": hit["_source"].get("skills"),
                "description": hit["_source"].get("description")
            })
        return results

    def bm25_search_title(self, query_text: str, top_k: int = TOP_K_PER_SOURCE) -> List[Dict[str, Any]]:
        """
        BM25 search on title field with high boost.
        Use this for exact job title matching.
        """
        body = {
            "size": top_k,
            "query": {
                "bool": {
                    "must": [
                        {"term": {"doc_type": "job"}},
                        {"match": {"title": {"query": query_text, "boost": 2.0}}}
                    ]
                }
            }
        }
        resp = self.es.search(index=INDEX_NAME, body=body)
        results = []
        for hit in resp["hits"]["hits"]:
            results.append({
                "id": hit["_id"],
                "score": hit["_score"],
                "source": "bm25_title",
                "title": hit["_source"].get("title"),
                "skills": hit["_source"].get("skills"),
                "description": hit["_source"].get("description")
            })
        return results

    # ------------------------------------------------------------
    # Reciprocal Rank Fusion
    # ------------------------------------------------------------
    def reciprocal_rank_fusion(self, lists_of_results: List[List[Dict[str, Any]]],
                               weights: List[float],
                               rrf_k: float = RRF_K) -> List[Dict[str, Any]]:
        """RRF fusion with configurable weights per source."""
        assert len(lists_of_results) == len(weights)
        score_map = defaultdict(float)
        info_map = {}

        for lst_idx, results in enumerate(lists_of_results):
            w = weights[lst_idx]
            for rank, item in enumerate(results, start=1):
                docid = item["id"]
                contribution = w * (1.0 / (rrf_k + rank))
                score_map[docid] += contribution

                if docid not in info_map:
                    info_map[docid] = {
                        "title": item.get("title"),
                        "skills": item.get("skills"),
                        "description": item.get("description"),
                        "sources": {item["source"]: {"rank": rank, "score": item["score"]}}
                    }
                else:
                    info_map[docid]["sources"][item["source"]] = {"rank": rank, "score": item["score"]}

        merged = []
        for docid, agg_score in score_map.items():
            merged.append({
                "id": docid,
                "rrf_score": agg_score,
                "title": info_map[docid].get("title"),
                "skills": info_map[docid].get("skills"),
                "description": info_map[docid].get("description"),
                "sources": info_map[docid]["sources"]
            })

        merged.sort(key=lambda x: x["rrf_score"], reverse=True)
        return merged

    # ------------------------------------------------------------
    # Hybrid Search Orchestration
    # ------------------------------------------------------------
    def hybrid_search_cv_to_jobs(self, cv_data: Dict[str, Any],
                                 top_k_per_source: int = TOP_K_PER_SOURCE,
                                 final_k: int = FINAL_K) -> tuple:
        """
        Given a CV (parsed), find matching jobs using hybrid search.
        """

        title = cv_data.get("title", "")
        skills = cv_data.get("skills", [])
        description = cv_data.get("description", "")
        experience = cv_data.get("experience", "")

        print("\n=== Starting Hybrid Search ===")

        # 1. Semantic search on description
        print("1. Semantic search (description)...")
        desc_vector = self.embed_text(description) if description else None
        semantic_desc = self.semantic_search_by_field(desc_vector, "description_vector",
                                                      top_k_per_source) if desc_vector else []

        # 2. Semantic search on experience
        print("2. Semantic search (experience)...")
        exp_vector = self.embed_text(experience) if experience else None
        semantic_exp = self.semantic_search_by_field(exp_vector, "experience_vector",
                                                     top_k_per_source) if exp_vector else []

        # 3. MinHash search on skills (THE MOST IMPORTANT!)
        print("3. MinHash search (skills matching)...")
        skills_signature = self.generate_minhash_signature(skills) if skills else []
        minhash_results = self.minhash_search(skills_signature, top_k_per_source) if skills_signature else []

        # 4. BM25 search on title
        print("4. BM25 search (title matching)...")
        bm25_results = self.bm25_search_title(title, top_k_per_source) if title else []

        # 5. Fusion
        print("5. Performing RRF fusion...")
        all_results = [semantic_desc, semantic_exp, minhash_results, bm25_results]
        weights = [WEIGHT_SEMANTIC, WEIGHT_SEMANTIC * 0.8, WEIGHT_MINHASH, WEIGHT_BM25]

        merged = self.reciprocal_rank_fusion(all_results, weights, rrf_k=RRF_K)

        return merged[:final_k], {
            "semantic_desc": semantic_desc,
            "semantic_exp": semantic_exp,
            "minhash": minhash_results,
            "bm25": bm25_results
        }


# ------------------------------------------------------------
# Demo (Logic giữ nguyên, chỉ gọi qua class instance)
# ------------------------------------------------------------
if __name__ == "__main__":
    # KHỞI TẠO CLASS
    matcher = HybridJDCVMatching(es_client=es, openai_client=client)

    # # 1. Create index
    # matcher.create_index()

    # 4. Mock CV (parsed)
    cv_data = {
        "title": "Backend Developer with 4 years experience",
        "skills": ["Python", "Django", "REST API", "PostgreSQL", "Redis", "Docker", "Git", "AWS"],
        "description": "Experienced backend developer passionate about building scalable APIs and working with databases.",
        # "experience": "4 years developing microservices at tech startups. Built payment processing system handling 10k+ transactions daily. Worked with Django, PostgreSQL, Redis caching, and deployed on AWS with Docker."
    }

    print("\n=== Searching Jobs for CV ===")
    print(f"CV Title: {cv_data['title']}")
    print(f"CV Skills: {cv_data['skills']}")

    # 5. Search (Gọi qua matcher instance)
    merged_results, components = matcher.hybrid_search_cv_to_jobs(cv_data, top_k_per_source=5, final_k=5)

    # 6. Display results (Giữ nguyên cấu trúc in ấn)
    print("\n" + "=" * 60)
    print("FINAL RANKED RESULTS (Top Jobs)")
    print("=" * 60)
    for i, r in enumerate(merged_results, start=1):
        print(f"\n{i}. {r['title']} (ID: {r['id']})")
        print(f"   RRF Score: {r['rrf_score']:.4f}")
        print(f"   Required Skills: {r.get('skills', [])}")
        print(f"   Matching Sources:")
        for source, info in r["sources"].items():
            print(f"     - {source}: rank={info['rank']}, score={info['score']:.2f}")

    print("\n" + "=" * 60)
    print("Component Analysis")
    print("=" * 60)
    print(f"\nMinHash (Skills Match) - Top 3:")
    for i, r in enumerate(components["minhash"][:3], start=1):
        print(f"{i}. {r['title']} - Score: {r['score']:.0f} hash matches")

    print(f"\nSemantic (Description) - Top 3:")
    for i, r in enumerate(components["semantic_desc"][:3], start=1):
        print(f"{i}. {r['title']} - Score: {r['score']:.4f}")

    print(f"\nBM25 (Title Match) - Top 3:")
    for i, r in enumerate(components["bm25"][:3], start=1):
        print(f"{i}. {r['title']} - Score: {r['score']:.4f}")