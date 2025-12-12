
from dotenv import load_dotenv
from pydantic import BaseModel, Field
from typing import Dict, Any, Optional


import os
import google.generativeai as genai
from ollama import chat

import json


class ResumeData(BaseModel):
    """Internal Pydantic schema for structured output enforcement."""
    title: str = Field(..., description="The candidate's current professional title.")
    skills: list[str] = Field(..., description="A list of technical hard skills extracted from the resume.")
    description: str = Field(..., description="A short professional summary/bio.")
    experience: str = Field(..., description="A condensed summary of work history.")

class GeminiModel:
    def __init__(self, model_name: str = "gemini-2.5-flash"):
        load_dotenv()
        self.api_key = os.getenv("GOOGLE_API_KEY")
        if not self.api_key:
            raise ValueError("GOOGLE_API_KEY not found in environment variables.")
        genai.configure(api_key=self.api_key)
        self.model = genai.GenerativeModel(model_name)
    def run(self, markdown_text: str):

        prompt = f"""
        You are an expert HR Data Parser. 
        Extract information from the following Resume Markdown text.
        Expected output:

        RESUME MARKDOWN:
        {markdown_text}
        """
        result = self.model.generate_content(
                prompt,
                generation_config=genai.GenerationConfig(
                    response_mime_type="application/json",
                    response_schema=ResumeData
                )
            )
        result = result.text
        return json.loads(result)
    
class LocalModel:
    """
    Fixed implementation using Ollama Native Structured Outputs.
    Requires: 'pip install ollama' and 'ollama run qwen2.5:1.5b'
    """

    def __init__(self, model_name: str = "qwen2.5:1.5b"):
        # No file paths needed. Ollama manages models by name.
        
        self.model_name = model_name 

    def run(self, text_content: str) -> Dict[str, Any]:
        print(f"DEBUG: Running local inference with {self.model_name}...")
        
        try:
            response = chat(
                model=self.model_name,
                messages=[
                    {
                        'role': 'user',
                        'content': f"Extract resume data from this text:\n\n{text_content}"
                    }
                ],
                # This forces the output to match the Pydantic schema exactly
                format=ResumeData.model_json_schema(), 
                options={
                    "temperature": 0.2,
                    "num_ctx": 4096 
                }
            )
            print("=============================== DEBUG =============================== \n ",response)
            
            return json.loads(response.message.content)

        except Exception as e:
            print(f"Local Inference Error: {e}")
            return {}
        
class Classifier:
    def __init__(self):
        raise NotImplementedError("This model is on hold for future implementation. Expect a <500MB lightweight text classifier.")
    def run(self, text_content: str) -> Dict[str, Any]:
        print("Running dummy classifier...")
        return {"category": "General", "confidence": 0.95}
    
MODEL = {
    "gemini": GeminiModel,
    "local": LocalModel,
    "classifier": Classifier
    }
def setup(model_name: str = "gemini"):
    """
    Factory function to instantiate the LLM backend.
    """
    if model_name not in MODEL:
        raise ValueError(f"Model {model_name} not supported.")
        
    return MODEL[model_name]()