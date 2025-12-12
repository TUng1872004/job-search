import os
import json
import logging
from typing import Optional, Dict, Any, List

from dotenv import load_dotenv
from pydantic import BaseModel, Field
from openai import OpenAI
from docling.document_converter import DocumentConverter

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')


class ResumeData(BaseModel):
    """Internal Pydantic schema for structured output enforcement."""
    title: str = Field(..., description="The candidate's current professional title.")
    skills: List[str] = Field(..., description="A list of technical hard skills extracted from the resume.")
    description: str = Field(..., description="A short professional summary/bio.")
    experience: str = Field(..., description="A condensed summary of work history.")


class ResumeExtractor:
    def __init__(self, model_name: str = "gpt-4o-mini", test: int = 0):
        """
        Initialize the extractor with OpenAI.
        """
        load_dotenv()
        self.test = test
        self.api_key = os.getenv("OPENAI_API_KEY")
        if not self.api_key:
            raise ValueError("OPENAI_API_KEY not found in environment variables.")

        self.client = OpenAI(api_key=self.api_key)
        self.model_name = model_name
        self.converter = DocumentConverter()
        self.file_name = None

    def _convert_pdf_to_markdown(self, file_path: str) -> str:
        """Internal: Convert PDF to Markdown using Docling."""
        logging.info(f"Converting PDF: {file_path}")
        try:
            result = self.converter.convert(file_path)
            markdown_text = result.document.export_to_markdown()

            if self.test == 1:
                # Ensure directory exists
                os.makedirs("test/raw", exist_ok=True)
                with open(f"test/raw/{self.file_name}.md", "w", encoding="utf-8") as f:
                    f.write(markdown_text)
            return markdown_text
        except Exception as e:
            logging.error(f"Docling conversion failed: {e}")
            raise

    def _parse_markdown_with_openai(self, markdown_text: str) -> Optional[ResumeData]:
        """Internal: Send markdown to OpenAI for structured parsing."""
        logging.info("Sending content to OpenAI for extraction...")

        system_prompt = "You are an expert HR Data Parser. Extract structured information from the provided Resume Markdown text."

        try:
            completion = self.client.beta.chat.completions.parse(
                model=self.model_name,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": markdown_text},
                ],
                response_format=ResumeData,
            )

            parsed_data = completion.choices[0].message.parsed

            if not parsed_data.experience:
                logging.warning("Extraction returned empty experience field.")
                parsed_data.experience = "N/A"

            return parsed_data

        except Exception as e:
            logging.error(f"OpenAI extraction failed: {e}")
            return None

    def extract(self, file_path: str) -> Dict[str, Any]:
        """
        The Main Method.
        Args:
            file_path: Path to the PDF file.
        Returns:
            Dictionary containing 'title', 'skills', 'description', 'experience'.
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        self.file_name = os.path.basename(file_path).split('.')[0]

        # 1. Convert PDF -> Markdown
        markdown_text = self._convert_pdf_to_markdown(file_path)

        # 2. Extract Structure
        resume_obj = self._parse_markdown_with_openai(markdown_text)

        if not resume_obj:
            raise RuntimeError("Extraction returned empty result.")

        result = resume_obj.model_dump()

        if self.test == 1:
            os.makedirs("test/res", exist_ok=True)
            with open(f"test/res/{self.file_name}.json", "w", encoding="utf-8") as f:
                json.dump(result, f, ensure_ascii=False, indent=2)

        return result


if __name__ == "__main__":
    # 1. Init
    extractor = ResumeExtractor(test=1)

    # 2. Call
    try:
        # Example path - ensure this file exists or change path
        path = "./data/Economy/SALES/10724818.pdf"

        if os.path.exists(path):
            data = extractor.extract(path)
            print("\n[INFO] Extraction Complete:")
            print(json.dumps(data, indent=2))
        else:
            print(f"[ERROR] Test file not found at: {path}")

    except Exception as e:
        print(f"[ERROR] {e}")