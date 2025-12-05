import os
import json
import logging
from typing import Optional, Dict, Any

from dotenv import load_dotenv
from pydantic import BaseModel, Field
import google.generativeai as genai
from docling.document_converter import DocumentConverter

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')


class ResumeData(BaseModel):
    """Internal Pydantic schema for structured output enforcement."""
    title: str = Field(..., description="The candidate's current professional title.")
    skills: list[str] = Field(..., description="A list of technical hard skills extracted from the resume.")
    description: str = Field(..., description="A short professional summary/bio.")
    experience: str = Field(..., description="A condensed summary of work history.")


class ResumeExtractor:
    def __init__(self, model_name: str = "gemini-2.5-flash", test :int =0):
        """
        Initialize the extractor. 
        Ensures API key exists and sets up the converter.
        """
        load_dotenv()
        self.test =test
        self.api_key = os.getenv("GOOGLE_API_KEY")
        if not self.api_key:
            raise ValueError("GOOGLE_API_KEY not found in environment variables.")
            
        genai.configure(api_key=self.api_key)
        self.model = genai.GenerativeModel(model_name)
        self.converter = DocumentConverter()

        self.file_name = None

    def _convert_pdf_to_markdown(self, file_path: str) -> str:
        """Internal: Convert PDF to Markdown using Docling."""
        logging.info(f"Converting PDF: {file_path}")
        try:
            result = self.converter.convert(file_path)
            result =  result.document.export_to_markdown()
             
        
            if self.test == 1:
                with open(f"test/raw/{self.file_name}.md", "w", encoding="utf-8") as f:
                    f.write(result)
            return result
        except Exception as e:
            logging.error(f"Docling conversion failed: {e}")
            raise

    def _parse_markdown_with_gemini(self, markdown_text: str) -> Optional[ResumeData]:
        """Internal: Send markdown to Gemini for structured parsing."""
        logging.info("Sending content to Gemini...")
        prompt = f"""
        You are an expert HR Data Parser. 
        Extract information from the following Resume Markdown text.
        Expected output:

        RESUME MARKDOWN:
        {markdown_text}
        """

        try:
            result = self.model.generate_content(
                prompt,
                generation_config=genai.GenerationConfig(
                    response_mime_type="application/json",
                    response_schema=ResumeData
                )
            )
            result = result.text

            result = json.loads(result)

            if result.get("experience") is None:
                logging.warning("Extraction returned empty title.")
                result["experience"] = "N/A"
            return ResumeData.model_validate(result)
        except Exception as e:
            logging.error(f"Gemini extraction failed: {e}")
            if self.test == 1:
                with open(f"test/error/{self.file_name}.txt", "w", encoding="utf-8") as f:
                        f.write(str(result))
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

        resume_obj = self._parse_markdown_with_gemini(markdown_text)
        result = resume_obj.model_dump()
        if not resume_obj:
            raise RuntimeError("Extraction returned empty result.")
        if self.test == 1:
            with open(f"test/res/{self.file_name}.json", "w", encoding="utf-8") as f:
                        json.dump(result, f, ensure_ascii=False, indent=2)

        return result


if __name__ == "__main__":
    # 1. Init
    extractor = ResumeExtractor(test=1)

    # 2. Call
    try:
        path = "./data/Economy/SALES/10724818.pdf"
        
        data = extractor.extract(path)
        
        print("\n✅ Extraction Complete:")
        print(json.dumps(data, indent=2))
        
    except Exception as e:
        print(f"❌ Error: {e}")