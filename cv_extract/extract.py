import os
import json
import logging
from typing import Optional, Dict, Any


import google.generativeai as genai
from docling.document_converter import DocumentConverter

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

from model import setup, ResumeData




class ResumeExtractor:
    def __init__(self, model_name: str = "gemini", test : int =0):
        """
        Initialize the extractor. 
        Ensures API key exists and sets up the converter.
        """

        self.model = setup(model_name)
        self.converter = DocumentConverter()
        self.test = test
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

        try:
            result = self.model.run(markdown_text)

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
    MODEL = ["gemini", "local", "classifier"]
    extractor = ResumeExtractor(model_name=MODEL[1], test=1)

    try:
        path = "./data/Economy/SALES/10724818.pdf"
        
        data = extractor.extract(path)
        
        print("\n✅ Extraction Complete:")
        print(json.dumps(data, indent=2))
        
    except Exception as e:
        print(f"❌ Error: {e}")