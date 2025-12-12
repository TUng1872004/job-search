import os
import shutil
import uuid
from fastapi import FastAPI, File, UploadFile, HTTPException, Form
from extract import ResumeExtractor

# Initialize FastAPI
app = FastAPI(title="Resume Extraction API")

# Configuration
UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

@app.post("/extract-resume")
async def extract_resume(
    file: UploadFile = File(...), 
):
    """
    Uploads a PDF resume, saves it, and extracts structured data.
    """
    model: str = Form("gemini")  
    if not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Only PDF files are allowed.")


    file_id = str(uuid.uuid4())
    safe_filename = f"{file_id}_{file.filename}"
    file_path = os.path.join(UPLOAD_DIR, safe_filename)

    try:
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)


        extractor = ResumeExtractor(model_name=model, test=0)

        extracted_data = extractor.extract(file_path)

        return {
            "status": "success",
            "file_id": file_id,
            "data": extracted_data
        }

    except Exception as e:

        raise HTTPException(status_code=500, detail=f"Extraction failed: {str(e)}")

    finally:
        file.file.close()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=6700)