dataset = "https://www.kaggle.com/datasets/snehaanbhawal/resume-dataset"

from docling.document_converter import DocumentConverter

source = "./data/CS/"  # document per local path or URL
file = "cohota_bao.pdf"
converter = DocumentConverter()
result = converter.convert(source+file)

with open(f"test/{file.split(".")[0]}.md", "w", encoding="utf-8") as f:
    f.write(result.document.export_to_markdown())