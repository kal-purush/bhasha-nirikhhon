import os
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List
import uvicorn

from langchain.embeddings import OpenAIEmbeddings
from langchain.vectorstores import Chroma
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain.llms import OpenAI
from langchain.chains import RetrievalQA

import pdfplumber
import docx

# --- Configuration ---
DATA_DIR = "./documents"  # Folder containing .pdf and .docx files
PERSIST_DIR = "./vector_store"  # Where Chroma will persist embeddings

# Initialize FastAPI
app = FastAPI(title="RAG MVP API")

# Initialize components
embedder = OpenAIEmbeddings()
text_splitter = RecursiveCharacterTextSplitter(chunk_size=800, chunk_overlap=100)

# Load or create vectorstore
vectordb = Chroma(
    collection_name="rag_docs",
    embedding_function=embedder,
    persist_directory=PERSIST_DIR
)

# Function to extract text

def extract_text_from_docx(path: str) -> str:
    doc = docx.Document(path)
    return "\n".join([p.text for p in doc.paragraphs])


def extract_text_from_pdf(path: str) -> str:
    full_text = []
    with pdfplumber.open(path) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if text:
                full_text.append(text)
    return "\n".join(full_text)

# Ingestion endpoint
@app.post("/ingest")
def ingest_documents():
    files = [f for f in os.listdir(DATA_DIR) if f.endswith(('.pdf', '.docx'))]
    if not files:
        raise HTTPException(status_code=404, detail="No documents found to ingest.")

    texts, metadatas = [], []
    for fname in files:
        path = os.path.join(DATA_DIR, fname)
        if fname.endswith('.pdf'):
            raw = extract_text_from_pdf(path)
        else:
            raw = extract_text_from_docx(path)
        # split into chunks
        chunks = text_splitter.split_text(raw)
        texts.extend(chunks)
        metadatas.extend([{"source": fname}] * len(chunks))

    # Upsert into vectorstore
    vectordb.add_texts(texts=texts, metadatas=metadatas)
    vectordb.persist()
    return {"ingested_files": len(files), "total_chunks": len(texts)}

# Query model
llm = OpenAI(model_name="gpt-4o-mini", temperature=0)
qa_chain = RetrievalQA.from_chain_type(
    llm=llm,
    chain_type="stuff",
    retriever=vectordb.as_retriever()
)

class QueryRequest(BaseModel):
    query: str
    top_k: int = 5

# Query endpoint
@app.post("/query")
def query_docs(request: QueryRequest):
    if not request.query:
        raise HTTPException(status_code=400, detail="Query text is required.")

    results = qa_chain.run(request.query)
    return {"answer": results}

# Run application
def main():
    uvicorn.run(app, host="0.0.0.0", port=8000)


if __name__ == "__main__":
    main()