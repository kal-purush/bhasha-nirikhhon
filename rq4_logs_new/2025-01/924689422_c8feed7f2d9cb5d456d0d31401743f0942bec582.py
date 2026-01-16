from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from langchain.document_loaders import TextLoader
from langchain.indexes import VectorstoreIndexCreator
from langchain.text_splitter import CharacterTextSplitter
from langchain_google_genai import GoogleGenerativeAI, GoogleGenerativeAIEmbeddings
from pydantic import BaseModel
import os
from dotenv import load_dotenv
import shutil
import tempfile
import pandas as pd
import logging

# ---------------------------
# 1. Configuration and Setup
# ---------------------------

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()

# Initialize FastAPI app
app = FastAPI()

# Configure CORS to allow requests from the frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  # Update with your frontend URL if different
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------
# 2. Initialize LLM and Embeddings
# ---------------------------

# Initialize LLM with Google Generative AI
google_api_key = os.getenv("GOOGLE_API_KEY")
if not google_api_key:
    logger.error("GOOGLE_API_KEY not found in environment variables.")
    raise ValueError("GOOGLE_API_KEY not found in environment variables.")

try:
    llm = GoogleGenerativeAI(model="gemini-2.0-flash-exp", google_api_key=google_api_key)
    logger.info("Google Generative AI LLM initialized successfully.")
except Exception as e:
    logger.error(f"Failed to initialize Google Generative AI LLM: {str(e)}")
    raise

# Global variable to store the index
index = None

# ---------------------------
# 3. Define Pydantic Models
# ---------------------------

class QueryRequest(BaseModel):
    prompt: str

# ---------------------------
# 4. Utility Functions
# ---------------------------

ALLOWED_EXTENSIONS = {'txt', 'xlsx', 'xls'}

def allowed_file(filename: str) -> bool:
    """Check if the uploaded file has an allowed extension."""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def load_excel_as_text(file_path: str, extension: str) -> str:
    """
    Reads an Excel file and converts its content to a single string.
    Includes all sheets with their respective names.
    """
    try:
        if extension.lower() == '.xlsx':
            engine = 'openpyxl'
        elif extension.lower() == '.xls':
            engine = 'xlrd'
        else:
            raise ValueError("Unsupported file extension for Excel file.")
        
        excel_data = pd.read_excel(file_path, sheet_name=None, engine=engine)  # Specify engine explicitly
        text = ""
        for sheet_name, df in excel_data.items():
            text += f"Sheet: {sheet_name}\n"
            text += df.to_string(index=False) + "\n\n"
        logger.info(f"Excel file '{file_path}' converted to text successfully.")
        return text
    except Exception as e:
        logger.error(f"Error reading Excel file '{file_path}': {str(e)}")
        raise ValueError(f"Error reading Excel file: {str(e)}")

# ---------------------------
# 5. API Endpoints
# ---------------------------

@app.post("/upload/")
async def upload_file(file: UploadFile = File(...)):
    """
    Endpoint to upload a file (.txt, .xlsx, .xls).
    Processes the file, creates embeddings, and builds the index.
    """
    global index
    tmp_path = None
    text_tmp_path = None
    try:
        # Validate file extension
        if not allowed_file(file.filename):
            error_msg = f"Unsupported file type: {file.filename.split('.')[-1]}. Supported types are .txt, .xlsx, .xls"
            logger.error(error_msg)
            raise HTTPException(status_code=400, detail=error_msg)
        
        # Save the uploaded file to a temporary location
        with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(file.filename)[1]) as tmp:
            shutil.copyfileobj(file.file, tmp)
            tmp_path = tmp.name
        logger.info(f"Uploaded file saved to temporary path: {tmp_path}")

        # Determine the loader based on file type
        extension = os.path.splitext(file.filename)[1]
        if extension.lower() == '.txt':
            loader = TextLoader(tmp_path)
            logger.info("Initialized TextLoader for .txt file.")
        elif extension.lower() in ('.xlsx', '.xls'):
            # Read Excel file and create a temporary text file
            excel_text = load_excel_as_text(tmp_path, extension)
            with tempfile.NamedTemporaryFile(delete=False, suffix='.txt') as text_tmp:
                text_tmp.write(excel_text.encode('utf-8'))
                text_tmp_path = text_tmp.name
            loader = TextLoader(text_tmp_path)
            logger.info("Initialized TextLoader for Excel file.")
        else:
            # This block is redundant due to earlier validation but kept for safety
            error_msg = f"Unsupported file type: {extension}. Supported types are .txt, .xlsx, .xls"
            logger.error(error_msg)
            raise HTTPException(status_code=400, detail=error_msg)

        # Create embeddings using Google Generative AI Embeddings
        try:
            embedding = GoogleGenerativeAIEmbeddings(model="models/embedding-001")
            logger.info("Google Generative AI Embeddings initialized successfully.")
        except Exception as e:
            logger.error(f"Failed to initialize Google Generative AI Embeddings: {str(e)}")
            raise HTTPException(status_code=500, detail="Failed to initialize embeddings.")

        # Split text into smaller chunks
        try:
            text_splitter = CharacterTextSplitter(chunk_size=500, chunk_overlap=100)
            logger.info("CharacterTextSplitter initialized successfully.")
        except Exception as e:
            logger.error(f"Failed to initialize text splitter: {str(e)}")
            raise HTTPException(status_code=500, detail="Failed to initialize text splitter.")

        # Create the index with the specified embedding model and text splitter
        try:
            index_creator = VectorstoreIndexCreator(
                embedding=embedding,
                text_splitter=text_splitter
            )
            index = index_creator.from_loaders([loader])
            logger.info("Vector store index created successfully.")
        except Exception as e:
            logger.error(f"Failed to create index: {str(e)}")
            raise HTTPException(status_code=500, detail="Failed to create index.")

        return {"message": "File uploaded and index created successfully"}

    except HTTPException as he:
        # Re-raise HTTP exceptions with logging
        logger.error(f"HTTPException: {he.detail}")
        raise he
    except ValueError as ve:
        # Handle value errors with logging
        logger.error(f"ValueError: {str(ve)}")
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        # Handle all other exceptions with logging
        logger.error(f"Exception during file upload: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        # Cleanup temporary files
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
                logger.info(f"Deleted temporary file: {tmp_path}")
            except Exception as e:
                logger.warning(f"Failed to delete temporary file '{tmp_path}': {str(e)}")
        if text_tmp_path and os.path.exists(text_tmp_path):
            try:
                os.remove(text_tmp_path)
                logger.info(f"Deleted temporary text file: {text_tmp_path}")
            except Exception as e:
                logger.warning(f"Failed to delete temporary text file '{text_tmp_path}': {str(e)}")

@app.post("/query/")
async def query_index(request: QueryRequest):
    """
    Endpoint to query the index with a prompt.
    Returns the generated response from the LLM.
    """
    global index
    prompt = request.prompt.strip()
    if not prompt:
        logger.error("Empty prompt received.")
        raise HTTPException(status_code=400, detail="Prompt cannot be empty.")
    
    if index is None:
        error_msg = "Index not initialized. Please upload a file first."
        logger.error(error_msg)
        raise HTTPException(status_code=400, detail=error_msg)
    
    try:
        logger.info(f"Received query: {prompt}")
        response = index.query(prompt, llm=llm)
        logger.info("Query processed successfully.")
        return {"response": response}
    except Exception as e:
        logger.error(f"Exception during query processing: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))