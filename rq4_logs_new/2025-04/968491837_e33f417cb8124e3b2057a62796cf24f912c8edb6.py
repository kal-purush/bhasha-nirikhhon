import os
import json
import io
import logging
from typing import Optional
from pathlib import Path

from fastapi import FastAPI, UploadFile, File, HTTPException, status, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from googleapiclient.errors import HttpError
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Initialize FastAPI app
app = FastAPI(
    title="Google Drive File Uploader",
    description="API for securely uploading files to Google Drive",
    version="1.0.0",
)

#credintials




# Add CORS middleware - CRITICAL for frontend connections
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins for testing
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Response models
class UploadResponse(BaseModel):
    message: str
    file_id: str
    file_name: str
    file_size: int
    mime_type: str
    upload_time: float

class ErrorResponse(BaseModel):
    error: str
    details: Optional[str] = None

# Custom exception handler
@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": exc.detail},
    )

# Global service account config function
def get_drive_service():
    """Initializes and returns Google Drive service"""
    try:
        # First try to use a JSON file if it exists
        service_account_path = os.getenv("GOOGLE_SERVICE_ACCOUNT_PATH")
        if service_account_path and os.path.exists(service_account_path):
            credentials = service_account.Credentials.from_service_account_file(
                service_account_path, scopes=["https://www.googleapis.com/auth/drive"]
            )
            logger.info(f"Loaded credentials from file: {service_account_path}")
        else:
            # Try to use the environment variable
            service_account_info = os.getenv("GOOGLE_SERVICE_ACCOUNT_KEY")
            
            if not service_account_info:
                # For testing/development: create a temporary credentials file
                creds_from_env = os.getenv("GOOGLE_SERVICE_ACCOUNT_CONTENT")
                if creds_from_env:
                    temp_creds_path = Path("temp_credentials.json")
                    temp_creds_path.write_text(creds_from_env)
                    credentials = service_account.Credentials.from_service_account_file(
                        str(temp_creds_path), scopes=["https://www.googleapis.com/auth/drive"]
                    )
                    logger.info("Created and loaded temporary credentials file")
                else:
                    raise ValueError("No Google credentials found. Please set GOOGLE_SERVICE_ACCOUNT_PATH or GOOGLE_SERVICE_ACCOUNT_KEY")
            else:
                # Parse the JSON from environment variable
                try:
                    service_account_dict = json.loads(service_account_info)
                    credentials = service_account.Credentials.from_service_account_info(
                        service_account_dict, scopes=["https://www.googleapis.com/auth/drive"]
                    )
                    logger.info("Loaded credentials from environment variable")
                except json.JSONDecodeError:
                    raise ValueError("Invalid JSON in GOOGLE_SERVICE_ACCOUNT_KEY")
        
        # Build and return the service
        drive_service = build("drive", "v3", credentials=credentials)
        return drive_service
    
    except Exception as e:
        logger.error(f"Failed to initialize Google Drive: {str(e)}")
        raise

# Simple test endpoint to verify API is working
@app.get("/test")
async def test_endpoint():
    """Simple endpoint to test if the API is working"""
    return {"status": "ok", "message": "API is working"}

@app.post(
    "/upload",
    response_model=UploadResponse,
    responses={
        400: {"model": ErrorResponse},
        500: {"model": ErrorResponse},
    },
)
async def upload_file(file: UploadFile = File(...)):
    """Upload a file to Google Drive"""
    logger.info(f"Received upload request for file: {file.filename}")
    start_time = time.time()
    
    try:
        # Initialize Drive service
        drive_service = get_drive_service()
        folder_id = os.getenv("GOOGLE_DRIVE_FOLDER_ID")
        if not folder_id:
            raise ValueError("GOOGLE_DRIVE_FOLDER_ID environment variable is not set")
        
        # Read file content
        file_content = await file.read()
        file_size = len(file_content)
        logger.info(f"File size: {file_size} bytes")
        
        # For security, generate a clean filename
        original_name = os.path.basename(file.filename)
        safe_filename = f"{int(time.time())}_{original_name}"
        
        # Prepare file metadata
        file_metadata = {
            "name": safe_filename,
            "parents": [folder_id]
        }
        
        # Upload to Google Drive
        media = MediaIoBaseUpload(
            io.BytesIO(file_content), 
            mimetype=file.content_type,
            resumable=True
        )
        
        logger.info(f"Uploading file to Google Drive folder: {folder_id}")
        uploaded_file = drive_service.files().create(
            body=file_metadata,
            media_body=media,
            fields="id, name, size, mimeType, webViewLink"
        ).execute()
        
        # Calculate upload time
        upload_time = time.time() - start_time
        
        logger.info(f"File uploaded successfully: {uploaded_file.get('name')} (ID: {uploaded_file.get('id')})")
        
        return {
            "message": "File uploaded successfully!",
            "file_id": uploaded_file.get("id"),
            "file_name": uploaded_file.get("name"),
            "file_size": int(uploaded_file.get("size", file_size)),
            "mime_type": uploaded_file.get("mimeType", file.content_type),
            "upload_time": round(upload_time, 2)
        }
        
    except ValueError as e:
        logger.error(f"Configuration error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Server configuration error: {str(e)}",
        )
        
    except HttpError as e:
        logger.error(f"Google Drive API error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Google Drive API error: {str(e)}",
        )
        
    except Exception as e:
        logger.error(f"Unexpected error during file upload: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Upload failed: {str(e)}",
        )

@app.get("/health")
async def health_check():
    """Health check endpoint for monitoring"""
    return {"status": "healthy", "timestamp": time.time()}

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)