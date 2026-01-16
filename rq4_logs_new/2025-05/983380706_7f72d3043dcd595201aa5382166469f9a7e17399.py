#import nest_asyncio
from fastapi import FastAPI, File, UploadFile, Form
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from insightface.app import FaceAnalysis
from numpy.linalg import norm
import numpy as np
from PIL import Image
from io import BytesIO
import torch
import uvicorn
import threading
import pickle
import os
import asyncio
from typing import List
from concurrent.futures import ThreadPoolExecutor
import psutil
import base64
from fastapi import HTTPException
import cv2

# Apply nest_asyncio to allow multiple event loops in Colab
#nest_asyncio.apply()

# Kill any existing processes
def kill_existing_processes():
    for proc in psutil.process_iter():
        try:
            if "uvicorn" in proc.name().lower():
                proc.kill()
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass

    print("Cleaned previous uvicorn processes")

def is_blurry(img, threshold=80.0):
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    lap_var = cv2.Laplacian(gray, cv2.CV_64F).var()
    return lap_var < threshold

def get_crop(image, bbox, margin=10):
    x1, y1, x2, y2 = [int(v) for v in bbox]
    h, w = image.shape[:2]

    # Add margin and ensure bounds stay within the image
    x1 = max(0, x1 - margin)
    y1 = max(0, y1 - margin)
    x2 = min(w, x2 + margin)
    y2 = min(h, y2 + margin)

    return image[y1:y2, x1:x2]

#Thread-safe embeddings store
embedding_lock = threading.Lock()
user_embeddings = {}

path = './'
EMBEDDINGS_PATH = path+"user_embeddings.pkl"

#Load from disk if exists
if os.path.exists(EMBEDDINGS_PATH):
    with open(EMBEDDINGS_PATH, "rb") as f:
        user_embeddings = pickle.load(f)

#Setup FastAPI
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

#Setup InsightFace
ctx_id = 0 if torch.cuda.is_available() else -1
face_app = FaceAnalysis(name='buffalo_l')
face_app.prepare(ctx_id=ctx_id, det_size=(640, 640))
print("Using device:", ctx_id)
# Get the ONNX backend providers used by the detection model
providers = face_app.models['detection'].session.get_providers()
print("Detection model providers:", providers)
providers = face_app.models['recognition'].session.get_providers()
print("Recognition model providers:", providers)


COSINE_THRESHOLD = 0.55
executor = ThreadPoolExecutor(max_workers=10)

def save_embeddings():
    with open(EMBEDDINGS_PATH, "wb") as f:
        pickle.dump(user_embeddings, f)

def process_frame(user_id, content):
    image = Image.open(BytesIO(content)).convert("RGB")
    img = np.array(image)
    faces = face_app.get(img)
    print(torch.cuda.memory_allocated() / 1024**2, "MB")
    results = []

    if not faces:
        return {"results": [], "image_base64": None, "cropped_faces":{}}

    # Convert original image into base64 early
    buffered = BytesIO()
    image.save(buffered, format="JPEG")
    img_base64 = base64.b64encode(buffered.getvalue()).decode("utf-8")

    send_img = False

    with embedding_lock:
        # Ensure user data exists
        if user_id not in user_embeddings:
            user_embeddings[user_id] = {
                "embeddings": np.empty((0, 512), dtype=np.float32),
                "labels": [],
                "unknown_idx": 1
            }
        user_data = user_embeddings[user_id]  # Reference, safe inside lock
        cropped_faces = {}
        for face in faces:
            x1, y1, x2, y2 = [int(i) for i in face.bbox]
            embedding = face.embedding
            embedding = embedding / norm(embedding)

            max_sim = 0
            identity = ""

            if user_data["embeddings"].shape[0] == 0:
                identity = "Unknown"
            else:
                embeddings_tensor = torch.tensor(user_data["embeddings"], dtype=torch.float32).cuda()
                embedding_tensor = torch.tensor(embedding, dtype=torch.float32).cuda()
                sims = torch.nn.functional.cosine_similarity(embeddings_tensor, embedding_tensor.unsqueeze(0)).cpu().numpy()

                max_sim = np.max(sims)
                idx = np.argmax(sims)
                identity = "Unknown" if max_sim < COSINE_THRESHOLD else user_data["labels"][idx]

            if identity == "Unknown":
                send_img = True
                identity = f"Unknown {user_data['unknown_idx']}"
                user_data["unknown_idx"] += 1
                user_data["embeddings"] = np.vstack([user_data["embeddings"], embedding])
                user_data["labels"].append(identity)

            results.append({
                "name": identity,
                "bbox": [x1, y1, x2, y2],
                "distance": float(max_sim)
            })
            if identity.startswith("Unknown"):
                send_img = True
                # crop the face
                face_img = img[y1:y2, x1:x2]
                pil_crop = Image.fromarray(face_img)
                buffer = BytesIO()
                pil_crop.save(buffer, format="JPEG")
                base64_crop = base64.b64encode(buffer.getvalue()).decode("utf-8")
                cropped_faces[identity] = base64_crop

        # Save only after modifying shared state
        save_embeddings()

    if not send_img:
        img_base64 = None

    return {
    "results": results,
    "image_base64": img_base64,  # Optional
    "cropped_faces": cropped_faces  # Optional
}


# Prediction route
@app.post("/test-frame")
async def predict(user_id: str = Form(...), file: UploadFile = File(...)):
    content = await file.read()

    result = await asyncio.get_event_loop().run_in_executor(
        executor, process_frame, user_id, content
    )
    return JSONResponse(content = result)

@app.post("/update")
async def change_label(user_id: str = Form(...),oldLabel: str = Form(...),newLabel: str = Form(...)):
    with embedding_lock:
        labels = user_embeddings[user_id]["labels"]
        # Replace oldLabel with newLabel
        updated_labels = [newLabel if label == oldLabel else label for label in labels]
        user_embeddings[user_id]["labels"] = updated_labels
    save_embeddings()
    return JSONResponse(
        content={
        "success": True,
        "message": f"Replaced all '{oldLabel}' with '{newLabel}'",
        "labels": updated_labels
        }
    )

#Add new face (for registration)
@app.post("/add-face")
async def add_face(
    user_id: str = Form(...),
    name: str = Form(...),
    files: List[UploadFile] = File(...)
):
    print("add face started")

    embeddings = []

    for file in files:
        try:
            content = await file.read()
            image = Image.open(BytesIO(content)).convert("RGB")
            img = np.array(image)

            faces = face_app.get(img)
            print(torch.cuda.memory_allocated() / 1024**2, "MB")
            if not faces:
                print(f"No face detected in file: {file.filename}")
                return JSONResponse(status_code=400, content={"message": f"No face detected in file: {file.filename}"})

            embedding = faces[0].embedding
            embedding = embedding / np.linalg.norm(embedding)
            embeddings.append(embedding)
            image.close()
            del img

        except Exception as e:
            print(f"Error processing file {file.filename}: {e}")
            return JSONResponse(status_code=500, content={"message": f"Error processing image: {file.filename}"})

    with embedding_lock:
        for embedding in embeddings:
            if user_id not in user_embeddings:
                user_embeddings[user_id] = {
                    "embeddings": np.array([embedding]),
                    "labels": [name],
                    "unknown_idx":1
                }
            else:
                user_embeddings[user_id]["embeddings"] = np.vstack([
                    user_embeddings[user_id]["embeddings"], embedding
                ])
                user_embeddings[user_id]["labels"].append(name)

        save_embeddings()

    print(f"Added {len(embeddings)} embeddings for '{name}' under user '{user_id}'")
    return JSONResponse(content={"message": f"Added {len(embeddings)} embeddings for '{name}' under user '{user_id}'"})

#delete faces of name in user_id
@app.post("/delete-face")
async def delete_face(user_id: str = Form(...), name: str = Form(...)):
    with embedding_lock:
        if user_id not in user_embeddings:
            return JSONResponse(status_code=404, content={"message": f"User '{user_id}' not found"})
        embeddings = user_embeddings[user_id]["embeddings"]
        labels = user_embeddings[user_id]["labels"]
        # Find all indices where the label matches the name
        indices_to_keep = [i for i, label in enumerate(labels) if label != name]
        if len(indices_to_keep) == len(labels):
            return JSONResponse(status_code=404, content={"message": f"No embeddings found for name '{name}' under user '{user_id}'"})
        # Filter embeddings and labels
        user_embeddings[user_id]["embeddings"] = embeddings[indices_to_keep]
        user_embeddings[user_id]["labels"] = [labels[i] for i in indices_to_keep]
        # If all embeddings deleted, remove the user completely (optional)
        if not user_embeddings[user_id]["labels"]:
            del user_embeddings[user_id]
        save_embeddings()

    print(f"Deleted all embeddings for '{name}' under user '{user_id}'")
    return JSONResponse(content={"message": f"Deleted all embeddings for '{name}' under user '{user_id}'"})



# ✅ Run ngrok and FastAPI server
def run_server():
    kill_existing_processes()  # Ensure no previous FastAPI servers are running
    print("Server running on http://127.0.0.1:8000")  # Localhost URL
    print(ctx_id)
    uvicorn.run(app, host="0.0.0.0", port=8000)  # Run the FastAPI server directly without ngrok

if __name__ == "__main__":
    run_server()