import io
import streamlit as st
import cv2
import numpy as np
from PIL import Image
from azure.storage.blob import BlobServiceClient
from azure.cognitiveservices.vision.computervision import ComputerVisionClient
from azure.cognitiveservices.vision.computervision.models import OperationStatusCodes
from msrest.authentication import CognitiveServicesCredentials
import pyodbc

# Azure Blob Storage Config
AZURE_STORAGE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=numberplates;AccountKey=UVB9HnEAWKDlq5lbGafLsz183mne7Y216RHliq4LxEyCflFo3SADIvALVmI1HSS8miXH2frLqshr+AStUDZCjg==;EndpointSuffix=core.windows.net"
CONTAINER_NAME = "vehicle-plate-images"

# Azure OCR Config
COMPUTER_VISION_ENDPOINT = "https://numberplate-cv.cognitiveservices.azure.com/"
COMPUTER_VISION_SUBSCRIPTION_KEY = "71kxuEKo6qhfr08KBcyuXmCk7BzetT7HoJkyz1NGqUk3Hg2kImPtJQQJ99ALACGhslBXJ3w3AAAFACOG1HuX"

# Database Config
server = 'serverocr.database.windows.net'
database = 'License-Plate-DB'
username = 'ggrserver'
password = 'Sreenamahesh@me'

def create_connection():
    try:
        conn = pyodbc.connect(
            f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}"
        )
        return conn
    except Exception as e:
        st.error(f"Error connecting to the database: {e}")
        return None

def execute_query(query, params=None, fetch=False):
    conn = create_connection()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute(query, params) if params else cursor.execute(query)
            if fetch:
                return cursor.fetchall()
            else:
                conn.commit()
                return True
        except Exception as e:
            st.error(f"Error executing query: {e}")
            conn.rollback()  # Rollback on failure
        finally:
            conn.close()
    return None

# Initialize Clients
blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
computer_vision_client = ComputerVisionClient(COMPUTER_VISION_ENDPOINT, CognitiveServicesCredentials(COMPUTER_VISION_SUBSCRIPTION_KEY))

def crop_license_plate(image):
    try:
        gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
        blur = cv2.GaussianBlur(gray, (5, 5), 0)
        edges = cv2.Canny(blur, 50, 200)

        contours, _ = cv2.findContours(edges, cv2.RETR_TREE, cv2.CHAIN_APPROX_SIMPLE)
        contours = sorted(contours, key=cv2.contourArea, reverse=True)[:10]

        for contour in contours:
            approx = cv2.approxPolyDP(contour, 0.02 * cv2.arcLength(contour, True), True)
            if len(approx) == 4:  # Check for rectangle
                x, y, w, h = cv2.boundingRect(approx)
                cropped = image[y:y + h, x:x + w]

                # Check if the cropped image has valid dimensions
                if cropped.size > 0 and w > 50 and h > 15:
                    return cropped

        st.warning("No valid license plate detected.")
    except Exception as e:
        st.error(f"Error during cropping: {e}")
    return None

def perform_ocr_on_cropped(image):
    try:
        _, buffer = cv2.imencode('.jpg', image)
        result = computer_vision_client.read_in_stream(io.BytesIO(buffer.tobytes()), raw=True)
        operation_location = result.headers["Operation-Location"]
        operation_id = operation_location.split("/")[-1]

        while True:
            result = computer_vision_client.get_read_result(operation_id)
            if result.status not in [OperationStatusCodes.not_started, OperationStatusCodes.running]:
                break

        if result.status == OperationStatusCodes.succeeded:
            license_plate_text = []
            for read_result in result.analyze_result.read_results:
                for line in read_result.lines:
                    license_plate_text.append(line.text)
            return license_plate_text
        else:
            st.warning("OCR failed to recognize text.")
    except Exception as e:
        st.error(f"Failed to perform OCR on cropped image: {e}")
    return []

st.title("License Plate Recognition")

menu = ["Upload Image", "View Database"]
choice = st.sidebar.selectbox("Menu", menu)

if choice == "Upload Image":
    st.subheader("Upload an Image")
    uploaded_file = st.file_uploader("Choose an image file", type=['png', 'jpg', 'jpeg'])

    if uploaded_file is not None:
        image = Image.open(uploaded_file)
        image = np.array(image)
        st.image(image, caption="Uploaded Image", use_column_width=True)

        cropped_image = crop_license_plate(image)
        if cropped_image is not None:
            st.image(cropped_image, caption="Cropped License Plate", use_column_width=True)
            detected_text = perform_ocr_on_cropped(cropped_image)
            if detected_text:
                st.success("Detected License Plate Text:")
                st.write(detected_text)

                toll_booth_id = st.text_input("Enter Toll Booth ID")
                if toll_booth_id:
                    if st.button("Save to Database"):
                        for text in detected_text:
                            query = "INSERT INTO OCRDetails (TollBoothID, LicensePlate) VALUES (?, ?)"
                            params = (toll_booth_id, text)
                            success = execute_query(query, params)
                            if success:
                                st.success(f"License plate '{text}' saved successfully!")
                            else:
                                st.error(f"Failed to save license plate '{text}' to the database.")
                else:
                    st.error("Please enter Toll Booth ID.")
        else:
            st.warning("No license plate detected.")
elif choice == "View Database":
    st.subheader("Database Records")
    query = "SELECT * FROM OCRDetails"
    data = execute_query(query, fetch=True)

    if data:
        for row in data:
            st.write(row)
    else:
        st.warning("No data found in the database.")