import streamlit as st
import pandas as pd
from typing import List, Sequence
from google.cloud import documentai
import os
from os.path import splitext

KEY_PATH = "C:\\Users\\HP\\Downloads\\service_account_key_for_project.json"
# Set the environment variable (optional, can be done before running the script)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = KEY_PATH


def online_process(
    project_id: str,
    location: str,
    processor_id: str,
    file_content: bytes,
    mime_type: str,
) -> documentai.Document:
    opts = {"api_endpoint": f"{location}-documentai.googleapis.com"}
    # Instantiates a client
    documentai_client = documentai.DocumentProcessorServiceClient(client_options=opts)

    # The full resource name of the processor, e.g.:
    # projects/project-id/locations/location/processor/processor-id
    # You must create new processors in the Cloud Console first
    resource_name = documentai_client.processor_path(project_id, location, processor_id)

    # Load Binary Data into Document AI RawDocument Object
    raw_document = documentai.RawDocument(
        content=file_content, mime_type=mime_type
    )

    # Configure the process request
    request = documentai.ProcessRequest(
        name=resource_name, raw_document=raw_document
    )

    # Use the Document AI client to process the sample form
    result = documentai_client.process_document(request=request)

    return result.document


def get_table_data(
    rows: Sequence[documentai.Document.Page.Table.TableRow], text: str
) -> List[List[str]]:
    all_values: List[List[str]] = []
    for row in rows:
        current_row_values: List[str] = []
        for cell in row.cells:
            current_row_values.append(
                text_anchor_to_text(cell.layout.text_anchor, text)
            )
        all_values.append(current_row_values)
    return all_values


def text_anchor_to_text(text_anchor: documentai.Document.TextAnchor, text: str) -> str:
    response = ""
    # If a text segment spans several lines, it will
    # be stored in different text segments.
    for segment in text_anchor.text_segments:
        start_index = int(segment.start_index)
        end_index = int(segment.end_index)
        response += text[start_index:end_index]
    return response.strip().replace("\n", " ")


def convert_pdf_to_dataframe(file_content: bytes, project_id: str, location: str, processor_id: str, selected_headers: List[str]) -> pd.DataFrame:
    document = online_process(
        project_id=project_id,
        location=location,
        processor_id=processor_id,
        file_content=file_content,
        mime_type="application/pdf",
    )

    all_body_row_values = []

    for page in document.pages:
        for table in page.tables:
            header_values = get_table_data(table.header_rows, document.text)[0]
            if all(header in header_values for header in selected_headers):
                body_row_values = get_table_data(table.body_rows, document.text)
                all_body_row_values.extend(body_row_values)

    df = pd.DataFrame(
        data=all_body_row_values,
        columns=selected_headers,
    )

    return df


st.title("PDF to DataFrame Converter")

# File Uploader
uploaded_file = st.file_uploader("Upload a PDF file", type=["pdf"])

if uploaded_file is not None:
    # Ask for user input - Dropdowns
    st.write("Select the header row values for the table you want to extract:")
    selected_headers = []
    for i in range(5):
        selected_headers.append(st.selectbox(f"Select header {i+1}", options=["Transaction Date", "Description", "Debit", "Credit", "Balance"]))

    if st.button("Convert"):
        # Convert and display the DataFrame
        result_df = convert_pdf_to_dataframe(uploaded_file.read(), "437222221292", "us", "372c4ab238787a42", selected_headers)
        st.dataframe(result_df)