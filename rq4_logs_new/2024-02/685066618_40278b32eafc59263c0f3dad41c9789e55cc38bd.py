import streamlit as st
import fitz
import re
import io
from summary_functions import  getSummary
import os
from fpdf import FPDF
import base64
import re

def chunk_text(text, max_chunk_size=1024, overlap_size=50):
    chunks = []

    # Split the text into sentences
    sentences = re.split(r'(?<!\w\.\w.)(?<![A-Z][a-z]\.)(?<=\.|\?)\s', text)

    current_chunk = ""
    current_chunk_size = 0

    for sentence in sentences:
        sentence_size = len(sentence.split())
        
        # Check if adding the current sentence to the chunk exceeds the max_chunk_size
        if current_chunk_size + sentence_size <= max_chunk_size:
            current_chunk += sentence + " "
            current_chunk_size += sentence_size
        else:
            # Add the current chunk to the list
            chunks.append(current_chunk.strip())
            
            # Reset the current chunk
            current_chunk = sentence + " "
            current_chunk_size = sentence_size

    # Add the last chunk to the list
    chunks.append(current_chunk.strip())

    # Create overlapping chunks
    overlapping_chunks = []
    for i in range(len(chunks) - 1):
        overlapping_chunks.append(' '.join(chunks[i:i+2]))

    return overlapping_chunks

def clean_and_summarize(pdf_path):
    if pdf_path is None:
        st.error("Please upload a valid PDF file.")
        return ""

    try:
        # st.write("Opening PDF file...")
        pdf_document = fitz.open(stream=io.BytesIO(pdf_path.read()), filetype="pdf")
    except Exception as e:
        st.error(f"Error opening PDF file: {e}")
        return ""

    cleaned_text = ""
    
    # Iterate through each page of the PDF
    for page_number in range(pdf_document.page_count):
        # Extract text from the page
        page = pdf_document[page_number]
        page_text = page.get_text()

        # Remove URLs
        page_text = re.sub(r'http\S+', '', page_text, flags=re.MULTILINE)
        # Remove email addresses
        page_text = re.sub(r'\S+@\S+', '', page_text)
        # Remove other special characters or patterns as needed
        page_text = re.sub(r'\[.*?\]', '', page_text)  # Remove text within square brackets, common in references

        # Append cleaned text to the result
        cleaned_text += page_text

    # Close the PDF document
    pdf_document.close()

    # Divide the text into overlapping chunks
    text_chunks = chunk_text(cleaned_text)


    # Summarize each chunk and concatenate the summaries
    concatenated_summaries = ""
    for chunk in text_chunks:
        summary = getSummary(chunk)

        concatenated_summaries += summary + " "
        

    return concatenated_summaries

def app():
    st.title("SimplifyMe - A Content Summarizer")

    # Sidebar options
    st.sidebar.image('logo1.png')
    selected_option = st.sidebar.radio("Select Option", ["Text", "Research Paper", "Books"])
    if selected_option == "Text":
        st.header("Summarize Text")
        summary_input = st.text_area("Enter your text for summarization")
        max_length = st.slider("Select Maximum Summary Length", min_value=30, max_value=500, value=150)
        min_length = st.slider("Select Minimum Summary Length", min_value=10, max_value=200, value=50)

        if st.button("Generate Summary"):
            if summary_input:
                # Summarize the user input with user-defined max_length and min_length
                summary = getSummary(summary_input)
                st.success("Summary:\n" + summary)

    elif selected_option == "Research Paper":
        st.header("Summarize Research Paper")
        uploaded_file = st.file_uploader("Upload a Research Paper (PDF)", type=["pdf"])

        if uploaded_file:
            st.success("File uploaded successfully")
            
            # Clean and summarize the PDF
            if st.button("Generate Summary"):
                with st.spinner("Processing..."):
                    summaries = clean_and_summarize(uploaded_file)
                    st.subheader("Summary:")
                    st.success(f"{summaries}")
                    export_as_pdf = st.button("Generate PDF")
                    if export_as_pdf:

                        pdf = FPDF()
                        pdf.add_page()
                        pdf.set_draw_color(0, 0, 0)
                        pdf.set_line_width(1)

                        # add a border to the entire page
                        pdf.rect(5.0, 5.0, 200.0, 287.0, 'D')
        
                        # Set font for title
                        pdf.set_font('Times', 'B', 24)
                        pdf.cell(200, 20, 'SimplifyMe: A Content Summarizer', 0, 1, 'C')
                        
                        pdf.set_font("Arial", size=12)
                        pdf.cell(200, 10, txt="Summary:", ln=True, align="C")

                        # Split summaries into lines if it's too long
                        summary_lines = summaries.split("\n")
                        for line in summary_lines:
                            pdf.multi_cell(0, 10, line)

                        # Save the PDF content as binary data
                        pdf_output = pdf.output(dest="S").encode("latin-1")
                        
                        # Generate download link
                        b64 = base64.b64encode(pdf_output)
                        href = f'<a href="data:application/octet-stream;base64,{b64.decode()}" download="summary.pdf">Download PDF</a>'
                        st.markdown(href, unsafe_allow_html=True)

    elif selected_option == "Books":
        st.write("Coming Soon!....")

# Run the app
if __name__ == '__main__':
    app()