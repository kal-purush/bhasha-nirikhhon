import streamlit as st
import os
from PyPDF2 import PdfFileReader
from pdf2image import convert_from_path

def main():
    st.title("PDF Viewer with Streamlit")

    # PDF 파일 경로
    pdf_path = "data/project2_포폴.pdf"

    # PDF 파일이 존재하는지 확인
    if not os.path.exists(pdf_path):
        st.error("PDF file not found.")
        return

    # PDF 파일 정보 가져오기
    with open(pdf_path, "rb") as f:
        pdf_reader = PdfFileReader(f)
        num_pages = pdf_reader.numPages

    st.write(f"Number of Pages: {num_pages}")

    # PDF 파일을 이미지로 변환하여 Streamlit에 표시
    st.write("PDF Pages:")
    for i in range(num_pages):
        page_image = convert_from_path(pdf_path, first_page=i+1, last_page=i+1)[0]
        st.image(page_image, caption=f"Page {i+1}")

if __name__ == "__main__":
    main()