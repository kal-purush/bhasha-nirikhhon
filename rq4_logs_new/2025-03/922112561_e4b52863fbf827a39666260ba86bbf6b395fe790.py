import os
from langchain_community.document_loaders import AzureAIDocumentIntelligenceLoader
from dotenv import load_dotenv
from PIL import Image

load_dotenv()

PATH = os.path.dirname(os.path.abspath(__file__))

# Azure AI Document Intelligence
AZURE_KEY = os.getenv("AZURE_KEY")
AZURE_ENDPOINT = os.getenv("AZURE_ENDPOINT")


class Image2txt:
    def __init__(self, file_path: str):
        """
        param file: 이미지 file path
        """
        self.file_path = file_path
        self.jd_text = ""

    def load_jd(self) -> str:
        """
        AzureAIDocumentIntelligenceLoader를 사용하여 JD에서 텍스트를 추출합니다.
        """
        # 이미지 확장자가 webp인 경우 jpg로 변환
        if self.file_path.endswith(".webp"):
            img = Image.open(self.file_path)
            img.convert("RGB").save("temp.jpg", "jpeg")
            file_path = f"{PATH}/temp.jpg"
            img = Image.open(file_path)

        jd_loader = AzureAIDocumentIntelligenceLoader(
            api_endpoint=AZURE_ENDPOINT,
            api_key=AZURE_KEY,
            file_path=file_path,
            api_model="prebuilt-layout",
        )
        jd_docs = jd_loader.load()

        # temp.jpg 파일 삭제
        file_path = f"{PATH}/temp.jpg"
        if os.path.exists(file_path):
            os.remove(file_path)

        for doc in jd_docs:
            jd = doc.metadata["content"]

        jd = f"<직무소개>\n{jd}</직무소개>"
        self.jd_text = jd
        return self.jd_text


if __name__ == "__main__":
    img2txt = Image2txt("(프계)_마케팅_솔루션_구축_(Google_Analytic4).webp")
    jd = img2txt.load_jd()
    print(jd)