import os

from langchain_openai import ChatOpenAI

openai_api_key = os.getenv("OPENAI_API_KEY")
LLM = ChatOpenAI(model="gpt-4o" ,api_key=openai_api_key)
# LLM = ChatOpenAI(model="gemma:2b")

question = input("Enter the question")
response = LLM.invoke(question)
print(response.content)