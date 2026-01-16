# Remove this if needed 
# import chroma_patch

from langchain_chroma import Chroma
from retriever import Retriever
from langchain_ollama import OllamaEmbeddings
import re
import ollama
from langchain.memory import ConversationBufferMemory



class MDUBot:
    # Change model_name, embed_model_name or persist_path if needed
    # model_name = "gemma3:4b" or :1b
    def __init__(self, model_name="gemma3:4b", embed_model_name="mxbai-embed-large", persist_path="./chroma"): 
        self.model = model_name
        self.embed_model = OllamaEmbeddings(model=embed_model_name)
        self.db = Chroma(embedding_function=self.embed_model, persist_directory=persist_path)
        self.retriver = Retriever(self.db, self.embed_model)
        self.memory = ConversationBufferMemory(memory_key="chat_history", return_messages=True)
        self.chat_history = []

    def run(self):
        
        test_prompts = [
    "Can you tell me about the CDT406 course?",
    "What are the prerequisites for that?",
    "Which program includes it?",
    "Is it active this year?",
    "exit"
    ]

        
        while True:
            prompt = test_prompts.pop(0) 
            print(f"Test prompt: {prompt}")
            # prompt = input("You: ")
            if prompt == "exit":
                break

            prompt = prompt.lower().strip()
            course_code = re.findall(r'\b[a-z]{2,3}\d{3}\b', prompt)
            program_code = re.findall(r'\b[a-z]{2,3}\d{2}\b', prompt)
            num_codes = len(course_code) or len(program_code)

            context = self.retriver.query(prompt, course_code, program_code, num_codes)
            context_text = "\n".join([doc.page_content for doc in context])

            system_prompt = f"""You are an assistant helping answer questions about university courses and programs at Mälardalens universitet (MDU).
                        Here is the context about the course or program:\n{context_text}\n
                        This is the question: {prompt}\n
                        Answer the question by:
                        Providing relevant information from the context.
                        Using your knowledge to generate a response.
                        Ensuring the response is accurate and helpful.
                        Using the correct course or program codes when referring to specific courses or programs.
                        Referring to the university as Mälardalens universitet or MDU. Do not use MDH or Mälardalens Högskola, as these are old abbreviations.
                        Answer in the same language as the question provided.
                        """

            # response = ollama.generate(model=self.model, prompt=prompt)
            # print(f"\nMDUBot: {response["response"]}")
            self.chat_history.append({"role": "user", "content": system_prompt})
            response = ollama.chat(
                model=self.model,
                messages=self.chat_history
            )
            bot_reply = response['message']['content']
            self.chat_history.append({"role": "assistant", "content": bot_reply})

            print(f"\nMDUBot: {bot_reply}")

bot = MDUBot()
bot.run()