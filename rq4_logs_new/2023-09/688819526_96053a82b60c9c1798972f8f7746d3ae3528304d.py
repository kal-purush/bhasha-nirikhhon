from pathlib import Path
import json

from langchain.chat_models import ChatOpenAI
from langchain.prompts.prompt import PromptTemplate
from langchain.chains import ConversationChain, LLMChain
from langchain.memory import ConversationBufferWindowMemory

from utils.search_google import search

class ClassificationLLM():
    def __init__(self) -> None:
        self.prompt = self.get_prompt()
        self.model = self.get_model()
    
    def get_model(self) -> ChatOpenAI:
        secret_path = Path("resource").joinpath("secrets.json")
        secrets = json.loads(open(secret_path).read())
        openai_api_key = secrets["OPENAI_API_KEY"]
        chat_model = ChatOpenAI(model="gpt-4", openai_api_key=openai_api_key)
        conversation = LLMChain(
            prompt=self.prompt,
            llm=chat_model,
        )
        return conversation
    
    def get_prompt(self) -> PromptTemplate:
        template = """
당신은 입력을 적절한 작업에 배치하는 역할입니다
주어진 작업의 이름과 세부내용은 다음과 같습니다
1. free_chat: 다른 작업에 속하지 않는 일반적인 대화
2. provide_link: 입력에 대한 배경지식을 제공할 필요가 있는 경우"""

        prompt = PromptTemplate.from_template(template=template)
        return prompt
    
    def get_task(self, user_input:str) -> str:
        return self.model.predict(input=user_input)
        

class OpenAIFreeChat():
    def __init__(self) -> None:
        self.prompt = self.get_prompt()
        self.model = self.get_model()
    
    def get_model(self) -> ChatOpenAI:
        secret_path = Path("resource").joinpath("secrets.json")
        secrets = json.loads(open(secret_path).read())
        openai_api_key = secrets["OPENAI_API_KEY"]
        model = ChatOpenAI(model="gpt-4", openai_api_key=openai_api_key)
        return model
    
    def get_prompt(self) -> PromptTemplate:
        template = """


Current conversation:
{history}
Human: {input}
AI Assistant:"""

        prompt = PromptTemplate(input_variables=["history", "input"], template=template)
        return prompt
    
    def get_chain(self) -> ConversationChain:
        conversation = ConversationChain(
            prompt=self.prompt,
            llm=self.model,
            verbose=True,
            memory=ConversationBufferWindowMemory(ai_prefix="AI Assistant", k=10)
        )
        return conversation
    
    def get_answer(self, user_input:str, content:str) -> str:
        task = self.classifier_llm.get_task(user_input)
        chain = self.get_chain(task)
        answer = chain.predict(input=user_input)
        
        return answer


class OpenAILinkProvider():
    def __init__(self) -> None:
        self.prompt = self.get_prompt()
        self.model = self.get_model()
    
    def get_model(self) -> ChatOpenAI:
        secret_path = Path("resource").joinpath("secrets.json")
        secrets = json.loads(open(secret_path).read())
        openai_api_key = secrets["OPENAI_API_KEY"]
        chat_model = ChatOpenAI(model="gpt-4", openai_api_key=openai_api_key)
        conversation = LLMChain(
            prompt=self.prompt,
            llm=chat_model,
        )
        return conversation
    
    def get_prompt(self) -> PromptTemplate:
        template = """
입력에 대한 답을 구글에서 검색할 수 있도록 검색어를 추천합니다"""

        prompt = PromptTemplate.from_template(template=template)
        return prompt
    
    def get_answer(self, user_input:str) -> str:
        query = self.model.predict(input=user_input)
        return search(query)
    
    
class OpenAISubjectRecommander():
    def __init__(self) -> None:
        pass
    
    def get_model(self) -> ChatOpenAI:
        secret_path = Path("resource").joinpath("secrets.json")
        secrets = json.loads(open(secret_path).read())
        openai_api_key = secrets["OPENAI_API_KEY"]
        model = ChatOpenAI(model="gpt-4", openai_api_key=openai_api_key)
        return model
    
    def get_prompt(self, subject:str) -> PromptTemplate:
        template1 = """
{subject}에 연관된 주제를 제시합니다
논쟁적인 글쓰기를 위한 주제를 제시합니다
10대 학생을 대상으로 합니다
포괄적인 주제는 제외합니다
지나치게 논쟁적인 주제는 제외합니다
20자 미만으로 출력합니다
한 번에 5개씩 출력합니다
각 주제는 줄바꿈으로 구분합니다 """
        template2 = """
Current conversation:
{history}
Human: {input}
AI Assistant:"""

        prompt = PromptTemplate(input_variables=["history", "input"], template=template1.format(subject=subject)+template2)
        return prompt
    
    def get_chain(self, prompt, model) -> ConversationChain:
        conversation = ConversationChain(
            prompt=prompt,
            llm=model,
            verbose=True,
            memory=ConversationBufferWindowMemory(ai_prefix="AI Assistant", k=10)
        )
        return conversation
    
    def get_answer(self, category:str) -> str:
        chain = self.get_chain(prompt=self.get_prompt(category), model=self.get_model())
        answer = chain.predict(input=category)
        answer = answer.split("\n")
        return answer