from fastapi import APIRouter
from typing import Union
from pydantic import BaseModel
import json

import openai
import dotenv
import os
from resource.model.openai_llms import ClassificationLLM, OpenAIFreeChat, OpenAILinkProvider
from utils.make_chatbot import intention, provide_links, freechat

dotenv_file = dotenv.find_dotenv()
dotenv.load_dotenv(dotenv_file)

key = os.environ["OPENAI_API_KEY"]
openai.api_key = key

router = APIRouter(
    prefix="/chatbot",
)

############################################
# 1. 채팅 생성(의도 분류, 링크 생성, 자유 대화)
############################################
class Chat(BaseModel):
    content: Union[str, None] = None
    user_input: Union[str, None] = None

@router.post("/chat")
def chat(item: Chat):
    content = item.dict()['content']
    user_input = item.dict()['user_input']
    print(user_input)

    # 의도 분류
    intent = intention(user_input)
    # intent = ClassificationLLM().get_task(user_input) # 출력 이상있음
    print(intent)

    if "provide_link" in intent:
        # classifier = ClassificationLLM()        # 의도 분류 langchain 
        # free_chat = OpenAIFreeChat()            # 여기서 쓰일 게 아님.
        link_provider = OpenAILinkProvider()

        # task = classifier.get_task(user_input)
        # model = free_chat if task == "free_chat" else link_provider
        
        return link_provider.get_answer(user_input, content)
    
    elif "free_talk" in intent:
        answer = freechat(content, user_input)
        
        return {"answer":answer}

############################################
# 2. 질문 생성
############################################
class Question(BaseModel):
    content: Union[str, None] = None
    
@router.post("/make_question")
def read_item(item: Question):
    content = item.dict()['content']

    system_text='''
    1.사용자가 글을 쓸 때 도움이 되는 질문을 해줘야 돼
    2.입력은 <주제^서론^본론^결론> 순서로 나와
    3.출력으로는 간단한 질문을 3가지 정도를 말해줘
    4.출력형식은 Json형식으로 나타내줘
    '''

    pre_input1 = '''
    실시간 검색어 서비스 중단^실시간 검색어 서비스 중단은 좋은 방안이 아니였다.^실시간 검색어라는 것은 사람들이 무엇에 관심 있는지 알 수있는 지표기 때문이다.실시간 검색어를 통해 현재 가장 논란이 되고있는 뉴스나 사건들을 접하여 새로운 정보를 알수가있다.^실시간 검색어 서비스를 중단하지 않고 다른 방안을 생각하는 것이 더 나은 방안이라고 생각한다.
    '''

    pre_output1 = '''
    {
    "questions":["실시간 검색어가 사람들의 가장 큰 관심사를 대변한다고 생각하시나요?","실시간 검색어를 통해 새로운 정보를 얻었던 경험이 있나요?","실시간 검색어 서비스 대신 어떤 서비스를 생각하고 있나요?"]
    }
    '''

    pre_input2 ='''
    가장 여행가고싶은 나라^^^
    '''

    pre_output2 = '''
    {
    "questions": ["여행을 통해 얻고 싶은 경험이나 기대치가 있나요?","어떤 종류의 여행을 선호하나요?","먹는 것을 좋아하시나요?"]
    }
    '''

    messages=[
            {"role": "system", "content": system_text},
            {"role": "user", "content": pre_input1},
            {"role": "assistant", "content": pre_output1},
            {"role": "user", "content": pre_input2},
            {"role": "assistant", "content": pre_output2},
            {"role": "user", "content": content},
        ]

    chat_completion = openai.ChatCompletion.create( ## gpt 오브젝트 생성후 메세지 전달
        model="gpt-4", 
        messages=messages,
        temperature=1,
        max_tokens=1000
        )

    make_q = chat_completion.choices[0].message.content  ## gpt결과값 출력
    output = json.loads(make_q)
    return output