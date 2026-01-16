import os

import openai
from langchain.chat_models import ChatOpenAI
from langchain.chains import create_extraction_chain


OPENAI_API_KEY = "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"
os.environ['OPENAI_API_KEY'] = OPENAI_API_KEY
openai.api_key = OPENAI_API_KEY

schema = {
    "properties": {
        "parentName": {"type": "string"},
        "phoneNumber": {"type": "string"},
        "email": {"type": "string"},
        "age": {"type": "integer"},
    },
    "required": ["parentName", "phoneNumber","age"],
}

# Run chain
llm = ChatOpenAI(temperature=0, model="gpt-3.5-turbo")
chain = create_extraction_chain(schema, llm)

def complete(input_data: str):
    return chain.run(input_data)

if __name__ == '__main__':
    input_data = ""
    print("Welcome to GenAi Application assistant (type exit to stop)")
    while input_data != "exit":
        print("Please provide the following information: your full name, phone number, email, and kid age.")
        input_data = input()
        if input_data.lower() == "exit":
            break
        response = complete(input_data)
        print(response)

    print("Bye bye")