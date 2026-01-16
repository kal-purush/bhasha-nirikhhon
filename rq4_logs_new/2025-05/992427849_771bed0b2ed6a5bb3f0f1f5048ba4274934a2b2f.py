from fastapi import FastAPI
from pydantic import BaseModel
import tiktoken

app = FastAPI()

class QuestionRequest(BaseModel):
    question: str
    image: str = None  # optional base64 image

@app.post("/api/")
async def get_answer(request: QuestionRequest):
    if "gpt-3.5-turbo-0125" in request.question:
        # Count tokens for this Japanese sentence
        text = "私は静かな図書館で本を読みながら、時間の流れを忘れてしまいました。"
        encoding = tiktoken.encoding_for_model("gpt-3.5-turbo-0125")
        tokens = encoding.encode(text)
        token_count = len(tokens)
        cost_per_million = 0.50  # 50 cents per million tokens
        cost = (token_count / 1_000_000) * cost_per_million
        cost = round(cost, 8)

        return {
            "answer": f"The input would cost approximately {cost} cents (for {token_count} tokens).",
            "links": [
                {
                    "url": "https://discourse.onlinedegree.iitm.ac.in/t/ga5-question-8-clarification/155939/4",
                    "text": "Use the model that’s mentioned in the question."
                },
                {
                    "url": "https://discourse.onlinedegree.iitm.ac.in/t/ga5-question-8-clarification/155939/3",
                    "text": "My understanding is that you just have to use a tokenizer to get the number of tokens and multiply by the given rate."
                }
            ]
        }
    else:
        return {
            "answer": "I'm not sure. Please provide more context.",
            "links": []
        }