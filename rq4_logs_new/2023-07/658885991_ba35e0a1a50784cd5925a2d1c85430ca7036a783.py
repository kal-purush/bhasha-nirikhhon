import openai
from dotenv import dotenv_values

def get_response(user_context, job_context):
    config = dotenv_values(".env")
    openai.api_key = config["OPENAI_API_KEY"]
    response = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=[
            {
                "role": "system", "content": "You are responding to linkedin offers by email"
            },
            {
                "role": "user", "content": "Respond to this job offer in an email format:\n" + job_context + "\n\nThis is mi curriculum vitae:\n" + str(user_context) +"\n\nStart with a brief introduction about myself, mentioning the position im interested in. Highlight me skills and relevant experience that make me an ideal candidate for the position! Get all my details from the curriculum vitae. Important: dont use more than 3000 characters"
            }
        ]
    )
    content = response['choices'][0]['message']['content']
    return content