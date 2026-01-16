import google.generativeai as palm
import os
import dotenv
import os

dotenv.load_dotenv()
palm_api_key = os.getenv("palm_api_key")



class PalmApi:
    def __init__(self, palm_api_key):
        self.palm_api_key = palm_api_key
        self.refine_count = 0
        self.try_count = 0
        self.max_output_tokens=800
        self.temperature = 0
        self.model="models/text-bison-001"


        palm.configure(api_key=self.palm_api_key)

    def hey_bot(self, prompt):
        completion = palm.generate_text(
            model=self.model,
            prompt=prompt,
            temperature=self.temperature,
            max_output_tokens=self.max_output_tokens,
        )
        response = completion.result

        if len(response) > 2000:
            self.refine_count += 1
            refine_character_prompt = f"""Here is my question {prompt}.
             Here is your response {response}. You exceed the character count of 2000. Make it shorter."""
            self.hey_bot(refine_character_prompt)

        elif response is None:
            self.try_count += 1
            self.hey_bot(prompt)

        else:
            print(f"Refine count: {self.refine_count} \nNumber of tries: {self.try_count}")
            return response



palm_instance = PalmApi(palm_api_key=palm_api_key)
response = palm_instance.hey_bot("How are you ?")