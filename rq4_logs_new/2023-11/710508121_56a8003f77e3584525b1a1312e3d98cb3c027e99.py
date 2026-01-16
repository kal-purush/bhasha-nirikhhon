import pprint
import google.generativeai as palm
import google.auth.transport.requests
from google.protobuf import json_format
import dotenv
import os


dotenv.load_dotenv()
palmp_api_key = os.getenv("palmp_api_key")


class PalmApi:
    
    def __init__(self, prompt,output_max_length):
        self.key = palmp_api_key
        self.refine_count = 0
        self.try_count = 0
        self.prompt =prompt
        self.response =""
        self.output_max_length=output_max_length

    def message_length_refiner_agent(self,  ):
        """
        This method is intended to refine the previous AI output.
        It should make a request to get the previous output text to be re-adjusted depending on your preferences.
        Args:
            output_max_length (int): The maximum words output of the previous AI output. Assumin that each word has on average 6.5 characters.

        """       
        original_prompt = self.prompt
        previous_ai_response = self.response

        refine_character_prompt = f"""
                                Here is my question {original_prompt}.
                                Here is your response {previous_ai_response}. You exceeded the word count of {self.output_max_length}. Make it shorter.
                                Rewrite your response to be {self.output_max_length} words!     
                                    """
        

        refined_output = self.text_generator_agent(refine_character_prompt)
        
        print(f"Refine count: {self.refine_count}")
    
        return refined_output

    def text_generator_agent(self):
        
        palm.configure(api_key=os.getenv("palmp_api_key"))
        
        models = [m for m in palm.list_models() if 'generateText' in m.supported_generation_methods]
        model = models[0].name
        
        completion = palm.generate_text(
        model=model,
        prompt=self.prompt,
        temperature=0,
        # The maximum length of the response
        max_output_tokens=800,)
        
        self.response = completion.result
       
        if (len(self.response))/6.5 > self.output_max_length:
            self.message_length_refiner_agent(self.output_max_length)
            
        
        else:
            print(f"Refine count: {self.refine_count} \nNumber of tries: {self.try_count}")
            return self.response

    

# Uncomment this to test.
# if __name__ == "__main__":
#     prompt = "How are you?"
#     api = PalmApi(prompt,200)
   
#     api.text_generator_agent()
#     print(api.response)
    