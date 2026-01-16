"""
OpenAI_Functions.py

23 November 2023

https://platform.openai.com/docs/guides/function-calling

"""

from openai import OpenAI
import json

client = OpenAI()

# Example dummy function hard coded to return the same weather
# In production, this could be your backend API or an external API
def get_current_weather(location, unit="fahrenheit"):
    """Get the current weather in a given location"""
    if "tokyo" in location.lower():
        return json.dumps({"location": "Tokyo", "temperature": "10", "unit": "celsius"})
    elif "san francisco" in location.lower():
        return json.dumps({"location": "San Francisco", "temperature": "72", "unit": "fahrenheit"})
    elif "paris" in location.lower():
        return json.dumps({"location": "Paris", "temperature": "22", "unit": "celsius"})
    else:
        return json.dumps({"location": location, "temperature": "unknown"})


def run_conversation():

    # Send the conversation and description of available function call(s) to the model
    messages = [{"role": "user", "content": "What's the weather like in San Francisco, Tokyo, and Paris?"}]
    tools = [
        {
            "type": "function",
            "function": {
                "name": "get_current_weather",
                "description": "Get the current weather in a given location",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "location": {
                            "type": "string",
                            "description": "The city and state, e.g. San Francisco, CA"
                        },
                        "unit": {"type": "string", 
                                 "enum": ["celsius", "fahrenheit"]
                                },
                    },
                    "required": ["location"],
                },
            },
        }
    ]

    # Stage 1 chat.completions
    response = client.chat.completions.create(
        model="gpt-3.5-turbo-1106",
        messages=messages,
        tools=tools,
        tool_choice="auto",  # auto is default, but we'll be explicit
    )

    response_message = response.choices[0].message

    tool_calls = response_message.tool_calls

    # Step 2: check if the model wanted to call a function. The model describes the function calls it wants to make
    # for this_call in tool_calls:
    #     print("this_call", this_call.function)
    # this_call Function(arguments='{"location": "San Francisco", "unit": "celsius"}', name='get_current_weather')
    # this_call Function(arguments='{"location": "Tokyo", "unit": "celsius"}', name='get_current_weather')
    # this_call Function(arguments='{"location": "Paris", "unit": "celsius"}', name='get_current_weather')

    if tool_calls:
        # Note: the JSON response may not always be valid; be sure to handle errors
        available_functions = {
            "get_current_weather": get_current_weather,   # Note: get_current_weather is an actual pointer to the function defined at the top
        }  # only one function in this example, but you can have multiple

        messages.append(response_message)  # extend conversation with assistant's reply

        # Assemble each function call described in output of Stage 1
        for tool_call in tool_calls:

            function_name = tool_call.function.name
            function_to_call = available_functions[function_name] #  Note: function_to_call is an actual pointer to the function defined at the top
            function_args = json.loads(tool_call.function.arguments)

            # Make the function call
            function_response = function_to_call(location=function_args.get("location"), unit=function_args.get("unit"))
            
            # Append each function call and its return value to the Stage 1 message 
            messages.append(
                {
                    "tool_call_id": tool_call.id,
                    "role": "tool",
                    "name": function_name,
                    "content": function_response,
                }
            ) 
        # eo loop


        # Call chat.completions on the Stage 1 message + Stage 1 completions + all function calls and their return values
        # Stage 2 chat.completions
        second_response = client.chat.completions.create(
            model="gpt-3.5-turbo-1106",
            messages=messages,
        )

        return second_response  # return Stage 2
    

stage2_results = run_conversation()
print(stage2_results)