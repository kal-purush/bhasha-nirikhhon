from ast import pattern
from http.client import responses
import re
import random
from urllib import response
rules ={
    'greeting':{
        'patterns':[r'hello',r'hi',r'hey'],
        'responses':['Hello ! Welcome to our food ordering service.','HI there! How can I assist you with you order ?']
    },
    'menu':{
        'patterns':[r'menu',r'options'],
        'responses':['Sure!, Here is our menu: ...\n 1.Pav Bhaji \n Vada pav \n 3. Maggi \n 4.chai'],
    },
    'food items':{
        'patterns':[r'Vada Pav',r'Pav Bhaji',r'Chai',r'Maggi'],
        'responses':['Do you want to confirm your order??'],
    },
    'order':{
        'patterns':[r'order',r'I want to order'],
        'responses':['Greate! please let me know items you would like to order'],
    },
    'customization':{
        'patterns':[r'customize',r'special request'],
        'responses':['Cerainaly ! Let me know you specific requirentment or any dietary restrincations. '],
    },
    'confirm_order':{
        'patterns':[r'confirm',r'place order'],
        'responses':['Prefect! your order has been placed. THe estimated delilvery time is approximately 30 minutes'],
    },
    'gratitude':{
        'patterns':[r'thank you'],
        'responses':['My pleasure']
    },
    'cancel_order':{
        'patterns':[r'cancel',r'change',r'update'],
        'responses':['I apologize for the inconvenience. Please contact our customer support for order modifications '],

    },
    'goodbye':{
        'patterns':[r'bye',r'goodbye',r'see you'],
        'responses':['Thank you for choosing our food ordering serivce. Goodbye!'],

    },
    'default':{
        'responses':['I am sorrry , I didnt uderstand that, can you please rephrase?']

    }
}

# to match user input with patterns
def match_patterns(user_input,patterns):
    for pattern in patterns:
        match = re.search(pattern,user_input,re.IGNORECASE)
        if match:
            return True
    return False

#function to get chatbot response
def get_response(user_input):
    for intent,data in rules.items():
        patterns = data.get('patterns')
        if patterns and match_patterns(user_input,patterns):
            responses = data.get('responses')
            return random.choice(responses)
    return random.choice(rules['default']['responses'])

#main conversation loop

def chat():
    print("chatbot: Hello! welcome to our food ordering services")
    while True:
        user_input = input("User: ")
        response = get_response(user_input)
        print("Chatbot: ",response)

        #Exit the loop if the user says goodbye
        if any(re.search(pattern,user_input) for pattern in rules['goodbye']['patterns']):
            break;

chat()