# given a list of lists of regexen indentify which list was chosen by the user given some user input in python.
import re
import sys
from pprint import pprint
from subprocess import call
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String

# DB initilzation 
engine = create_engine('sqlite:///./contacts.db', echo=True)
Session = sessionmaker(bind=engine)
Base = declarative_base()
session = Session()

class Contact(Base):
    __tablename__ = 'CONTACTS'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    number = Column(String)
    date = Column(String)

    def __repr__(self):
        return f'{self.name}, {self.number},'

    def tovoice(self):
        return f'{self.name}, {self.number},'

def today():
    date = datetime.now().replace(microsecond=0).isoformat().replace('T',' ')
    return date




# contact = Contact(name="John Doe", number='555-555-9111', date=date)
# session.add(contact)
# session.commit()
#
# contact api

state = {}
state['INTENT']='ROOT'
state['PREVIOUS_INTENT']='ROOT'
contacts = {}

def add_contact(phone_number,name): # todo: store in sqlite
    global contacts
    # contacts[name] = {'phone_number':phone_number,'name':name}
    contact = Contact(name=name, number=phone_number, date=today())
    session.add(contact)
    session.commit()

def udpate_contact(old_name,new_name,phone_number):
    global contacts
    del contacts[old_name]
    contacts[new_name] = {'phone_number':phone_number,'name':new_name}

def contact_list():
    contacts = session.query(Contact).all()
    return contacts

def get_contact_by_name(name):
    global contacts
    if not (name in contacts):
        return None
    return contacts[name]


def say(text):
    call(['say',text])
    print(text)


def greet_root():
    say("Hi, you can say anything, i.e. create or read or udpate.") #todo only once.

def prompt_for_phone_number():
    say("Please provide a phone number for the new contact:") # todo: randomize

def prompt_for_name():
    say("Please provide a name for the new contact:")

def save_and_exit():
    # todo save to db
    # store the full state
    sys.exit(0)

def create_success():
    global state    
    # save to db.
    # clear state
    phone_number = state['CONTACT_PHONE']
    name = state['CONTACT_NAME']

    if 'CONTACT_PHONE' in state:
        del state['CONTACT_PHONE']
    if 'CONTACT_NAME' in state:
        del state['CONTACT_NAME']

    add_contact(phone_number, name)
    say("Contact "+name+" created with phone number:" + phone_number)

def cancel_and_root():
    global state    

    if 'CONTACT_PHONE' in state:
        del state['CONTACT_PHONE']
    if 'CONTACT_NAME' in state:
        del state['CONTACT_NAME']

    say("No Problem!")

def show_list_of_contacts():
    contacts = contact_list()

    if len(contacts) == 1:
        say("There is one contact")
    else:
        say("There are %d  contacts" % (len(contacts),))

    for contact in contacts:
        say(contact.tovoice())


def main():
    global state
    # list of lists
    states = {}
    states['ROOT'] = {'GREET':greet_root}
    states['ROOT']['TRANSITIONS'] = []
    states['ROOT']['TRANSITIONS'].append({'REGEXEN':['create','set','add'],'DESTINATION':'CREATE'})
    states['ROOT']['TRANSITIONS'].append({'REGEXEN':['read', 'show', 'get'],'DESTINATION':'READ'})
    states['ROOT']['TRANSITIONS'].append({'REGEXEN':['update', 'replace'],'DESTINATION':'UPDATE'})
    states['ROOT']['TRANSITIONS'].append({'REGEXEN':['delete','remove'],'DESTINATION':'DELETE'})
    states['ROOT']['TRANSITIONS'].append({'REGEXEN':['quit','exit'],'DESTINATION':'EXIT'})
    
    states['EXIT'] = {'GREET':save_and_exit}
    states['EXIT']['TRANSITIONS'] = []

    states['CREATE'] = {'GREET':prompt_for_phone_number,'STORE':'CONTACT_PHONE'}
    states['CREATE']['TRANSITIONS'] = []
    states['CREATE']['TRANSITIONS'].append({'REGEXEN':['^[0-9]+$'],'DESTINATION':'CREATE_ASK_FOR_NAME'})
    states['CREATE']['TRANSITIONS'].append({'REGEXEN':['[a-zA-Z ]+'],'DESTINATION':'CREATE'})
    states['CREATE']['TRANSITIONS'].append({'REGEXEN':['cancel'],'DESTINATION':'CANCEL_AND_ROOT'})

    states['CREATE_ASK_FOR_NAME'] = {'GREET':prompt_for_name,'STORE':'CONTACT_NAME'}
    states['CREATE_ASK_FOR_NAME']['TRANSITIONS'] = []
    states['CREATE_ASK_FOR_NAME']['TRANSITIONS'].append({'REGEXEN':['.*'],'DESTINATION':'CREATE_SAVE_CONTACT'})

    states['CREATE_SAVE_CONTACT'] = {'GREET':create_success,'NOPROMPT':1}
    states['CREATE_SAVE_CONTACT']['TRANSITIONS'] = []
    states['CREATE_SAVE_CONTACT']['TRANSITIONS'].append({'REGEXEN':['.*'],'DESTINATION':'ROOT'})

    states['CANCEL_AND_ROOT'] = {'GREET':cancel_and_root,'STORE':'CONTACT_PHONE'}
    states['CANCEL_AND_ROOT']['TRANSITIONS'] = []
    states['CANCEL_AND_ROOT']['TRANSITIONS'].append({'REGEXEN':['.*'],'DESTINATION':'ROOT'})


    states['READ'] = {'GREET':show_list_of_contacts}
    states['READ']['TRANSITIONS'] = []
    states['READ']['TRANSITIONS'].append({'REGEXEN':['.*'],'DESTINATION':'ROOT'})
    
    states['UPDATE'] = {}
    states['UPDATE']['TRANSITIONS'] = []
    states['DELETE'] = {}
    states['DELETE']['TRANSITIONS'] = []



    # this is the main greeter.
    states[state['INTENT']]['GREET']()

    while True:

        #pprint(state)
        sys.stdout.write(state['INTENT'] + ">")


        # Ask for input, only if the state requires it.
        do_prompt = 1
        if 'NOPROMPT' in states[state['INTENT']]:
            if states[state['INTENT']]['NOPROMPT'] == 1:
                do_prompt = 0
    
        user_input = 'NOINPUT'
        if do_prompt == 1:
            user_input = input() # todo noprompt

        if 'STORE' in states[state['INTENT']]:
           state[states[state['INTENT']]['STORE']] = user_input

        possible_transitions = states[state['INTENT']]['TRANSITIONS']

        for transition in possible_transitions:
            regex_list = transition['REGEXEN']
            found = False
            for regex in regex_list:
                # Search through user input for word
                if re.search(regex, user_input, flags=re.IGNORECASE):

                    state['PREVIOUS_INTENT'] = state['INTENT']
                    state['INTENT'] = transition['DESTINATION']
                    found = True
                    break

            if found == True:
                break

        if state['PREVIOUS_INTENT'] != state['INTENT']:
            #only if there was change
            if 'GREET' in states[state['INTENT']]:
                states[state['INTENT']]['GREET']()
            else:
                say(state['INTENT']+" HAS NO GREET()")


        #if user_input == 'quit' or user_input == 'exit':
        #    break


if __name__ == "__main__":
    main() 