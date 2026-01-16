from __future__ import print_function
from random import randint

# --------------- Helpers that build all of the responses ----------------------

def build_speechlet_response(title, output, reprompt_text, should_end_session):
    return {
        'outputSpeech': {
            'type': 'PlainText',
            'text': output
        },
        'card': {
            'type': 'Simple',
            'title': title,
            'content': output
        },
        'reprompt': {
            'outputSpeech': {
                'type': 'PlainText',
                'text': reprompt_text
            }
        },
        'shouldEndSession': should_end_session
    }


def build_response(session_attributes, speechlet_response):
    return {
        'version': '1.0',
        'sessionAttributes': session_attributes,
        'response': speechlet_response
    }


# --------------- Functions that control the skill's behavior ------------------

def get_welcome_response():
    """ If we wanted to initialize the session to have some attributes we could
    add those here
    """

    session_attributes = {}
    card_title = "Welcome"
    speech_output = "Welcome to the Time Phrase Alexa Skill. " \
                    "I'm going to say few phrases related to time. Ready to tell the equivalent time ?"
    # If the user either does not reply to the welcome message or says something
    # that is not understood, they will be prompted again with this text.
    reprompt_text = "Please tell me if you are ready by saying yes."
    should_end_session = False
    return build_response(session_attributes, build_speechlet_response(
        card_title, speech_output, reprompt_text, should_end_session))


def handle_session_end_request():
    card_title = "Session Ended"
    speech_output = "Thank you for trying the Alexa Skills Kit sample. " \
                    "Have a nice day! "
    # Setting this to true ends the session and exits the skill.
    should_end_session = True
    return build_response({}, build_speechlet_response(
        card_title, speech_output, None, should_end_session))


def set_phrase_in_session(intent, session):
    should_end_session = False
    card_title = "YesIntent"
    option = randint(0, 2)                 # 0 -> half; 1 -> quarter; 2 -> number
    if option == 0 :
        firstPhrase = 'half hour '
    elif option == 1 :
        firstPhrase = 'quarter '
    else:
        minutes = str(randint(1, 59))
        firstPhrase = minutes + ' minutes '

    option = randint(0, 1)
    if option == 0 :
        secondPhrase = 'to '
    else:
        secondPhrase = 'past '

    hour = str(randint(1, 12))
    finalPhrase = firstPhrase + secondPhrase + hour

    session_attributes = {"firstPhrase" : firstPhrase, "secondPhrase" : secondPhrase, "hour" : hour}
    speech_output = "Can you tell the equivalent of " + finalPhrase + " ?"
    reprompt_text = "Do you want me to repeat the question?"
    return build_response(session_attributes, build_speechlet_response(
        card_title, speech_output, speech_output, should_end_session))

def check_answer(intent, session):
    card_title = "AnswerIntent"
    firstPhrase = session['attributes']['firstPhrase']
    secondPhrase = session['attributes']['secondPhrase']
    hour = int(session['attributes']['hour'])
    
    if(firstPhrase.find('half') != -1) :
        two = 30
    elif(firstPhrase.find('quarter') != -1) :
        two = 15
    else :
        two = int(firstPhrase[:2])

    if(secondPhrase.find('to') != -1) :
        hour = hour -1
        two = 60 - two

    if(hour == 0) :
        hour = 12

    if(int(intent['slots']['first']['value']) == hour and int(intent['slots']['second']['value']) == two) :
        speech_output = "Good job!"
    else :
        speech_output = "Sorry, that's the wrong answer."
    
    should_end_session =True

    return build_response({"first":intent['slots']['first']['value'], "second":intent['slots']['first']['value'], "one":hour, "two":two}, build_speechlet_response(
        card_title, speech_output, speech_output, should_end_session))

# --------------- Events ------------------

def on_session_started(session_started_request, session):
    """ Called when the session starts """

    print("on_session_started requestId=" + session_started_request['requestId']
          + ", sessionId=" + session['sessionId'])


def on_launch(launch_request, session):
    """ Called when the user launches the skill without specifying what they
    want
    """

    print("on_launch requestId=" + launch_request['requestId'] +
          ", sessionId=" + session['sessionId'])
    # Dispatch to your skill's launch
    return get_welcome_response()


def on_intent(intent_request, session):
    """ Called when the user specifies an intent for this skill """

    print("on_intent requestId=" + intent_request['requestId'] +
          ", sessionId=" + session['sessionId'])

    intent = intent_request['intent']
    intent_name = intent_request['intent']['name']

    # Dispatch to your skill's intent handlers
    if intent_name == "YesIntent":
        return set_phrase_in_session(intent, session)
    elif intent_name == "AnswerIntent":
        return check_answer(intent, session)
    else:
        raise ValueError("Invalid intent")


def on_session_ended(session_ended_request, session):
    """ Called when the user ends the session.

    Is not called when the skill returns should_end_session=true
    """
    print("on_session_ended requestId=" + session_ended_request['requestId'] +
          ", sessionId=" + session['sessionId'])
    # add cleanup logic here


# --------------- Main handler ------------------

def lambda_handler(event, context):
    
    print("event.session.application.applicationId=" +
          event['session']['application']['applicationId'])

    if event['session']['new']:
        on_session_started({'requestId': event['request']['requestId']},
                           event['session'])

    if event['request']['type'] == "LaunchRequest":
        return on_launch(event['request'], event['session'])
    elif event['request']['type'] == "IntentRequest":
        return on_intent(event['request'], event['session'])
    elif event['request']['type'] == "SessionEndedRequest":
        return on_session_ended(event['request'], event['session'])