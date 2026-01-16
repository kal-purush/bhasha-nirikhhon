import json
import logging
import random

from rest_framework.renderers import JSONRenderer

from api.serializers.manuscript import ManuscriptSerializer
from messenger_bot.graph_api import get_user_profile
from messenger_bot.messages import format_text, format_question, TYPE_ANSWER, format_quick_reply_next, \
    format_image_attachment
from messenger_bot.models import ChatSession
from messenger_bot.send_api import send_message

from quiz.models import Manuscript, ManuscriptItem

logger = logging.getLogger(__name__)


def get_replies(sender_id, session):
    replies = []
    manus = session.meta['manuscript']
    item = manus['items'][session.meta['current_item']]

    while item['type'] == ManuscriptItem.TYPE_TEXT and session.meta['current_item'] < len(manus['items']):
        replies.append(format_text(sender_id, item['text']))
        session.meta['current_item'] += 1
        item = manus['items'][session.meta['current_item']]

    if item['type'] == ManuscriptItem.TYPE_PROMISES and session.meta['current_promise'] < len(manus['promises']):
        replies.append(format_question(sender_id, manus['promises'][session.meta['current_promise']], session.uuid))
        session.meta['current_promise'] += 1

    elif item['type'] == ManuscriptItem.TYPE_PROMISES and session.meta['current_promise'] == len(manus['promises']):
        session.meta['current_item'] += 1

    elif item['type'] == ManuscriptItem.TYPE_BUTTON:
        # FIXME: Create new type quick reply
        replies.append(format_quick_reply_next(sender_id, item['text'], session.uuid))
        session.meta['current_item'] += 1

    return replies


def is_manuscript_complete(session):
    _is_last_item = session.meta['current_item'] >= len(session.meta['manuscript']['items']) - 1
    _is_last_promise = session.meta['current_promise'] >= len(session.meta['manuscript']['promises']) - 1

    return _is_last_item and _is_last_promise


def received_message(event):
    # Is new session?
    session = ChatSession.objects.filter(user_id=event['sender']['id'], state=ChatSession.STATE_IN_PROGRESS).first()
    if not session:
        # NEW
        session = init_session(event)

    # Send replies
    replies = get_replies(event['sender']['id'], session)

    # Update session
    session.save()

    for reply in replies:
        print("reply:", reply)
        send_message(reply)

    if is_manuscript_complete(session):
        session.state = ChatSession.STATE_COMPLETE
        session.save()


def init_session(event):
    m_initial = Manuscript.objects.first()
    if not m_initial:
        msg = "No manuscripts, bailing..."
        send_message(format_text(event['sender']['id'], msg))
        raise Exception(msg)

    # Serialize what we need and put in the session state
    manus = json.loads(JSONRenderer().render(ManuscriptSerializer(m_initial).data).decode())
    meta = {
        'manuscript': manus,
        'current_item': 0,
        'current_promise': 0,
        'first_name': ''
    }

    return ChatSession.objects.create(user_id=event['sender']['id'], meta=meta)


def get_question_replies(sender_id, session, payload):
    first_name = session.meta['first_name']
    if not first_name:
        first_name = session.meta['first_name'] = get_user_profile(sender_id)['first_name']

    # Get last asked promise
    p_i = session.meta['current_promise']
    if p_i > 0:
        p_i -= 1
    promise = session.meta['manuscript']['promises'][p_i]

    # Is answer correct?
    if payload['answer'] == promise['status']:
        text = 'Good guess {}, that promise was {}'.format(first_name, promise['status'])
    else:
        text = 'Sorry {}, thats wrong, that promise was {}'.format(first_name, promise['status'])

    replies = [format_text(sender_id, text)]

    # Try to get a random image of correct type
    images = list(filter(lambda x: x['type'] == promise['status'], session.meta['manuscript']['images']))
    if images:
        image = random.choice(images)
        replies.append(format_image_attachment(sender_id, image['url']))

    return replies


def received_postback(event):
    payload = json.loads(event['postback']['payload'])

    # Get session id
    session = ChatSession.objects.get(uuid=payload['chat_session'])

    if is_manuscript_complete(session):
        session.state = ChatSession.STATE_COMPLETE
        session.save()

    replies = []
    # Add answer replies
    if payload.get('type') == TYPE_ANSWER:
        replies = get_question_replies(event['sender']['id'], session, payload)

    replies += get_replies(event['sender']['id'], session)
    # Update session
    session.save()

    for reply in replies:
        print("reply:", reply)
        send_message(reply)

    if is_manuscript_complete(session):
        session.state = ChatSession.STATE_COMPLETE
        session.save()