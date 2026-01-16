import unittest
import random

from sqlalchemy import func

from slackertracker.app import create_app, db
from slackertracker.app import Channel, User, Reaction
from slackertracker.tests.suite import test_config

from slackertracker.factories.channel import generate_channel
from slackertracker.factories.user import generate_user
from slackertracker.factories.reaction import generate_reaction
from slackertracker.factories.utils import generate_id

def add_users(count, team_id):
    users = [generate_user(team_id) for _ in range(0, count)]

    db.session.add_all(users)
    db.session.commit()
    return users

def add_channels(count, team_id):
    channels = [generate_channel(team_id, is_private=False) for _ in range(0, count)]

    db.session.add_all(channels)
    db.session.commit()
    return channels

def add_reactions_to_user(count, user, users, channels, team_id):
    reactions = []

    for _ in range(0, count):
        sender = random.choice(users)
        receiver = user
        channel = random.choice(channels)

        reaction = generate_reaction(sender, channel, receiver, team_id)
        reactions.append(reaction)

    db.session.add_all(reactions)
    db.session.commit()
    return reactions

def generate_slash_command(team_id, user_id, text):
    pass

# from https://gist.github.com/hest/8798884
def get_count(q):
    count_q = q.statement.with_only_columns([func.count()]).order_by(None)
    count = q.session.execute(count_q).scalar()
    return count

class TestRouteCommands(unittest.TestCase):
    def setUp(self):
        app = create_app(test_config)
        app.testing = True
        self.app = app.test_client()
        db.create_all()

    def test_one_user_one_reaction(self):
        team_id = generate_id("T")

        receiver = generate_user(team_id=team_id)
        sender = generate_user(team_id=team_id)
        channel = generate_channel(team_id=team_id)
        db.session.add_all([receiver, sender, channel])
        db.session.commit()

        query = Reaction.query.filter_by(receiver_id=receiver.id)
        self.assertEqual(get_count(query), 0)

        reaction = generate_reaction(sender, channel, receiver, team_id=team_id)
        db.session.add(reaction)
        db.session.commit()

        query = Reaction.query.filter_by(receiver_id=receiver.id)
        self.assertEqual(get_count(query), 1)
    
    def test_one_user_many_reactions(self):
        team_id = generate_id("T")
        users = add_users(5, team_id)
        receiver = users[0]
        channel = add_channels(1, team_id)[0]

        query = Reaction.query.filter_by(receiver_id=receiver.id)
        self.assertEqual(get_count(query), 0)

        reactions = []

        for i in range(0, 6):
            count = 10 - i
            for _ in range(0, count):
                sender = random.choice(users)
                reaction = generate_reaction(sender, channel, receiver, team_id)
                reaction.name = "test{}".format(i)
                reactions.append(reaction)

        db.session.add_all(reactions)
        db.session.commit()

        query = Reaction.query.filter_by(receiver_id=receiver.id)
        self.assertEqual(get_count(query), 45)

    def tearDown(self):
        db.session.remove()
        db.drop_all()