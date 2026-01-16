import random

from faker import Faker

from slackertracker.app import Channel
from slackertracker.factories.utils import generate_id

fake = Faker()

def generate_channel(team_id=None, is_private=None, private_probability=0.15):
    slack_id = generate_id("C")
    if not team_id:
        team_id = generate_id("T")
    name = fake.word()
    
    if is_private is None:
        is_private = random.random() < private_probability

    return Channel(
        slack_id = slack_id,
        team_id = team_id,
        name = name,
        is_private = is_private,
    )