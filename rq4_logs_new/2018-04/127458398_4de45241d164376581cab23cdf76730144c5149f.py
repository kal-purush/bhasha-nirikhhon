import random

from faker import Faker

from slackertracker.app import Reaction
from slackertracker.factories.utils import generate_id

fake = Faker()

reaction_names = [
    'smile', 'sweat_smile', 'sunglasses', 'joy', 'disappointed'
]

def generate_reaction(sender, channel, receiver=None, team_id=None):
    if not team_id:
        team_id = generate_id("T")
    
    name = random.choice(reaction_names)
    
    receiver_id = None
    if receiver is not None:
        receiver_id = receiver.id

    return Reaction(
        team_id = team_id,
        name = name,
        sender_id = sender.id,
        channel_id = channel.id,
        receiver_id = receiver_id,
    )