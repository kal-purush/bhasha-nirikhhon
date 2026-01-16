from collections import namedtuple

Event = namedtuple("Event", "eventTeam opponent eventId location isGoal shotOutcome")
Game = namedtuple("Game", "homeTeam visitingTeam season")