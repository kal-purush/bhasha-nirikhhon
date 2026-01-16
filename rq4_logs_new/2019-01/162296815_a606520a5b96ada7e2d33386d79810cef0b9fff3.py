from src.Auction import *

courses = {
    'second_price': [Course(capacity=1, name="second_price")]
}

fixed_auctions = {
    'second_price': Auction(
        max_tokens=math.inf,
        courses=courses['second_price'],
        players=[
            Player(utilitites={courses['second_price'][0]: 3.0}),
            Player(utilitites={courses['second_price'][0]: 4.0}),
            Player(utilitites={courses['second_price'][0]: 1.0})
        ]
    )
}