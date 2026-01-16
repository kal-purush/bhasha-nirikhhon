# poker dealer
from flask import Flask, render_template, request
import random

app = Flask(__name__)


def initialize():
    global state

    state = {
        'players': {},
        'flop': "** ** **",
        'turn': "**",
        'river': "**",
        'phase': "shuffle",
        'dealer': 0,
        'dealer_name': "no-name",
        'deck': [],
        'message': ""
    }


def myplayer(name):
    try:
        return {'name': name,
                'hole': state['players'][name]['hole'],
                'flop': state['flop'],
                'turn': state['turn'],
                'river': state['river'],
                'dealer': state['dealer_name'],
                'phase': state['phase'],
                'message': state['message']}
    except:
        page_not_found(404)


def shuffle():
    ranks = ('A', '2', '3', '4', '5', '6', '7', '8', '9', '10', 'J', 'Q', 'K')
#    suits = ('H', 'C', 'D', 'S')
    suits = ('h', 'c', 'd', 's')
    global deck
    deck = []
    for i in ranks:
        for s in suits:
            deck.append(("{}{}".format(i, s)))
    random.shuffle(deck)
    return deck


def deal_card():
    card = "{}".format(state['deck'][0])
    state['deck'].pop(0)
    return card


def hole():
    return "{} {}".format(deal_card(), deal_card())


def flop():
    return "{} {} {}".format(deal_card(), deal_card(), deal_card())


def turn():
    return "{}".format(deal_card())


def river():
    return "{}".format(deal_card())


def nextdeal():
    # resets game state and selects next dealer
    players = [*state['players']]  # unpacs dictionary
    num_players = len(players)
    state['dealer'] = (state['dealer'] + 1) % num_players
    for player in players:
        state['players'][player]['hole'] = "** **"
        if state['players'][player]['position'] == state['dealer']:
            state['dealer_name'] = player
    state['flop'] = "** ** **"
    state['turn'] = "**"
    state['river'] = "**"

@app.errorhandler(404)
def page_not_found(e):
    # note that we set the 404 status explicitly
    return render_template('404.html'), 404

@app.route('/', methods=['POST', 'GET'])
def index():
    players = [*state['players']]  # unpacs dictionary
    if request.method == 'POST':
        name = request.form['name'].capitalize()
        if len(name) > 2 and name not in state['players']:
            position = len(state['players'])  # keep order of registration
            state['players'][name] = {'hole': "**", 'position': position}
            if position == 0:
                state['dealer_name'] = name
            players = [*state['players']]  # unpacs dictionary
            print(player)
            state['message'] = ""
            return render_template('player.html', players=players, mystatus=myplayer(name))
        else:
            msg = "name must be 3 or more characters and not already taken"
    else:
        msg = "Enter your name below, 3 or more characters"

    return render_template('index.html', message=msg, players=players)


@app.route('/player/<name>')
def player(name):
    try:
        players = [*state['players']]  # unpacs dictionary
    except:
        page_not_found(404)
    return render_template('player.html', name=name, players=players, mystatus=myplayer(name))


#@app.route('/deal', methods=['POST', 'GET'])
@app.route('/deal/<name>')
def deal(name):
    try:
        players = [*state['players']]  # unpacs dictionary
    except:
        page_not_found(404)
    if True: # request.method == 'POST':
        # name = request.form['name']

        if state['phase'] == 'shuffle':  # shuffle and deal hole
            state['deck'] = shuffle()
            state['phase'] = 'hole'

            for player in players:
                state['players'][player]['hole'] = hole()
                state['phase'] = 'flop'

        elif state['phase'] == 'flop':
            state['flop'] = flop()
            state['phase'] = 'turn'

        elif state['phase'] == 'turn':
            state['turn'] = turn()
            state['phase'] = 'river'

        elif state['phase'] == 'river':
            state['river'] = river()
            state['phase'] = 'next-deal'

        elif state['phase'] == 'next-deal':
            state['phase'] = 'shuffle'
            nextdeal()

    return render_template('player.html', name=name, players=players, mystatus=myplayer(name))

@app.route('/admin/<name>')
def admin(name):
    if name == 'reset':
        initialize()
        return "<h1><a href='/'>Syestem Reset, Login</a></h1"

    players = [*state['players']]  # unpacs dictionary
    return render_template('admin.html', name=name, players=players, mystatus=myplayer(name))

    return render_template('admin.html')

@app.route('/status')
def status():
    return state


if __name__ == "__main__":
    initialize()
    app.run(debug=True, host='0.0.0.0')