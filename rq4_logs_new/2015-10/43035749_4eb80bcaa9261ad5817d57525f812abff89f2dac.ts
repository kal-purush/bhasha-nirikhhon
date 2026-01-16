module GameConstants {
    export var DEFAULT_ROOM = 'demo';
    export var DEFAULT_PLAYER = 'player';

    export var PLAYER_STATES = {
        BUST: 'bust',
        CURRENT: 'current',
        DEALING: 'deal',
        STAY: 'stay',
        WAITING: 'wait',
        WIN: 'win'
    };

    export var PLAYER_ACTIONS = {
        DEAL: 'deal',
        HIT: 'hit',
        STAY: 'stay'
    };

    export var DEALER = 'dealer';
    export var CARD_HIDDEN = 'XX'; // used to hide dealer cards when necessary

    export var EVENTS = {
        ACTIONREMINDER: 'action',
        CARD: 'card',
        GLOBALCHAT: 'globalchat:created',
        TIME: 'time',
        PLAYERSTATE: 'state'
    };
}