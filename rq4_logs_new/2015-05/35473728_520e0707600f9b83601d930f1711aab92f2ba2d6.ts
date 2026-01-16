/// <reference path='phaser.comments.d.ts' />

module GestionAirTV {
    export class Game {

        constructor() {
            this.game = new Phaser.Game(800, 600, Phaser.AUTO, 'content', new MenuState());
        }

        game: Phaser.Game;

        handleEvent() {
            var event = {
                type: 'Game',
                state: 'start',
                endTime: new Date(new Date().getTime() + 10000),
                players: [
                    { id: 1, name: 'A' },
                    { id: 2, name: 'B' },
                    { id: 3, name: 'C' }
                ],
                phones: [
                    { number: 111, x: 0, y: 0, orientation: 0 },
                    { number: 222, x: 200, y: 200, orientation: 180 }
                ]
            };
            var state = new GameState(event)
            this.game.state.remove('game');
            this.game.state.add('game', state, true);
        }

    }

    class MenuState extends Phaser.State {
        preload() {
            this.game.load.image('logo', 'images/phaser-logo-small.png');
        }

        create() {
            var logo = this.game.add.sprite(this.game.world.centerX, this.game.world.centerY, 'logo');
            logo.anchor.setTo(0.5, 0.5);
            logo.scale.setTo(0.2, 0.2);
            this.game.add.tween(logo.scale).to({ x: 1, y: 1 }, 2000, Phaser.Easing.Bounce.Out, true);
            logo.inputEnabled = true;
            logo.events.onInputDown.add(function (sprite, pointer) {
                gestionAirTV.handleEvent();
            }, this);
        }
    }

    interface GameStateConfig {
        players: Array<any>;
        phones: Array<PhoneConfig>;
    }
    interface PhoneConfig {
        number: number;
        x: number;
        y: number;
        orientation: number;
    }
    interface PhoneMap {
        [number: number]: Phone;
    }
    interface PlayerMap {
        [id: number]: Player;
    }

    class GameState extends Phaser.State {

        initData: GameStateConfig;
        phones: PhoneMap = {};
        players: PlayerMap = {};

        constructor(init:GameStateConfig) {
            super();
            this.initData = init;
        }

        preload() {
            this.game.load.image('phone', 'images/flags/fr.png');
        }

        create() {
            this.initData.phones.forEach(phoneData => {
                var phone = new Phone(this.game, phoneData);
                this.phones[phoneData.number] = phone;
                this.add.existing(phone);
            });
        }
    }

    class Phone extends Phaser.Sprite {
        number: number;
        state: Phone.State;
        player: Player;
        flag: string;

        constructor(game: Phaser.Game, conf:PhoneConfig) {
            super(game, conf.x, conf.y, 'phone')
            this.number = conf.number;
        }
    }
    module Phone {
        export enum State { RINGING = 1 };
    }

    class Player {
        id: number;
        name: string;
        score: number = 0;

    }



    /* Phone
    - Has an id
    - Has a flag/language (or hosts a question)
    - A state: (not_connected(out of order))/ready/ringing/in-use/(cool-down)
    - hosts a player?
    */
    /* Player
    - has an id
    - has color (or linked to id)
    - has a score
    - A state: Answering Question,finished answering = display new result/(moved to a new phone =answering question)
    - Anitation success/failure (on entering finished answering state)
       - moveTo followef by answering (timer) for answering question test
    - At a phone id?
    - array of phones visited? (= display trail)
    - avg answer time?
    - number of right / wrong questions? / list of answered questions
    */
    /*
    labels and score 
    */
    /*
    Scoreboard
    display current game session scrores
    display overall score? animate current to overall?
    */
    /*
    Timer display next schedule, current time, wait?
    */
}
var gestionAirTV;
window.onload = () => {

    gestionAirTV = new GestionAirTV.Game();

};