/// <reference path="../objects/ring.ts" />
/// <reference path="../objects/background.ts" />
/// <reference path="../objects/bee.ts" />
/// <reference path="../objects/button.ts" />
/// <reference path="../objects/gameobject.ts" />
/// <reference path="../objects/label.ts" />
/// <reference path="../objects/nemo.ts" />
/// <reference path="../objects/scoreboard.ts" />




module states {
    // PLAY STATE
    export class Play {
        // INSTANCE VARIABLES ++++++++++++++++++++++++++++++++++++++++++++++
        public game: createjs.Container;
        public nemo: objects.Nemo;
        public ring: objects.Ring;
        public bees: objects.Bee[] = [];
        public background: objects.Background;
        public scoreboard: objects.ScoreBoard;

        // CONSTRUCTOR ++++++++++++++++++++++++++++++++++++++++++++++++++++++
        constructor() {
            // Instantiate Game Container
            this.game = new createjs.Container();

            // Add ocean to game
            this.background = new objects.Background();
            this.game.addChild(this.background);


            // Add island to game
            this.ring = new objects.Ring();
            this.game.addChild(this.ring);


            // Add plane to game
            this.nemo = new objects.Nemo();
            this.game.addChild(this.nemo);

            // Add clouds to game
            for (var cloud = constants.CLOUD_NUM; cloud > 0; cloud--) {
                this.bees[cloud] = new objects.Bee();
                this.game.addChild(this.bees[cloud]);
            }

            this.scoreboard = new objects.ScoreBoard(this.game);



            stage.addChild(this.game);
        } // constructor end


        // PUBLIC METHODS ++++++++++++++++++++++++++++++++++++++++++++++++++++

        // Calculate the distance between two points
        distance(p1: createjs.Point, p2: createjs.Point): number {

            return Math.floor(Math.sqrt(Math.pow((p2.x - p1.x), 2) + Math.pow((p2.y - p1.y), 2)));
        } // distance end

        // check collision between nemo and any bee object
        private nemoAndBees(bee: objects.Bee) {
            var p1: createjs.Point = new createjs.Point();
            var p2: createjs.Point = new createjs.Point();
            p1.x = this.nemo.x;
            p1.y = this.nemo.y;
            p2.x = bee.x;
            p2.y = bee.y;
            if (this.distance(p1, p2) < ((this.nemo.height / 2) + (bee.height / 2))) {
                createjs.Sound.play("thunder");
                this.scoreboard.lives -= 1;
                bee._reset();

            }
        }

        // check collision between nemo and ring
        private nemoAndRing() {
            var p1: createjs.Point = new createjs.Point();
            var p2: createjs.Point = new createjs.Point();
            p1.x = this.nemo.x;
            p1.y = this.nemo.y;
            p2.x = this.ring.x;
            p2.y = this.ring.y;
            if (this.distance(p1, p2) < ((this.ring.height / 2) + (this.ring.height / 2))) {
                createjs.Sound.play("yay");
                this.scoreboard.score += 100;
                this.ring._reset();
            }
        }

        // UPDATE METHOD
        public update() {

            this.background.update();
            this.nemo.update();
            this.ring.update();

            if (this.scoreboard.lives > 0) {
                for (var cloud = constants.CLOUD_NUM; cloud > 0; cloud--) {
                    this.bees[cloud].update();
                    this.nemoAndBees(this.bees[cloud]);
                }

                this.nemoAndRing();
            }

            this.scoreboard.update();

            if (this.scoreboard.lives < 1) {
                createjs.Sound.stop();
                this.game.removeAllChildren();
                stage.removeAllChildren();
            }
        } // update method end


    }
}  