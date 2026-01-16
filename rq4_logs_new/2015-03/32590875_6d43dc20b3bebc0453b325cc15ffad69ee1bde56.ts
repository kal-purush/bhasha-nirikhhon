module objects {

    export class Laser extends objects.GameObject {
        public width: number;
        public height: number;
        public name: string;
        
        public samus: objects.Samus;

        // CONSTRUCTOR ++++++++++++++++++++++++++++++++++++++++++++
        constructor(x: number, y: number, samus: objects.Samus) {
            super("laser");

            this.name = "laser";
            //this.soundString = "explosion";

            this.width = this.getBounds().width;
            this.height = this.getBounds().height;

            this.regX = this.width * 0.5;
            this.regY = this.height * 0.5;

            this.x = x;
            this.y = y;
            this.samus = samus;

            this.soundString = "laser_sound";
        
            
        }
        //PRIVATE METHODS+++++++++++++++++++++++++++++++++++++++++++

        // PUBLIC METHODS ++++++++++++++++++++++++++++++++++++++++++

        public update() {

            this.x += 5;

            if (this.x > constants.SCREEN_WIDTH) {

                this.samus.totalLasers--;
                this.samus.lasers.splice(this.samus.lasers.indexOf(this), 1);//remove a laser from the array
                stage.removeChild(this);
            }
        }
        public hit() {
            console.log("laser exploded");
            createjs.Sound.play("enemyexplosion");
            this.samus.totalLasers--; //decrease the number of lasers in game
            this.samus.lasers.splice(this.samus.lasers.indexOf(this), 1);
            stage.removeChild(this);
        }
    }

}  