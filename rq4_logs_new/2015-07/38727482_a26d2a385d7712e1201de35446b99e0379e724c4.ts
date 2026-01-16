module objects {
    //Game Object Class ++++++++++++++++++++++++++++++++++++++++++++
    export class GameObject extends createjs.Bitmap {
        //Public properties ++++++++++++++++++++++++++++++++++
        public width: number;
        public height: number;
        public isColliding; boolean = false;
        public soundString: string = "";

        //Protected properties ++++++++++++++++++++++++++++++++++
        protected dy: number;
        protected dx: number;

        //Constructor ++++++++++++++++++++++++++++++++++++++++
        constructor(imageString: string) {
            super(imageString);
            this.width = this.getBounds().width;
            this.height = this.getBounds().height;
            this.regX = this.width * 0.5;
            this.regY = this.width * 0.5;
        }
    }
}