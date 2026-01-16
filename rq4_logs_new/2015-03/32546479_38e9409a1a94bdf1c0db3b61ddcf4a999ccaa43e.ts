module objects {

    export class GameObject extends createjs.Bitmap {

        // PUBLIC VARIABLES +++++++++++++++++++++++++++++++++++
        public width: number;
        public height: number;
        public isColliding: boolean;
        public soundString: string;
        public name: string;

        // PRIVATE VARIABLE+++++++++++++++++++++++++++++++++++++++++++
        protected _dy;
        protected _dx;

        // CONSTRUCTOR ++++++++++++++++++++++++++++++++++++++++++++
        constructor(assetString: string) {

            super(assetLoader.getResult(assetString));

            this.width = this.getBounds().width;
            this.height = this.getBounds().height;

            //console.log(this.width.toString());
            this.regX = this.width * 0.5;
            this.regY = this.height * 0.5;

            this.isColliding = false;
        }
    }
}     