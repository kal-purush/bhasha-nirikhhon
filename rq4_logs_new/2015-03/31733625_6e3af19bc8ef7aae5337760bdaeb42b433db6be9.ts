module objects {
    export class GameObject extends createjs.Bitmap {
        //instance variables
        public width: number;
        public height: number;
        public isColliding: boolean;
        public soundString: string;

        protected _dx;
        protected _dy;

        //Constructor/////////////////////////////////////////////////////////////////////////////
        constructor(assetString: string) {
            super(assetLoader.getResult(assetString));

            this.width = this.getBounds().width;
            this.height = this.getBounds().height;

            this.regX = this.getBounds().width * 0.5;
            this.regY = this.getBounds().height * 0.5;

            this.isColliding = false;
        } //constructor ends
    } //class gameObject ends
} //module objects ends  