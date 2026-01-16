module objects {
    export class GameObject extends createjs.Bitmap {
        //instance variables
        public width;
        public height;
        public isColliding: boolean;
        public soundString: string;
        protected _dx = 5;

        //Constructor/////////////////////////////////////////////////////////////////////////////
        constructor(assetString: string) {
            super(assetLoader.getResult(assetString));

            this.width = this.getBounds().width;
            this.height = this.getBounds().height;

            this.regX = this.getBounds().width * 0.5;
            this.isColliding = false;
        } //constructor ends

    }
}