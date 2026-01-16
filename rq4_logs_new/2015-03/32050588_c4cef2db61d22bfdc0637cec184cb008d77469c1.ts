module objects {
    export class GameObject extends createjs.Bitmap {
        //instance variables
        public width: number;
        public height: number;

        private isColliding: boolean;

        //Getters and Setters//////////////////////////////////////////////////////////////////////
        public setCollission(isColliding): void {
            this.isColliding = isColliding;
        } //method setCollission ends

        //Constructor/////////////////////////////////////////////////////////////////////////////
        constructor(assetString: string) {
            super(assetLoader.getResult(assetString));

            this.width = this.getBounds().width;
            this.height = this.getBounds().height;

            this.regX = this.getBounds().width * 0.5;
            this.regY = this.getBounds().height * 0.5;

            this.setCollission(false);
        } //constructor ends

        //Empty Methods///////////////////////////////////////////////////////////////////////////
        public collide(): void {
        } //method collide ends
    } //class gameObject ends
} //module objects ends   