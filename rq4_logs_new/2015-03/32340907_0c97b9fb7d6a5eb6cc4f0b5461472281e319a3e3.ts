module objects {
    export class Button extends createjs.Bitmap{
        
 //instance variables
        public width;
        public height;
        public soundString: string;

    //Constructor/////////////////////////////////////////////////////////////////////////////
    constructor(sound, image) {
        super(assetLoader.getResult(image));
        
        this.width = this.getBounds().width;
        this.height = this.getBounds().height;

        this.regX = this.getBounds().width * 0.5;
        this.regY = this.getBounds().height * 0.5;
        this.soundString = sound;
        this.on("mouseover", this.onButtonOver);
        this.on("mouseout", this.onButtonOut);  
    } //constructor ends


     setButtonListeners() {
        this.cursor = 'pointer';
       // this.on('rollover', this.onButtonOver);
       // this.on('rollout', this.onButtonOut);

    }

      onButtonOver() {
        this.alpha = 0.8;
    }

     onButtonOut() {
        this.alpha = 1;
    }
}
}