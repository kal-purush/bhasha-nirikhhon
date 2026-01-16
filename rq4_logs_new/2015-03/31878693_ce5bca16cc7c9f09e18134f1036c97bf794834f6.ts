module objects {
    // plane class
   export class Allien extends createjs.Bitmap {
       // Constructor
       constructor() {
           super(assets.getResult("allien"));

           this.y = 430;
           this.regX = this.getBounds().width * 0.5;
           this.regY = this.getBounds().height * 0.5;



        }
        // Public Method
       public update() {
           this.x = stage.mouseX;
       }
    }
}