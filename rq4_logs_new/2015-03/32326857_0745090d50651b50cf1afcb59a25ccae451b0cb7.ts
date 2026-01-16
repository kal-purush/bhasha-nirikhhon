module objects {
    export class Bullet extends objects.GameObject {
        //Constructor/////////////////////////////////////////////////////////////////////////////
      
        
        constructor(x: number , y: number) {
            super("bullet");
            this.name = "bullet";
            this.x = x;
            this.y = y;
           
        } //constructor ends

        
        public update(): void {
            this.x += 5;


            if (this.x > 700) {

                stage.removeChild(this);
            } //if ends
        }

        
    }
} //method update end 