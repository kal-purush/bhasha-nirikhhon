import processing.core.PApplet;

public class MySketch extends PApplet {
    public static void main(String[] args) {
		PApplet.runSketch(new String[]{""}, new MySketch());
    }

    Ball b1 = new Ball(color(20,50,230));
    Ball b2 = new Ball(color(230,240,17));

    int speedX = 5;
    int speedY = 5;
    int speddX2 = 6;
    int speddY2 = 7;

    public void settings() {
        size(600,600);
    }

    public void setup() { 
        background(255);
        b1.randomPos();
        b2.randomPos();
    }

    public void draw() {

        background(0);
       // b1.randomPos();
        b1.draw();
       // b2.randomPos();
        b2.draw();

         b1.x = b1.x+speedX;
            b1.y = b1.y+speedY;

        b2.x = b2.x+speddX2;

        b2.y = b2.y+speddY2;

        if(b1.x >= width || b1.x <=0){
            speedX = -speedX;
        }
        if(b1.y >= height || b1.y <=0){
            speedY = -speedY;
        }

        if(b2.x >= width || b2.x <=0){
            speddX2 = -speddX2;
        }
        if(b2.y >= height || b2.y <=0){
            speddY2 = -speddY2;
        }

        if( Math.sqrt(Math.pow((b1.x-b2.x),2)+Math.pow((b1.y-b2.y),2))<=60){
            
            speedX = -speedX;
            speedY = -speedY;
            speddX2 = -speddX2;
            speddY2 = -speddY2;
        }
    }
    class Ball {
        int x, y;
        int color;
        Ball(int color) {
            this.color = color;
            randomPos();
        }
        void randomPos() {
            x = (int)(Math.random() * width);
            y = (int)(Math.random() * height);
        }
        void draw() {
            fill(color);
            ellipse(x,y,10,20);
            ellipse(x,y,20,10);
        }
    }
}
