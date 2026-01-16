import processing.core.PApplet;
import processing.core.PImage;

public class Missile {
    //Attributes
    public PImage[] Img;
    private int posX;
    private int posY;
    private int Height;
    private int Width;
    private int Speed;
    private String Type;
    private int infDamage;
    public int currentImg;
    //Constructor
    public Missile(int pX, int pY, int sPd, String type, int dam, PApplet app)
    {
        this.posX = pX;
        this.posY = pY;
        this.Speed = sPd;
        this.Type = type;
        this.infDamage = dam;
        this.Img = new PImage[8];
        for(int i = 0; i < Img.length; i++)
        {
            this.Img[i] = app.loadImage("missile_"+i+".png");
        }
    }
    //Methods
    public void updateCoor()
    {
        this.posX = this.posX + this.Speed;
        this.currentImg ++;
        this.Speed = this.Speed*currentImg;
    }
    public void checkCollision(Entity target)
    {
        if(this.posX <= target.entityX && this.posX >= target.entityX + target.entityH && target.entityY >= this.posY )
        {

        }
    }
}