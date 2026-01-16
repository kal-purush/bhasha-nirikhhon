package ne.game.objects;
import org.newdawn.slick.Graphics;
import org.newdawn.slick.Image;
import org.newdawn.slick.Input;
import org.newdawn.slick.geom.Rectangle;
import org.newdawn.slick.geom.Shape;

public class Crusher extends SpielObjekt {
    private Input input;
    private Rectangle shape;
    private float acceleration = 0.2f;
    public Crusher(int x, int y, Image image, Input input) {
        super(x, y, image);
        this.input = input;
        shape = new Rectangle(x,y,image.getWidth(),image.getHeight());
    }

    @Override
    public void draw(Graphics g) {
        this.getImage().drawCentered(this.getX(),this.getY());

    }
    @Override
    public Shape getShape() {
        return shape;
    }

    @Override
    public void update(int delta, int rotation) {

        if (input.isKeyPressed(Input.KEY_D)) {
            if (rotation<=7) {
                rotation=rotation++;
            } else {
                rotation = 0;
            }
        }
        if (input.isKeyPressed(Input.KEY_A)) {
            if (rotation>=0) {
                rotation=rotation--;
            } else {
                rotation = 7;
            }
        }
        if (input.isKeyDown(Input.KEY_W)) {
               switch(rotation)  {
                   case 0:
                       this.setY(this.getY() - delta);
                       this.setX(this.getX() - delta);
                   case 0:
                       this.setY(this.getY() - delta);
                       this.setX(this.getX() - delta);
                   case 0:
                       this.setY(this.getY() - delta);
                       this.setX(this.getX() - delta);
                   case 0:
                       this.setY(this.getY() - delta);
                       this.setX(this.getX() - delta);
                   case 0:
                       this.setY(this.getY() - delta);
                       this.setX(this.getX() - delta);
                   case 0:
                       this.setY(this.getY() - delta);
                       this.setX(this.getX() - delta);
                   case 0:
                       this.setY(this.getY() - delta);
                       this.setX(this.getX() - delta);
                   case 0:
                       this.setY(this.getY() - delta);
                       this.setX(this.getX() - delta);
               }
            }
            if (this.getY() < 0+this.getHeight()/2) this.setY(0+this.getHeight()/2);
            if (this.getY() > 1080-this.getHeight()/2) this.setY(1080-this.getHeight()/2);
            if (this.getX() < 0+this.getWidth()/2) this.setX(0+this.getWidth()/2);
            if (this.getX() > 1920-this.getWidth()/2) this.setY(1920-this.getWidth()/2);
        }
        if (input.isKeyDown(Input.KEY_S)) {
            this.setY(this.getY() + delta);
            if ((this.getY() > (768-this.getHeight()/2))) this.setY(768-this.getHeight()/2);
        }

        shape.setCenterX(this.getX());
        shape.setCenterY(this.getY());
    }
    public boolean intersects(Shape shape) {
        if (shape != null) {
            return this.getShape().intersects(shape);
        }
        return false;
    }
}
