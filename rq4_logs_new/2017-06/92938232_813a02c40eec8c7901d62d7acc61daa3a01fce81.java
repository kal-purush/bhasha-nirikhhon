import java.awt.*;

/**
 * Created by luni14 on 2017-05-12.
 */
public class Car {

    private Rectangle[] blocks;

    public Car(){

    // Bilens startposition
    blocks = new Rectangle[7];
    int x = 192;
    int y = 384;

        // Kordinaterna för bilens position
        blocks[0] = new Rectangle(x, y, 32, 32);
        blocks[1] = new Rectangle(x-32, y+32, 32, 32);
        blocks[2] = new Rectangle(x, y+32, 32, 32);
        blocks[3] = new Rectangle(x+32, y+32, 32, 32);
        blocks[4] = new Rectangle(x, y+64, 32, 32);
        blocks[5] = new Rectangle(x-32, y+96, 32, 32);
        blocks[6] = new Rectangle(x+32, y+96, 32, 32);

    }

    /**
     * Metoden flyttar på bilen till höger och vänster.
     * @param dir är antal block som bilen föflyttar sig på.
     */
    public void carMove(int dir){
        int dist = 96;
        for (int i = 0; i < blocks.length; i++)
        blocks[i].x = blocks[i].x+dir*dist;

    }
    // Bilen ritas ut.

    /**
     * Metoden ritar ut bilen
     * @param g
     */
    public void draw(Graphics g) {


        for (int a=0; a < blocks.length; a++) {
            g.setColor(Color.black);
            g.fillRect(blocks[a].x, blocks[a].y, 32, 32);
        }
}
}