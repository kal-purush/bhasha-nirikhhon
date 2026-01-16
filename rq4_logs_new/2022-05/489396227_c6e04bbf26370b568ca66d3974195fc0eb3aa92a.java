import java.awt.Graphics;
import java.awt.Color;
import java.awt.Container;
import javax.swing.JFrame;
import javax.swing.JPanel;

/**
 *  Square Rose, a recursive algorithm
 */

public class BoxFractal extends JPanel
{
    private int levels;
    public BoxFractal(int lev)
    {
        levels = lev;
    }
    public void paintComponent(Graphics g)
    {
        super.paintComponent(g);  // Call JPanel's paintComponent method
        //   to paint the background
        int xCenter = getWidth() / 2;
        int yCenter = getHeight() / 2;


        int x = xCenter - 243;
        int y = yCenter - 243;
        int width = 486;
        int height = 486;

        g.setColor(Color.RED);
        drawAndSplit(g, x, y,width,height, levels);

    }

    public int [] midpoints(int [] x)
    {
        int [] m = new int [4];

        m[0] = (x[0] + x[1])/2;
        m[1] = (x[1] + x[2])/2;
        m[2] = (x[2] + x[3])/2;
        m[3] = (x[3] + x[0])/2;

        return m;
    }

    public void drawAndSplit(Graphics g, int x, int y ,int height, int width,int times)
    {
        int a=height/3;
        int b=width/3;

        if(times==1){
            g.fillRect(x,y,a,b);
            return;
        }
        if(times!=0) {
            drawAndSplit(g,x,y,a,b,times-1);
            //top left^
            drawAndSplit(g,x,y+2*b,a,b,times-1);
            //bottom left^
            drawAndSplit(g,x+2*a,y,a,b,times-1);
            //top right^
            drawAndSplit(g,x+2*a,y+2*b,a,b,times-1);
            //bottom right^
            drawAndSplit(g,x+a,y+b,a,b,times-1);
            //middle^

        }
    }
    public static void main(String[] args)
    {
        JFrame window = new JFrame("Fractals");
        window.setBounds(200, 200, 500, 500);
        window.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        SquareRose panel = new SquareRose(5);
        panel.setBackground(Color.WHITE);
        Container c = window.getContentPane();
        c.add(panel);
        window.setVisible(true);
    }
}