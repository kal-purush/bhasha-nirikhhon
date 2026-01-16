import java.applet.*;
import java.awt.*;
import java.awt.event.*;

public class sleepy extends Applet implements KeyListener
{
 int x=600,y=360;
 public void paint(Graphics g)
 {

  addKeyListener(this);
        //face
        g.setColor(Color.yellow);
        g.fillOval(x, y,384,384);

        g.setColor(Color.black);
        g.fillRect(x+50,y+200,100,4);
        g.fillRect(x+250,y+200,100,4);

       

 }
 public void keyPressed(KeyEvent ke){

    int KeyCode = ke.getKeyCode();

    switch(KeyCode)
    {
        case KeyEvent.VK_UP:
        if(y>0)
        y=y-1;
 break;
        case KeyEvent.VK_DOWN:
        if(y<getHeight()-200)
        y=y+1;
        break;
        case KeyEvent.VK_LEFT:
        if(x>0)
        x=x-1;
        break;
        case KeyEvent.VK_RIGHT:
        if(x<getWidth()-200)
        x++;
        break;
    }
    repaint();
 }

 public void keyTyped(KeyEvent ke) {} 

 public void keyReleased(KeyEvent ke){}




}
