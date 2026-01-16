import java.awt.*;
import java.awt.event.*;
import java.applet.*;
/*
<applet code=Mouse width=500 height=500>
</applet>
*/

public class Mouse extends Applet implements MouseListener,MouseMotionListener
{
int X=30,Y=30;
String msg="MOUSE EVENTS";
public void init()
{
addMouseListener(this);
addMouseMotionListener(this);
setBackground(Color.black);
setForeground(Color.white);
}

public void mouseEntered(MouseEvent me)
{
setBackground(Color.cyan);
repaint();
}
public void mouseExited(MouseEvent me)
{
msg="Mouse Exited";
setBackground(Color.red);
showStatus("Mouse Exited");
repaint();
}
public void mousePressed(MouseEvent me)
{
msg="Mouse Pressed";
setBackground(Color.yellow);
showStatus("Mouse Pressed");
repaint();
}
public void mouseReleased(MouseEvent me)
{
msg="Mouse Released";
setBackground(Color.pink);
showStatus("Mouse Released");
repaint();
}
public void mouseClicked(MouseEvent me)
{
msg="Mouse Clicked";
setBackground(Color.green);
showStatus("Mouse Clicked");
repaint();
}
public void mouseMoved(MouseEvent me)
{
msg="Mouse Moved";
X=me.getX();
Y=me.getY();
showStatus("Mouse Moved");
repaint();
}
public void mouseDragged(MouseEvent me)
{
msg="Mouse Dragged";
setBackground(Color.magenta);
showStatus("Mouse Dragged("+me.getX()+","+me.getY()+")");
repaint();
}
public void paint(Graphics g)
{
g.drawString(msg,X,Y);
}
}
