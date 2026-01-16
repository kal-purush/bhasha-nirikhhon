package applet;


/*
<applet code="rec" width=200 height=200>
</applet>
*/
 
import java.awt.*;
import java.applet.*;
public class sq extends Applet
{
  public void paint(Graphics g)
  {
    
    g.setColor(Color.pink);
    g.fillRect(100,100,100,100);
    g.drawRect(100,100,100,100);

  }
}