package toolboxPanel.mouseAdapter;

import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;

/**
 *
 * @author ΙΩΑΝΝΑ
 */
public class MouseEventsForToolBox implements MouseListener{
    
    boolean clicked;

    @Override
    public void mouseClicked(MouseEvent e) {
        clicked = true;
        System.out.println("einai ston MouseEvent For ToolBox");
    }

    @Override
    public void mousePressed(MouseEvent e) {
        mouseClicked(e);
    }

    @Override
    public void mouseReleased(MouseEvent e) {
       clicked = false;
    }

    @Override
    public void mouseEntered(MouseEvent e) {
    }

    @Override
    public void mouseExited(MouseEvent e) {
    }
    
    public boolean isClicked(){
        
        return clicked;
    } 
}