import javax.swing.*;
import java.awt.*;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import java.awt.event.MouseMotionListener;

/**
 * Created by Danyo on 2017-06-22.
 */
public class MapPanel extends JPanel implements MouseListener, MouseMotionListener {

    Map map ;
    public MapPanel() {
        map = new Map() ;

    }

    public void paint(Graphics g) {
        Graphics2D g2 = (Graphics2D) g ;
        float thickness ;
        Stroke oldStroke = g2.getStroke() ;
        for(int i = 0 ; i < map.DEFAULT_SIZE ; i++) {
            for(int j = 0 ; j < map.DEFAULT_SIZE ; j++) {
                g2.setColor(map.map[i][j].getCellColor()) ;
                g2.fillRect(40 + (i * map.map[i][j].getWidth()), 40 + (j * map.map[i][j].getHeight()), map.map[i][j].getWidth(), map.map[i][j].getHeight()) ;

                thickness = map.map[i][j].getBorder() ;
                g2.setStroke(new BasicStroke(thickness)) ;
                g2.setColor(map.map[i][j].BORDER_COLOR_DARK) ;
                g2.drawRect(40 + (i * map.map[i][j].getWidth()), 40 + (j * map.map[i][j].getHeight()), map.map[i][j].getWidth(), map.map[i][j].getHeight()) ;

            }
        }
    }

    public void mouseExited(MouseEvent e) {

    }

    public void mouseReleased(MouseEvent e) {

    }

    public void mousePressed(MouseEvent e) {

    }

    public void mouseEntered(MouseEvent e) {

    }

    public void mouseMoved(MouseEvent e) {

    }

    public void mouseClicked(MouseEvent e) {

    }

    public void mouseDragged(MouseEvent e) {

    }


}