package touro.ConwaysGameOfLife;

import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;

public class GridMouseListener implements MouseListener {

    public GridMouseListener(Grid grid) {
        System.out.println(grid);
    }

    @Override
    public void mouseClicked(MouseEvent e) {
        System.out.println("clicked");
    }

    @Override
    public void mousePressed(MouseEvent e) {
        System.out.println("clicked");
    }

    @Override
    public void mouseReleased(MouseEvent e) {
        System.out.println("clicked");
    }

    @Override
    public void mouseEntered(MouseEvent e) {
        System.out.println("clicked");
    }

    @Override
    public void mouseExited(MouseEvent e) {
        System.out.println("clicked");
    }
}