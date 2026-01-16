package touro.ConwaysGameOfLife;

import javax.swing.*;
import java.awt.*;

public class GridView extends JComponent {

    private final Grid grid;
    public static final int CELL_SIZE = 10;

    public GridView(Grid grid) {
        this.grid = grid;
    }

    @Override
    protected void paintComponent(Graphics g) {
        super.paintComponent(g);
        paintBoard(g);
    }

    private void paintBoard(Graphics g) {
        g.setColor(Color.GREEN);
        g.fillRect(0, 0, getWidth(), getHeight());
    }
}