package touro.ConwaysGameOfLife;

import javax.swing.*;
import java.awt.*;

public class GameFrame extends JFrame {

    private JPanel panel;
    private JButton clear;
    private JButton play;
    private JButton next;

    public GameFrame(GridView gridView, Grid grid, GridMouseListener mouseListener) {
        setSize(1000, 400);
        setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
        setTitle("Game of Life");
        setLayout(new BorderLayout());

        clear = new JButton("Clear");
        play = new JButton("Play");
        next = new JButton("Next >");
        panel = new JPanel(new FlowLayout());
        panel.add(clear);
        panel.add(play);
        panel.add(next);

        add(gridView, BorderLayout.CENTER);
        addMouseListener(mouseListener);
        add(panel, BorderLayout.SOUTH);
    }
}