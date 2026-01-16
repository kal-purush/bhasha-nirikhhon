package Homework_7;

import javax.swing.*;
import java.awt.*;

public class PlayingField extends JPanel {
    public static final int HUMAN_VS_AI = 0;
    public static final int HUMAN_VS_HUMAN = 1;


    private static Dimension screenSize = Toolkit.getDefaultToolkit().getScreenSize();

    PlayingField(){
        setBackground(Color.DARK_GRAY);
        //setSize(680, 400);
    }

    void newGame(int gameMode, int fieldSizeX, int fieldSizeY, int victoryCondition) {
        System.out.println("Game mode: " + gameMode +
                "\nField size: " + fieldSizeX + " by " + fieldSizeY +
                "\nVictory conditions: " + victoryCondition + "\n");
        removeAll();
        setLayout(new GridLayout(fieldSizeX, fieldSizeY));

//        JToggleButton[] buttonGrid = new JToggleButton[fieldSizeX * fieldSizeY];
//
//        for (int i = 0; i < buttonGrid.length; i++) {
//            buttonGrid[i] = new JToggleButton("#" + i);
//            add(buttonGrid[i]);
//        }

        JLabel[] labelGrid = new JLabel[fieldSizeX * fieldSizeY];
        for (int i = 0; i < labelGrid.length; i++) {
            labelGrid[i] = new JLabel("・");
            labelGrid[i].setBorder(BorderFactory.createMatteBorder(1, 1, 1, 1, Color.WHITE));
            add(labelGrid[i]);
        }
        revalidate();
        repaint();
        //setVisible(true);
    }
}