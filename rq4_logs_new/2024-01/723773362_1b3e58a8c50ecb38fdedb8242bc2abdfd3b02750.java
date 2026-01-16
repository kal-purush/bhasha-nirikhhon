package user_interface;

import java.awt.Color;
import java.awt.Dimension;

import javax.swing.JLabel;
import javax.swing.JPanel;

/**
 * Timer
 */
public class Timer extends JPanel {

    private ChessFrame chessFrame;
    private JLabel timerLabel;

    public Timer(ChessFrame chessFrame) {
        this.chessFrame = chessFrame;
        configureTimer();
        createTimer();

        chessFrame.add(this);
    }

    public void configureTimer() {
        this.setPreferredSize(new Dimension(100, 100));
        this.setBackground(Color.white);
    }

    public void createTimer() {
        timerLabel = new JLabel("00:00");
        this.add(timerLabel);
    }
}