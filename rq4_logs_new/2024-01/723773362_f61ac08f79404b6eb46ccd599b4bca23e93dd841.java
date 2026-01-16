package user_interface;

import java.awt.Color;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.SwingConstants;
import javax.swing.Timer;

import pieces.Piece.PieceColor;

import java.awt.BorderLayout;

/**
 * Clock
 */
public class Clock {

    TimerPanel timer1;
    TimerPanel timer2;

    public Clock() {
        this.timer1 = new TimerPanel("Black", 600);
        this.timer2 = new TimerPanel("White", 600);

    }

    public void toggleTimer(boolean gameStarted, PieceColor turn) {

        if (!gameStarted) {
            timer1.startTimer();
            gameStarted = true;
        } else {
            if (!timer2.started) {
                timer2.startTimer();
                timer1.stopTimer();
                timer2.started = true;
            } else {
                if (turn == PieceColor.WHITE) {
                    timer1.stopTimer();
                    timer2.continueTimer();
                } else {
                    timer2.stopTimer();
                    timer1.continueTimer();
                }
            }
        }
    }

    private class TimerPanel extends JPanel {

        private JLabel timerLabel;
        private Timer countdownTimer;
        private int startTimeInSeconds;
        public boolean started = false;
        public String name;

        private TimerPanel(String name, int startTimeInSeconds) {
            this.name = name;
            this.startTimeInSeconds = startTimeInSeconds;
            configureTimer();
            createTimer();
        }

        public void configureTimer() {
            this.setPreferredSize(new Dimension(100, 100));
            this.setBackground(Color.white);
            this.setLayout(new BorderLayout());
            JLabel nameLabel = new JLabel(name);
            nameLabel.setFont(new Font("Arial", Font.PLAIN, 16));
            nameLabel.setHorizontalAlignment(SwingConstants.CENTER);
            nameLabel.setVerticalAlignment(SwingConstants.CENTER);
            this.add(nameLabel, BorderLayout.NORTH);
            timerLabel = new JLabel(formatTime(startTimeInSeconds));
            timerLabel.setFont(new Font("Arial", Font.PLAIN, 16));
            timerLabel.setHorizontalAlignment(SwingConstants.CENTER);
            timerLabel.setVerticalAlignment(SwingConstants.CENTER);
            this.add(timerLabel);
        }

        public void createTimer() {
            countdownTimer = new Timer(1000, new ActionListener() {
                @Override
                public void actionPerformed(ActionEvent e) {
                    startTimeInSeconds--;
                    if (startTimeInSeconds >= 0) {
                        timerLabel.setText(formatTime(startTimeInSeconds));
                    } else {
                        ((Timer) e.getSource()).stop();
                        timerLabel.setText("Time's up!");
                    }
                }
            });
        }

        public void startTimer() {
            if (!started) {
                started = true;
                countdownTimer.start();
            }
        }

        public void stopTimer() {
            if (countdownTimer.isRunning()) {
                countdownTimer.stop();
                started = false;
            }
        }

        public void continueTimer() {
            if (!started) {
                started = true;
                countdownTimer.start();
            }
        }

        private String formatTime(int seconds) {
            int minutes = seconds / 60;
            int remainderSeconds = seconds % 60;
            return String.format("%02d:%02d", minutes, remainderSeconds);
        }
    }

}