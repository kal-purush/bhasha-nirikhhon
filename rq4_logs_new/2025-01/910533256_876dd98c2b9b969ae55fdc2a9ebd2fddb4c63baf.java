package tictactoe.domain.usecases;

import java.util.Timer;
import java.util.TimerTask;
import javafx.application.Platform;
import javafx.scene.control.Label;

public class TimerUseCase {

    Timer timer;
    TimerTask task;
    int seconds;
    boolean isX = true;
    boolean isPc;
    OnTimeStopped onTimeStopped;
    Label playerOneTimer, playerTwoTimer;

    public TimerUseCase() {}

    public TimerUseCase(Label playerOneTimer, Label playerTwoTimer) {
        this.playerOneTimer = playerOneTimer;
        this.playerTwoTimer = playerTwoTimer;
    }

    public void startTimer(int s) {
        isPc = true;
        seconds = s;
        createTimer();
        createTask();
        timer.scheduleAtFixedRate(task, 0, 1000);
    }

    public void startTimer(int s, boolean isX) {
        isPc = false;
        this.isX = isX;
        seconds = s;
        createTimer();
        createTask();
        timer.scheduleAtFixedRate(task, 500, 1000);
    }

    public void setOnTimeStopped(OnTimeStopped onTimeStopped) {
        this.onTimeStopped = onTimeStopped;
    }

    public void createTimer() {
        timer = new Timer();
    }

    public void createTask() {
        task = new TimerTask() {
            @Override
            public void run() {
                if (seconds > 0) {
                    seconds--;
                    Platform.runLater(() -> {

                        if (!isPc) { // Player Two
                            if (isX) { //X
                                playerOneTimer.setText("Time: " + seconds + " seconds");
                            } else { //O
                                playerTwoTimer.setText("Time: " + seconds + " seconds");
                            }
                        }

                    });
                } else {

                    Platform.runLater(() -> {

                        if (!isPc) { // Player Two
                            isX = !isX; // reverseXO
                            startTimer(5, isX); // TODO
                            playerOneTimer.setText("");
                            playerTwoTimer.setText("");
                            onTimeStopped.onTimeStopped();
                        } else {
                            onTimeStopped.onTimeStopped();
                        }
                    });
                    timer.cancel();
                }
            }
        };
    }

    public void cancel() {
        timer.cancel();
        playerOneTimer.setText("");
        playerTwoTimer.setText("");
    }
}