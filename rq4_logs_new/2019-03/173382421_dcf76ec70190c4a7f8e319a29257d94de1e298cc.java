package controller;

import javafx.fxml.FXML;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import service.timer.StandardTimer;
import service.timer.TimerState;

public class TimerController {
    @FXML
    private Button playpause;

    @FXML
    private Button reset;

    @FXML
    private Label anzeige;

    private StandardTimer timer;

    @FXML
    private void playTimer(){
        if(timer.isStarted()){
            TimerState state = timer.togglePlayPause();
            renameButtons(state);
        }
        else{
            timer.start();
            renameButtons(TimerState.PLAY);
        }
    }

    @FXML
    private void resetTimer(){
        timer.reset();
    }

    public void update(String time){
        anzeige.setText(time);
    }

    public void setTimer(StandardTimer timer){
        this.timer = timer;
    }

    private void renameButtons(TimerState state){
        if(state == TimerState.PLAY){
            playpause.setText("pause");
        }
        else if (state == TimerState.PAUSE){
            playpause.setText("play");
        }
    }

    public void onEnd(){
        playpause.setDisable(true);
    }

    public void onReset(){
        playpause.setText("start");
        playpause.setDisable(false);
    }
}