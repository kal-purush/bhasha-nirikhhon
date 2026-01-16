package service.timer;

import javafx.beans.property.SimpleStringProperty;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;

public class StandardTimer extends Timer{

    private Button play;
    private Button pause;
    private Label timerLabel;

    public StandardTimer(TimerMode mode, int millis) {
        super(mode, millis);
    }


    @Override
    public Region createTimerPane() {
        play = new Button("play");
        pause = new Button("pause");
        play.setOnAction(e -> {
            if(isStarted()){
                play();
            }
            else{
                start();
            }
        });
        pause.setOnAction(e -> pause());
        timerLabel = new Label();

        millis.addListener(e-> update());
        update();
        return new VBox(play, pause, timerLabel);
    }

    private void update(){
        timerLabel.setText(millis.get()/1000 + ":" + (millis.get()%1000)/10);
    }

    @Override
    public void timerEnd() {
        System.out.println("finished");
    }
}