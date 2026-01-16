package com.example.krzysiek.etapi.HeartDROID.actions;

import heart.Action;
import heart.State;
import com.example.tomek.etapi.ResultActions;
/**
 * Created by Krzysiek on 2016-12-27.
 */

public class setSound implements Action {

    @Override
    public void execute(State state) {
        String argument = String.valueOf(state.getValueOfAttribute("sound_settings"));
        switch (argument){
            case "on":
                System.out.println("Dźwięki włączone"); // TODO on Android
                //ResultActions rs = new ResultActions();
                //rs.normalMode(this);
                break;
            case "off":
                System.out.println("Dźwięki wyłączone"); // TODO on Android
                //ResultActions rs = new ResultActions();
                //rs.silentMode(this);
                break;
            case "vibration":
                System.out.println("Wibracje włączone"); // TODO on Android
                //ResultActions rs = new ResultActions();
                //rs.vibrationsMode(this);
                break;
        }

    }

}