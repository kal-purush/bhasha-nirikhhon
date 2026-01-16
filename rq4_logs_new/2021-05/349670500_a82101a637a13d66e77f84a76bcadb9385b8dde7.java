package pl.jg.movingball.helpers;

import java.util.TimerTask;

public class CantChangeStateTimer extends TimerTask {

    Collider collider;
    public CantChangeStateTimer(Collider collider){
        this.collider = collider;
    }

    @Override
    public void run() {
        collider.setCanChangeBallstate(true);
    }
}