package com.example.cohalz.entropy;

import android.graphics.Point;

/**
 * Created by cohalz on 15/07/02.
 */
public class BtPlayer extends Player {
    Bt bt;
    BluetoothActivity act;
    public BtPlayer(int number, boolean ban, Bt bt, BluetoothActivity act){
        super(number,ban);
        this.bt = bt;
        this.act = act;
    }

    @Override
    public void doToClick(Player another, Board board,Point point){
        super.doToClick(another,board,point);

        if (board.checkToPoint(point)){
            String message =
                    String.valueOf(move.to.x) + "," +
                            String.valueOf(move.to.y) + "," +
                            String.valueOf(move.from.x) + "," +
                            String.valueOf(move.from.y) + "," +
                            String.valueOf(number);

            if (message != null) {
                act.invalidate();
                bt.sendMessage(message);
            }
        }
        act.invalidate();
    }

}