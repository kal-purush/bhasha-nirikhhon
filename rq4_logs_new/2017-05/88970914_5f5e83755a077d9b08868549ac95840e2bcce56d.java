package com.tlyong1992.client.model;

import com.tlyong1992.client.constant.Direction;

/**
 * USER：tangly
 * DATE：2017/5/24
 * TIME：10:58
 */
public class EnemyTank extends BaseTank{

    public EnemyTank(boolean bGood, int x, int y, int tankMoveSpeedX, int tankMoveSpeedY, int tankWidth, int tankHeight, Direction dir) {
        super(bGood, x, y, tankMoveSpeedX, tankMoveSpeedY, tankWidth, tankHeight, dir);
    }

    //当前行走的步数
    private int stepCount = 0;
    public void countStep(){
        this.stepCount ++;
    }
    public void changeDir(Direction dir){
        this.stepCount = 0;
        this.dir = dir;
    }

    public int getStepCount() {
        return stepCount;
    }
}