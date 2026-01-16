package com.tlyong1992.client.factory;

import com.tlyong1992.client.constant.Constant;
import com.tlyong1992.client.constant.Direction;
import com.tlyong1992.client.model.Tank;
import org.springframework.stereotype.Service;

/**
 * USER：tangly
 * DATE：2017/5/22
 * TIME：16:49
 */
@Service
public class TankFactory {

    public Tank getDefaulMyTank() {
        return new Tank(true, Constant.TANK_POSITION_DEFAULT_X, Constant.TANK_POSITION_DEFAULT_Y, Constant.TANK_MOVE_SPEED_X, Constant.TANK_MOVE_SPEED_Y, Constant.TANK_WIDTH, Constant.TANK_HEIGHT, Direction.STOP);
    }

    public Tank getEnmemyTank(int positionX, int positionY, Direction dir) {
        return new Tank(true, positionX, positionY, Constant.TANK_MOVE_SPEED_X, Constant.TANK_MOVE_SPEED_Y, Constant.TANK_WIDTH, Constant.TANK_HEIGHT, dir);
    }
}