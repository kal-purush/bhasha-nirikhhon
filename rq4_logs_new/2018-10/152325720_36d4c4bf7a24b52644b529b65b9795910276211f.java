package ktu.tanks.converters;

import ktu.tanks.Direction;

public class DirectionEnumConverter {

    public static Direction getDirection(int direction) {

        switch (direction) {
            case 0:
                return Direction.Up;
            case 1:
                return Direction.Right;
            case 2:
                return Direction.Left;
            case 3:
                return Direction.Down;

            default:
                return Direction.Up;
        }

    }

    public static int getDirection(Direction direction) {

        switch (direction) {
            case Up:
                return 0;
            case Down:
                return 3;
            case Left:
                return 2;
            case Right:
                return 1;

            default:
                return 0;
        }

    }

}