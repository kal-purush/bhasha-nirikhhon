package com.abunko.leson3.train;

/**
 * Created by Andrew on 28.07.2017.
 */
public class Runner {

    public static void main(String[] args) {
        Train train = new TrainImplementation();
        System.out.println(findLength(train));
    }

    public static int findLength(Train train) {
        boolean b = false;
        train.lightOn();
        int count = 0;
        int result = 0;

        while (!b) {
            train.turnRight();
            count++;
             if (train.isLightOn() == true) {
                train.lightOff();
                result = count;
                for (int i = 0; i < result; i++) {
                    train.turnLeft();
                    count--;
                }
                if (!train.isLightOn()) {
                    b = train.isLength(result);
                }
            }
        }
        return result;
    }



}