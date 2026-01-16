package com.qualcomm.ftcrobotcontroller.opmodes.AZopmodes.Tutorials;

import com.qualcomm.robotcore.eventloop.opmode.OpMode;

/**
 * Created by administrator on 9/10/16.
 */
public class IntroToVariables extends OpMode {

    boolean booleanVariable = false;

    int intVariable = 10;
    double doubleVariable = 10.0;

    float floatVariable = 10;
    // Relate float to analog data.

    public void init(){

    }

    public void loop(){
        floatVariable = gamepad1.left_stick_x;
    }

}