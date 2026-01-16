package org.firstinspires.ftc.teamcode;

import com.qualcomm.robotcore.eventloop.opmode.OpMode;
import com.qualcomm.robotcore.hardware.DcMotor;

/**
 * Created by administrator on 9/20/16.
 */
public class ArcadeDrive extends OpMode{

    DcMotor leftMotor, rightMotor;

    public void init(){
        leftMotor = hardwareMap.dcMotor.get("motorL");
        rightMotor = hardwareMap.dcMotor.get("motorR");
        leftMotor.setDirection(DcMotor.Direction.REVERSE);
    }


    public void loop() {
        leftMotor.setPower(-gamepad1.left_stick_y);
        rightMotor.setPower(gamepad1.left_stick_x);
    }
}