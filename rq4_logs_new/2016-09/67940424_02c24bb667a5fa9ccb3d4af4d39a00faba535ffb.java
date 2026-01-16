package org.firstinspires.ftc.teamcode;

import com.qualcomm.robotcore.eventloop.opmode.OpMode;
import com.qualcomm.robotcore.hardware.DcMotor;
import com.qualcomm.robotcore.hardware.DcMotorSimple;

/**
 * Created by administrator on 9/20/16.
 */
public class ArcadeDrive extends OpMode {
    DcMotor leftMotor, rightMotor;
    @Override
    public void init() {
        leftMotor = hardwareMap.dcMotor.get("motorL");
        rightMotor = hardwareMap.dcMotor.get("motorR");
        leftMotor.setDirection(DcMotor.Direction.REVERSE);

    }

    @Override
    public void loop() {
        leftMotor.setPower((-0.5*gamepad1.left_stick_y)-(0.5*gamepad1.left_stick_x));
        leftMotor.setPower((-0.5*gamepad1.left_stick_y)+(0.5*gamepad1.left_stick_x));

    }
}