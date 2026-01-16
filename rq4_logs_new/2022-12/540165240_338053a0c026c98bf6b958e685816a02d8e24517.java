package org.firstinspires.ftc.teamcode;

import com.qualcomm.robotcore.eventloop.opmode.LinearOpMode;
import com.qualcomm.robotcore.eventloop.opmode.TeleOp;
import com.qualcomm.robotcore.hardware.DcMotor;
import com.qualcomm.robotcore.hardware.DcMotor;
import com.qualcomm.robotcore.hardware.configuration.typecontainers.MotorConfigurationType;
import com.qualcomm.robotcore.util.ElapsedTime;

import org.firstinspires.ftc.robotcore.external.Telemetry;

import Team7159.ComplexRobots.Christopher;

@TeleOp(name = "ArmTest TeleOp")

public class ArmTestTeleOp  extends LinearOpMode {
    private Christopher robot = new Christopher();

    @Override
    public void runOpMode() {

        robot.init(hardwareMap);

        waitForStart();

        //drive controls
        double accel;
        double rotate;
        double powR;
        double powL;

        double mult = 0.5;

        //double maxPower = 0.05;

        while (opModeIsActive()) {
            telemetry.addData("Servo Pos: ", robot.servoClaw.getPosition());
            telemetry.addData("Strafe Mult: ", robot.mult);

            if(gamepad1.right_trigger > 0.1){
                robot.armMotor.setPower(mult * gamepad2.right_trigger);
            } else if (gamepad2.left_trigger > 0.1) {
                robot.armMotor.setPower(-mult * gamepad2.left_trigger);
            } else {
                robot.armMotor.setPower(0);
            }

            //Set servo claw position
            if(gamepad1.y) {
                robot.servoClaw.setPosition(robot.servoClawOpen);
            }else if(gamepad2.a){
                robot.servoClaw.setPosition(robot.servoClawGrab);
            }

            if(gamepad1.x){
                robot.mult += 0.1;
            } else if(gamepad1.b){
                robot.mult -= 0.1;
            }

            //Driving code
//          Left Stick--Acceleration
            accel = gamepad1.left_stick_y;

            //Left Stick--Rotation
            rotate = gamepad1.right_stick_x;

            //Determines ratio of motor powers (by sides) using the right stick
            double rightRatio = 0.5 - (0.5 * rotate);
            double leftRatio = 0.5 + (0.5 * rotate);
            //Declares the maximum power any side can have
            double maxRatio = 1;

            //If we're turning left, the right motor should be at maximum power, so it decides the maxRatio. If we're turning right, vice versa.
            if (rotate < 0) {
                maxRatio = 1 / rightRatio;
            } else {
                maxRatio = 1 / leftRatio;
            }


            //Uses maxRatio to max out the motor ratio so that one side is always at full power.
            powR = rightRatio * maxRatio;
            powL = leftRatio * maxRatio;
            //Uses left trigger to determine slowdown.
            robot.RFMotor.setPower(-powR * accel);
            robot.RBMotor.setPower(-powR * accel);
            robot.LFMotor.setPower(-powL * accel);
            robot.LBMotor.setPower(-powL * accel);


            robot.pivotTurn(1, gamepad1.left_bumper, gamepad1.right_bumper);
            robot.octoStrafe(1, gamepad1.dpad_up, gamepad1.dpad_down, gamepad1.dpad_left, gamepad1.dpad_right);
            telemetry.update();

        }
    }
}