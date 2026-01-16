package org.firstinspires.ftc.teamcode;

import com.qualcomm.robotcore.eventloop.opmode.LinearOpMode;
import com.qualcomm.robotcore.util.ElapsedTime;

import Team7159.ComplexRobots.Christwopher;

@com.qualcomm.robotcore.eventloop.opmode.TeleOp(name="Chris2 Magic Teleop")
public class MagicTeleop2 extends LinearOpMode {

    private Christwopher robot = new Christwopher();

    ElapsedTime et;
    double timeServo;

    final double servoDelay = 100;
    @Override
    public void runOpMode() {

        robot.init(hardwareMap);
        telemetry.addData("LS Motor 1 Pos:", () -> robot.linearSlidesMotor1.getCurrentPosition());
        telemetry.addData("LS Motor 2 Pos:", () -> robot.linearSlidesMotor2.getCurrentPosition());
        telemetry.addData("Servo Claw Pos:", () -> robot.servoClaw.getPosition());

        waitForStart();
        double slowPower = 0.25;

//        while(robot.armMotor.getCurrentPosition() <= 60  && opModeIsActive()) {
//            robot.armMotor.setPower(1);
//            telemetry.addData("Motor Arm Pos: ", robot.armMotor.getCurrentPosition());
//            telemetry.update();
//        }
//        robot.armMotor.setPower(0);

        while (opModeIsActive()) {
            if(gamepad1.left_trigger > 0.1){
                robot.linearSlidesMotor1.setPower(-gamepad1.left_trigger);
                robot.linearSlidesMotor2.setPower(-gamepad1.left_trigger);
            }else if(gamepad1.right_trigger > 0.1){
                robot.linearSlidesMotor1.setPower(gamepad1.right_trigger);
                robot.linearSlidesMotor2.setPower(gamepad1.right_trigger);
            }else{
                robot.linearSlidesMotor1.setPower(0);
                robot.linearSlidesMotor2.setPower(0);
            }
            if (et.time() - timeServo > servoDelay) {
                if (gamepad2.a) {
                    robot.servoClaw.setPosition(robot.servoClaw.getPosition() + 0.05);
                    timeServo = et.time();
//                    telemetry.addData("Claw Servo Position", () -> robot.servoClaw.getPosition());
//                    telemetry.update();
                } else if (gamepad2.b) {
                    robot.servoClaw.setPosition(robot.servoClaw.getPosition() - 0.05);
                    timeServo = et.time();
//                    telemetry.addData("Claw Servo Position", () -> robot.servoClaw.getPosition());
//                    telemetry.update();
                }
            }
            telemetry.update();
        }
    }
}