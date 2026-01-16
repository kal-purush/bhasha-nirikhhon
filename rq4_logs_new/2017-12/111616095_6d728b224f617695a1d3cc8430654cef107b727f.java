package org.firstinspires.ftc.teamcode;

import com.qualcomm.robotcore.eventloop.opmode.Autonomous;
import com.qualcomm.robotcore.eventloop.opmode.Disabled;
import com.qualcomm.robotcore.eventloop.opmode.LinearOpMode;
import com.qualcomm.robotcore.hardware.DcMotor;
import com.qualcomm.robotcore.util.ElapsedTime;

import org.firstinspires.ftc.robotcore.external.Telemetry;

/**
 * Created by anikaitsingh on 12/12/17.
 */

@Disabled
@Autonomous(name = "DriveDistanceRTP", group = "Test")
public class DriveDistanceRTP extends LinearOpMode{
    public void runOpMode(){
        Thread thread = new Thread();
        thread.start();
        Robot robot = new Robot(this.hardwareMap);
        robot.setDriveMode(DcMotor.RunMode.STOP_AND_RESET_ENCODER);
        robot.stop();

        waitForStart();
        robot.setDriveMode(DcMotor.RunMode.RUN_TO_POSITION);

        int distLeft = 5390;
        int distRight = 5440;

        robot.leftBack.setZeroPowerBehavior(DcMotor.ZeroPowerBehavior.FLOAT);
        robot.rightBack.setZeroPowerBehavior(DcMotor.ZeroPowerBehavior.FLOAT);

        float power = 1;

        robot.leftFront.setTargetPosition(distLeft);
        robot.rightFront.setTargetPosition(distRight);

        robot.leftFront.setPower(1);
        robot.rightFront.setPower(1);


        while(opModeIsActive()){
            double[] positionCurr = robot.getPosition();
            Logging.log("Left Back Motor Position", positionCurr[0], telemetry);
            Logging.log("Left Front Motor Position", positionCurr[1], telemetry);
            Logging.log("Right Back Motor Position", positionCurr[2], telemetry);
            Logging.log("Right Front Motor Position", positionCurr[3], telemetry);
            telemetry.update();

            if(Math.abs(robot.leftFront.getCurrentPosition() - distLeft) < RobotMap.DRIVE_TOLERANCE && Math.abs(robot.rightFront.getCurrentPosition() - distRight) < RobotMap.DRIVE_TOLERANCE){
                robot.setDriveMode(DcMotor.RunMode.RUN_WITHOUT_ENCODER);
                robot.stop();
                break;
            }
        }

        robot.setDriveMode(DcMotor.RunMode.RUN_WITHOUT_ENCODER);
        robot.stop();

        ElapsedTime time  = new ElapsedTime();
        time.startTime();
        while(opModeIsActive()){
            robot.smoothDrive(1, 1);
        }

        robot.stop();
    }
}