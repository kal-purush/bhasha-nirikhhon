package org.firstinspires.ftc.teamcode;

import com.qualcomm.robotcore.eventloop.opmode.LinearOpMode;
import com.qualcomm.robotcore.hardware.DcMotor;

import org.openftc.easyopencv.OpenCvCamera;
import org.openftc.easyopencv.OpenCvCameraFactory;
import org.openftc.easyopencv.OpenCvInternalCamera;

import Team7159.ComplexRobots.Christopher;
import Team7159.Enums.Direction;

@com.qualcomm.robotcore.eventloop.opmode.Autonomous(name = "AutoRight")
public class AutoRight extends LinearOpMode {

    private Christopher robot = new Christopher();

    SleeveDetection sleeveDetection = new SleeveDetection();
    OpenCvCamera phoneCam;
    String webcamName = "Webcam 1";
    public int location = 1;

    double power = 0.4;
    double mult2 = 0.28;

    @Override
    public void runOpMode() throws InterruptedException {
        robot.init(hardwareMap);


        // Signal Sleeve

        int cameraMonitorViewId = hardwareMap.appContext.getResources().getIdentifier("cameraMonitorViewId", "id", hardwareMap.appContext.getPackageName());
        phoneCam = OpenCvCameraFactory.getInstance().createInternalCamera(OpenCvInternalCamera.CameraDirection.BACK, cameraMonitorViewId);
        sleeveDetection = new SleeveDetection();
        phoneCam.setPipeline(sleeveDetection);

        phoneCam.openCameraDeviceAsync(new OpenCvCamera.AsyncCameraOpenListener()
        {
            @Override
            public void onOpened()
            {
                phoneCam.startStreaming(320,240);
            }

            @Override
            public void onError(int errorCode) {}
        });

        while (!isStarted()) {
            if(sleeveDetection.getPosition() == SleeveDetection.ParkingPosition.LEFT) {
                location = 1;
            }
            else if (sleeveDetection.getPosition() == SleeveDetection.ParkingPosition.CENTER) {
                location = 2;
            }
            else {
                location = 3;
            }
            telemetry.addData("ROTATION: ", sleeveDetection.getPosition());
            telemetry.update();
        }
        robot.servoClaw.setPosition(robot.servoClawGrab);

        //Auto

        waitForStart();

        telemetry.addData("LBMotor Pos: ", robot.LBMotor.getCurrentPosition());
        telemetry.addData("RBMotor Pos: ", robot.RBMotor.getCurrentPosition());
        telemetry.addData("LFMotor Pos: ", robot.LFMotor.getCurrentPosition());
        telemetry.addData("RFMotor Pos: ", robot.RFMotor.getCurrentPosition());

        strafe(Direction.RIGHT, 1.0, 0.1);
        rotate(Direction.RIGHT, 1.0, 5);


        if(location == 1) {
            strafe(Direction.FORWARDS, 1.0, 1);
            rotate(Direction.RIGHT, 1.0, 87);
            strafe(Direction.FORWARDS, 1.0, 1.1);
        }
        else if(location == 2) {
            rotate(Direction.RIGHT, 1.0, 87);
            strafe(Direction.FORWARDS, 1.0, 1.2);
        }
        else if(location == 3) {
            strafe(Direction.BACKWARDS, 1.0, 1);
            rotate(Direction.RIGHT, 1.0, 87);
            strafe(Direction.FORWARDS, 1.0, 1.3);
        }
    }

    // Tile Version
    public void strafe(Direction direction, double power, double tiles) throws InterruptedException{

        double tileTime = 1100.0;
        double time = tiles * tileTime;
        if(direction == Direction.LEFT){
            robot.LFMotor.setPower(-power * mult2);
            robot.RFMotor.setPower(power);
            robot.LBMotor.setPower(power * mult2);
            robot.RBMotor.setPower(-power);
            sleep((long) time);
//            sleep((long)tiles * tileTime * (1 / (long) power));
            robot.stop();
        }else if(direction == Direction.RIGHT){
            robot.LFMotor.setPower(power * mult2);
            robot.RFMotor.setPower(-power);
            robot.LBMotor.setPower(-power * mult2);
            robot. RBMotor.setPower(power);
            sleep((long) time);
//            sleep((long)tiles * tileTime * (1 / (long) power));
            robot.stop();
        }
        else if(direction == Direction.FORWARDS) {
            robot.LFMotor.setPower(power * mult2);
            robot.RFMotor.setPower(power);
            robot.LBMotor.setPower(power * mult2);
            robot.RBMotor.setPower(power);
            sleep((long) time);
            robot.stop();
//            sleep((long)tiles * tileTime * (1 / (long) power));
//            robot.stop();
        }
        else if(direction == Direction.BACKWARDS) {
            robot.LFMotor.setPower(-power * mult2);
            robot.RFMotor.setPower(-power);
            robot.LBMotor.setPower(-power * mult2);
            robot.RBMotor.setPower(-power);
//            sleep((long)tiles * tileTime * (1 / (long) power));
//            robot.stop()
            sleep((long) time);
            robot.stop();
        }
        else{
            //Throw an exception
        }

    }

    // Rotate angle method
    public void rotate(Direction direction, double power, double inputAngle) throws InterruptedException {
//        double timeTest = 5000;
//        double angleMoved = 510;
        //time for 90:
        double angleTime = 750.0;
        double time = inputAngle * (angleTime / 90.0);

        if(direction == Direction.LEFT) {

            robot.RFMotor.setPower(power);
            robot.LFMotor.setPower(-power * mult2);
            robot.RBMotor.setPower(power);
            robot.LBMotor.setPower(-power * mult2);
            sleep((long) time);
            robot.stop();
        } else if(direction == Direction.RIGHT) {
            robot.RFMotor.setPower(-power);
            robot.LFMotor.setPower(power * mult2);
            robot.RBMotor.setPower(-power);
            robot.LBMotor.setPower(power * mult2);
            sleep((long) time);
            robot.stop();
        }
    }
}