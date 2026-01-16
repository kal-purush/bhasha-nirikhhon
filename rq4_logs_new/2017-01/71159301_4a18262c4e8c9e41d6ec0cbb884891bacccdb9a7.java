package org.firstinspires.ftc.teamcode;

import com.qualcomm.robotcore.eventloop.opmode.LinearOpMode;
import com.qualcomm.robotcore.eventloop.opmode.TeleOp;
import com.qualcomm.robotcore.hardware.ColorSensor;
import com.qualcomm.robotcore.hardware.DcMotor;
import com.qualcomm.robotcore.hardware.DcMotorSimple;
import com.qualcomm.robotcore.hardware.OpticalDistanceSensor;
import com.qualcomm.robotcore.hardware.Servo;

@TeleOp(name="Blue Autonomous")
public class BlueAutonomous extends LinearOpMode {
    DcMotor rightMotor;
    DcMotor leftMotor;
    DcMotor motorbackLeft;
    DcMotor mototrbackRight;
    DcMotor con;
    DcMotor Ball;
    Servo arm1;
    Servo arm2;
    ColorSensor colorSensor;
    OpticalDistanceSensor ods1;
    OpticalDistanceSensor  ods2;


    @Override
    public void runOpMode() throws InterruptedException {
        arm1 = hardwareMap.servo.get("arm1");
        arm2 = hardwareMap.servo.get("arm2");
        rightMotor = hardwareMap.dcMotor.get("motor_right");
        leftMotor = hardwareMap.dcMotor.get("motor_left");
        leftMotor.setDirection(DcMotor.Direction.REVERSE);
        motorbackLeft = hardwareMap.dcMotor.get("motor_backleft");
        motorbackLeft.setDirection(DcMotorSimple.Direction.REVERSE);
        mototrbackRight = hardwareMap.dcMotor.get("motor_backright");
        Ball = hardwareMap.dcMotor.get("shoot");
        colorSensor = hardwareMap.colorSensor.get("sensor_color");
        ods1 = hardwareMap.opticalDistanceSensor.get("ods1");
        ods2 = hardwareMap.opticalDistanceSensor.get("ods2");

        colorSensor.enableLed(false);

        arm1.setDirection(Servo.Direction.REVERSE);
        arm2.setDirection(Servo.Direction.REVERSE);
        arm1.setPosition(0);
        arm2.setPosition(0);
        rightMotor.setPower(0);
        leftMotor.setPower(0);


        waitForStart();

        forwardBeggining(1000, 0.1);
        updateTelemetryStatus();

        shoot(1000);

        moveRight(2300,0.1);
        updateTelemetryStatus();

        moveforwardWithODS(0.1);
        updateTelemetryStatus();

        moveRight(500, 0.1);
        updateTelemetryStatus();

        BeaconPusher();

        moveLeft(250, 0.1);
        updateTelemetryStatus();

        moveforwardWithODS(0.1);
        updateTelemetryStatus();

        moveRight(500, 0.1);
        updateTelemetryStatus();

    }

    private void updateTelemetryStatus() {
        telemetry.addData("Right front : " , rightMotor.getPower());
        telemetry.addData("Right back : " , mototrbackRight.getPower());
        telemetry.addData("Left front : " , leftMotor.getPower());
        telemetry.addData("Left back : " , motorbackLeft.getPower());
        telemetry.addData("Color sensor red", colorSensor.red());
        telemetry.addData("Color sensor blue", colorSensor.blue());
        telemetry.update();
        sleep(1000);
    }

    public void BeaconPusher() {

        telemetry.addData("Blue Color", colorSensor.blue());
        telemetry.addData("Red Color", colorSensor.red());
        telemetry.update();
        sleep(2000);
        if(colorSensor.blue() > 10) {

            arm1.setPosition(1);
            telemetry.addData("In Red", colorSensor.red());
            telemetry.update();
            sleep(1000);
            arm1.setPosition(0);
            telemetry.update();
            sleep(1000);
        }else{
            arm2.setPosition(1);
            telemetry.addData("In blue", colorSensor.blue());
            telemetry.update();
            telemetry.update();
            sleep(1000);
            arm2.setPosition(0);
            telemetry.update();
            sleep(1000);
        }

    }
    public void forwardBeggining (int i, double power) {
        leftMotor.setPower(-power);
        rightMotor.setPower(-power);
        mototrbackRight.setPower(-power);
        motorbackLeft.setPower(-power);
        sleep(i);
        leftMotor.setPower(0);
        rightMotor.setPower(0);
        motorbackLeft.setPower(0);
        mototrbackRight.setPower(0);
    }

    public void moveforwardWithODS (double power) {
        int i = 0;
        while (i < 5000) {
            if (ods1.getRawLightDetected()  >= 1){
                leftMotor.setPower(0);
                rightMotor.setPower(0);
                mototrbackRight.setPower(0);
                motorbackLeft.setPower(0);
                break;
            }else{
                leftMotor.setPower(-power);
                rightMotor.setPower(-power);
                mototrbackRight.setPower(-power);
                motorbackLeft.setPower(-power);
                sleep(100);
            }
            i++;
        }
    }

    public void moveRight (int i, double power) {
        rightMotor.setPower(power);
        mototrbackRight.setPower(-power);
        leftMotor.setPower(-power);
        motorbackLeft.setPower(power);
        sleep(i);
        leftMotor.setPower(0);
        rightMotor.setPower(0);
        motorbackLeft.setPower(0);
        mototrbackRight.setPower(0);
    }
    public void moveLeft (int i, double power) {
        rightMotor.setPower(-power);
        mototrbackRight.setPower(power);
        leftMotor.setPower(power);
        motorbackLeft.setPower(-power);
        sleep(i);
        leftMotor.setPower(0);
        rightMotor.setPower(0);
        motorbackLeft.setPower(0);
        mototrbackRight.setPower(0);
    }
    public void turn (int i) {
        rightMotor.setPower(-0.3);
        leftMotor.setPower(0.3);
        sleep(i);
    }
    public void shoot (int i) {
        Ball.setPower(1);
        sleep(i);
        Ball.setPower(0);
        sleep(100);

    }
}
