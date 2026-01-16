package org.firstinspires.ftc.teamcode;


import com.qualcomm.hardware.bosch.BNO055IMU;
import com.qualcomm.hardware.bosch.JustLoggingAccelerationIntegrator;
import com.qualcomm.robotcore.eventloop.opmode.OpMode;
import com.qualcomm.robotcore.eventloop.opmode.TeleOp;
import com.qualcomm.robotcore.hardware.DcMotor;
import com.qualcomm.robotcore.hardware.DcMotorEx;
import com.qualcomm.robotcore.hardware.PIDFCoefficients;
import com.qualcomm.robotcore.hardware.Servo;

import org.firstinspires.ftc.robotcore.external.navigation.AngleUnit;
import org.firstinspires.ftc.robotcore.external.navigation.AxesOrder;
import org.firstinspires.ftc.robotcore.external.navigation.AxesReference;
import org.firstinspires.ftc.robotcore.external.navigation.Orientation;
import org.firstinspires.ftc.robotcore.external.navigation.Position;
import org.firstinspires.ftc.robotcore.external.navigation.Velocity;

import java.util.Locale;

@TeleOp(name= "RoverTeleOp")
public class RoverTeleOp  extends OpMode {
    Orientation angles;
    BNO055IMU imu;
    String angleDouble = "hi";
    DcMotorEx leftMotor;
    DcMotorEx rightMotor;
    DcMotorEx middleMotor;
    DcMotorEx shoulderMotor;
    DcMotorEx elbowMotor;
    DcMotorEx rotationMotor;
    DcMotorEx hangMotor;
    HDriveFCCalc calculator;
    ArmCalculator armCalculator;
    Servo leftIntake;
    Servo rightIntake;
    Servo bouncer;
    double bouncerPos = 0.0;
    double bicep = 8.0;
    double forearm = 8.0;

    public void init(){
        BNO055IMU.Parameters parameters = new BNO055IMU.Parameters();
        parameters.angleUnit           = BNO055IMU.AngleUnit.DEGREES;
        parameters.accelUnit           = BNO055IMU.AccelUnit.METERS_PERSEC_PERSEC;
        parameters.calibrationDataFile = "AdafruitIMUCalibration.json"; // see the calibration sample opmode
        parameters.loggingEnabled      = true;
        parameters.loggingTag          = "IMU";
        parameters.accelerationIntegrationAlgorithm = new JustLoggingAccelerationIntegrator();
        imu = hardwareMap.get(BNO055IMU.class, "imu");
        imu.initialize(parameters);
        imu.startAccelerationIntegration(new Position(), new Velocity(), 1000);
        leftMotor = (DcMotorEx)hardwareMap.get(DcMotor.class,"leftMotor");
        rightMotor = (DcMotorEx)hardwareMap.get(DcMotor.class, "rightMotor");
        middleMotor = (DcMotorEx)hardwareMap.get(DcMotor.class, "middleMotor");
        shoulderMotor = (DcMotorEx)hardwareMap.get(DcMotor.class, "shoulderMotor");
        elbowMotor = (DcMotorEx)hardwareMap.get(DcMotor.class, "elbowMotor");
        rotationMotor = (DcMotorEx)hardwareMap.get(DcMotor.class, "rotationMotor");
        hangMotor = (DcMotorEx)hardwareMap.get(DcMotor.class, "hangMotor");

        calculator = new HDriveFCCalc();
        armCalculator = new ArmCalculator(bicep, forearm);

        leftIntake = hardwareMap.get(Servo.class,"leftIntake");
        rightIntake = hardwareMap.get(Servo.class, "rightIntake");
        bouncer = hardwareMap.get(Servo.class, "bouncer");
        rightIntake.setDirection(Servo.Direction.REVERSE);
        leftIntake.setPosition(0.5);
        rightIntake.setPosition(0.5);
        bouncer.setPosition(0);


        leftMotor.setDirection(DcMotor.Direction.REVERSE);
        leftMotor.setMode(DcMotor.RunMode.RUN_USING_ENCODER);
        rightMotor.setMode(DcMotor.RunMode.RUN_USING_ENCODER);
        middleMotor.setMode(DcMotor.RunMode.RUN_USING_ENCODER);
        PIDFCoefficients pidf = leftMotor.getPIDFCoefficients(DcMotor.RunMode.RUN_USING_ENCODER);
        pidf.p = 12;
        pidf.i = 1;
        pidf.f = 1;
        leftMotor.setPIDFCoefficients(DcMotor.RunMode.RUN_USING_ENCODER, pidf);
        rightMotor.setPIDFCoefficients(DcMotor.RunMode.RUN_USING_ENCODER, pidf);
        middleMotor.setPIDFCoefficients(DcMotor.RunMode.RUN_USING_ENCODER, pidf);
    }


    public void loop(){
        angles   = imu.getAngularOrientation().toAxesReference(AxesReference.INTRINSIC).toAxesOrder(AxesOrder.ZYX);
        angleDouble = formatAngle(angles.angleUnit, angles.firstAngle);
        float leftX1 = gamepad1.left_stick_x;
        float leftY1 = gamepad1.left_stick_y;
        float rightX1 = gamepad1.right_stick_x;
        calculator.calculateMovement(leftX1, leftY1, rightX1, Double.parseDouble(angleDouble));
        if(gamepad1.left_trigger == 1) {
            leftMotor.setPower(.4 * calculator.getLeftDrive());
            rightMotor.setPower(.4 * calculator.getRightDrive());
            middleMotor.setPower(.4 * calculator.getMiddleDrive());
        }
        else {
            leftMotor.setPower(.8 * calculator.getLeftDrive());
            rightMotor.setPower(.8 * calculator.getRightDrive());
            middleMotor.setPower(.8 * calculator.getMiddleDrive());
        }

        float leftX2= gamepad2.left_stick_x;
        float leftY2 = gamepad2.left_stick_y;
        float rightX2 = gamepad2.right_stick_x;

        armCalculator.calculateSpeed(leftX2, leftY2, shoulderMotor, elbowMotor);

        if(gamepad2.b) {
            elbowMotor.setPower(.4 * armCalculator.getElbowMotorSpeed());
            shoulderMotor.setPower(.4 * armCalculator.getShoulderMotorSpeed());
        }
        else {
            elbowMotor.setPower(.8 * armCalculator.getElbowMotorSpeed());
            shoulderMotor.setPower(.8 * armCalculator.getShoulderMotorSpeed());
        }
        rotationMotor.setPower(rightX2);




        if(gamepad2.left_bumper){
            leftIntake.setPosition(.05);
        }
        else{
            leftIntake.setPosition(0.5 + (gamepad2.left_trigger/2.3));
        }
        if(gamepad2.right_bumper){
            rightIntake.setPosition(.05);
        }
        else{
            rightIntake.setPosition(0.5 + (gamepad2.right_trigger/2.3));

        }
        if(gamepad2.dpad_up){
            bouncerPos = .58;
        }
        else if(gamepad2.dpad_down){
            bouncerPos = .73;
        }


        bouncer.setPosition(bouncerPos);


        if(gamepad1.y){
            hangMotor.setPower(1);
        } else if (gamepad1.a){
            hangMotor.setPower(-1);
        }else {
            hangMotor.setPower(0);
        }



    }

    String formatAngle(AngleUnit angleUnit, double angle) {
        return formatDegrees(AngleUnit.DEGREES.fromUnit(angleUnit, angle));
    }
    String formatDegrees(double degrees) {
        return String.format(Locale.getDefault(), "%.1f", AngleUnit.DEGREES.normalize(degrees));


    }

}