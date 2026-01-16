package org.firstinspires.ftc.teamcode;

import com.qualcomm.hardware.bosch.BNO055IMU;
import com.qualcomm.robotcore.eventloop.opmode.OpMode;
import com.qualcomm.robotcore.eventloop.opmode.TeleOp;
import com.qualcomm.robotcore.hardware.DcMotor;
import com.qualcomm.robotcore.hardware.IntegratingGyroscope;
import com.qualcomm.robotcore.hardware.Servo;
import com.qualcomm.robotcore.hardware.ColorSensor;

/**
 * Created by guus on 9/23/2017.
 * FTC 2017, FTCUnits
 * Basis motor controller, nog niet af
 */

@TeleOp(name = "DriverOp2", group = "Guus")

public class DriverOp2 extends OpMode {


    private DcMotor MotorFrontRight;
    private DcMotor MotorFrontLeft;
    private DcMotor MotorBackRight;
    private DcMotor MotorBackLeft;
    private DcMotor ArmMotor;
    private DcMotor BakMotor;
    public  boolean IsControlled = true;
    private Servo BekServo1;
    private Servo BekServo2;
    double driveDirectionSpeed = 1 ;
    private ColorSensor testSensor;
    private float x;
    private float y;
    BNO055IMU imu;
    private double ServoPosition = 0.4;
    private double ClosedPos = 0.35 ;
    private double OpenPos=0.55;
    private String servomessage;


    @Override
    public void init() {
        MotorBackLeft   = hardwareMap.dcMotor.get("MotorBackLeft");
        MotorBackRight  = hardwareMap.dcMotor.get("MotorBackRight");
        MotorFrontLeft  = hardwareMap.dcMotor.get("MotorFrontLeft");
        MotorFrontRight = hardwareMap.dcMotor.get("MotorFrontRight");
        ArmMotor = hardwareMap.dcMotor.get("ArmMotor");
        BekServo1 = hardwareMap.servo.get("BekServo1");
        BekServo2 = hardwareMap.servo.get("BekServo2");
        BakMotor = hardwareMap.dcMotor.get("BakMotor");
        testSensor = hardwareMap.colorSensor.get("testSensor");
        driveDirectionSpeed  = 1;
        BekServo2.setDirection(Servo.Direction.REVERSE);
        BekServo1.setPosition(1);
        BekServo2.setPosition(1);


    }

    @Override
    public void loop() {

        SpeedChecks();
        DriveChecks();

        ArmChecks();
        telemetry.addData("color",testSensor.alpha());


    }


    void  DriveChecks () {

        MotorBackLeft.setPower(-1 * driveDirectionSpeed * gamepad1.left_stick_y);
        MotorFrontLeft.setPower(-1 * driveDirectionSpeed * gamepad1.left_stick_y);
        MotorBackRight.setPower(1 * driveDirectionSpeed * gamepad1.right_stick_y);
        MotorFrontRight.setPower(1 * driveDirectionSpeed * gamepad1.right_stick_y);
    }

    void ArmChecks() {
        if(gamepad2.a)
            ArmMotor.setPower(-0.4);

        if (gamepad2.y)
            ArmMotor.setPower(0.4);
        if(gamepad2.x)
            ArmMotor.setPower(0);
    }

    void SpeedChecks() {



        if (gamepad1.back) {
            double temp;
            temp = driveDirectionSpeed;
            driveDirectionSpeed = temp * -1;
        }



        if(gamepad1.left_bumper){
            if(x != -1)
                x -= 0.05;
        }
        if (gamepad1.right_bumper){
            if(x != 1)
                x += 0.05;
        }

        y = x*x*x;
        if(x < 0){
            y = y * -1;
        }

        driveDirectionSpeed = y;
        BakMotor.setPower(gamepad2.right_stick_y * -0.25);


        
       if (gamepad2.left_bumper)
           ServoPosition = ClosedPos;
        if (gamepad2.right_bumper)
            ServoPosition = OpenPos;
        
        BekServo1.setPosition(ServoPosition - 0.1);
        BekServo2.setPosition(ServoPosition+0.25);
        if (gamepad1.dpad_left) {
            sidemoving(-1);
        }
        if (gamepad1.dpad_right) {
            sidemoving(1);
        }

    }

        public void sidemoving(int speed) {
            MotorBackLeft.setPower(speed);
            MotorFrontLeft.setPower(-speed);
            MotorBackRight.setPower(-speed);
            MotorFrontRight.setPower(speed);
        }
    }





