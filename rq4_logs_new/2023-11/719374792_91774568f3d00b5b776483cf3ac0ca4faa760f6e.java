package org.firstinspires.ftc.teamcode.downpour;


import com.qualcomm.robotcore.eventloop.opmode.LinearOpMode;
import com.qualcomm.hardware.dfrobot.HuskyLens;
import org.firstinspires.ftc.robotcore.external.JavaUtil;
import org.firstinspires.ftc.robotcore.external.navigation.YawPitchRollAngles;
import org.firstinspires.ftc.robotcore.external.navigation.AngularVelocity;
import com.qualcomm.robotcore.hardware.IMU;
import org.firstinspires.ftc.robotcore.external.Telemetry;
import com.qualcomm.robotcore.hardware.Servo;
import com.qualcomm.robotcore.util.ElapsedTime;
import com.qualcomm.robotcore.eventloop.opmode.TeleOp;
import com.qualcomm.robotcore.hardware.DcMotor;
import com.qualcomm.robotcore.hardware.DcMotorSimple;
import com.qualcomm.robotcore.hardware.DigitalChannel;
import com.qualcomm.robotcore.util.Range;

import org.firstinspires.ftc.robotcore.external.navigation.Acceleration;
import org.firstinspires.ftc.robotcore.external.navigation.AngleUnit;
import org.firstinspires.ftc.robotcore.external.navigation.AxesOrder;
import org.firstinspires.ftc.robotcore.external.navigation.AxesReference;
import org.firstinspires.ftc.robotcore.external.navigation.Orientation;
import org.firstinspires.ftc.robotcore.external.navigation.Position;
import org.firstinspires.ftc.robotcore.external.navigation.Velocity;
import com.qualcomm.hardware.rev.RevHubOrientationOnRobot;


@TeleOp(name = "DownpourTeleopOverride")
public class DownpourTeleopOverride extends LinearOpMode {

    private DcMotor FrontLeft;

    private DcMotor FrontRight;

    private DcMotor BackLeft;

    private DcMotor BackRight;

    private DcMotor LeftArmM;

    private DcMotor RightArmM;

    private DcMotor Elbow;

    private DcMotor Wrist;

    private ElapsedTime runtime = new ElapsedTime();

    private Servo ServoLeft;

    private Servo ServoRight;
    
    private Servo Shooter;

    private IMU imu;



    static final double COUNTS_PER_MOTOR_REV = 1120;    // REV 40:1  1120
    static final double DRIVE_GEAR_REDUCTION = 1.0;     // This is < 1.0 if geared UP
    static final double WHEEL_DIAMETER_INCHES = 2;     // For figuring circumference
    static final double COUNTS_PER_INCH = (COUNTS_PER_MOTOR_REV * DRIVE_GEAR_REDUCTION) /
            (WHEEL_DIAMETER_INCHES * 3.1415);
    static final double DRIVE_SPEED = 1.0;
    static final double TURN_SPEED = 0.5;
    double Drivespeedmult = 1;
    
    //speed for all superstructure motors
    static final double ARMSPEED = 1;
    
    
    //Soft limits
     //Soft limits
    static final int ArmLimit = -4000;
    static final int JointLimit = 2150;

//Integer calls for setpoint
                    int ArmTarget = 0;
                    int JointTarget = 0;
    /**
     * This function is executed when this Op Mode is selected from the Driver Station.
     */
    @Override
    public void runOpMode() {

        FrontLeft = hardwareMap.get(DcMotor.class, "FrontLeft");
        FrontRight = hardwareMap.get(DcMotor.class, "FrontRight");
        BackLeft = hardwareMap.get(DcMotor.class, "BackLeft");
        BackRight = hardwareMap.get(DcMotor.class, "BackRight");
        LeftArmM = hardwareMap.get(DcMotor.class, "LeftArmM");
        RightArmM = hardwareMap.get(DcMotor.class, "RightArmM");
        Elbow = hardwareMap.get(DcMotor.class, "Elbow");
        Wrist = hardwareMap.get(DcMotor.class, "Wrist");
        ServoLeft = hardwareMap.get(Servo.class, "ServoLeft");
        ServoRight = hardwareMap.get(Servo.class, "ServoRight");
        Shooter = hardwareMap.get(Servo.class, "Shooter");


        LeftArmM.setMode(DcMotor.RunMode.STOP_AND_RESET_ENCODER);
        RightArmM.setMode(DcMotor.RunMode.STOP_AND_RESET_ENCODER);
        FrontRight.setMode(DcMotor.RunMode.STOP_AND_RESET_ENCODER);
        FrontLeft.setMode(DcMotor.RunMode.STOP_AND_RESET_ENCODER);
        BackLeft.setMode(DcMotor.RunMode.STOP_AND_RESET_ENCODER);
        BackRight.setMode(DcMotor.RunMode.STOP_AND_RESET_ENCODER);
        Elbow.setMode(DcMotor.RunMode.STOP_AND_RESET_ENCODER);
        Wrist.setMode(DcMotor.RunMode.STOP_AND_RESET_ENCODER);

        LeftArmM.setMode(DcMotor.RunMode.RUN_USING_ENCODER);
        RightArmM.setMode(DcMotor.RunMode.RUN_USING_ENCODER);
        FrontRight.setMode(DcMotor.RunMode.RUN_USING_ENCODER);
        FrontLeft.setMode(DcMotor.RunMode.RUN_USING_ENCODER);
        BackLeft.setMode(DcMotor.RunMode.RUN_USING_ENCODER);
        BackRight.setMode(DcMotor.RunMode.RUN_USING_ENCODER);
        Elbow.setMode(DcMotor.RunMode.RUN_USING_ENCODER);
        Wrist.setMode(DcMotor.RunMode.RUN_USING_ENCODER);

        BackLeft.setZeroPowerBehavior(DcMotor.ZeroPowerBehavior.BRAKE);
        BackRight.setZeroPowerBehavior(DcMotor.ZeroPowerBehavior.BRAKE);
        FrontRight.setZeroPowerBehavior(DcMotor.ZeroPowerBehavior.BRAKE);
        FrontLeft.setZeroPowerBehavior(DcMotor.ZeroPowerBehavior.BRAKE);
        Wrist.setZeroPowerBehavior(DcMotor.ZeroPowerBehavior.BRAKE);
        Elbow.setZeroPowerBehavior(DcMotor.ZeroPowerBehavior.BRAKE);
        LeftArmM.setZeroPowerBehavior(DcMotor.ZeroPowerBehavior.BRAKE);
        RightArmM.setZeroPowerBehavior(DcMotor.ZeroPowerBehavior.BRAKE);


        /*
        FrontRight.setMode(DcMotor.RunMode.RUN_TO_POSITION);
        FrontLeft.setMode(DcMotor.RunMode.RUN_TO_POSITION);
        BackRight.setMode(DcMotor.RunMode.RUN_TO_POSITION);
        BackLeft.setMode(DcMotor.RunMode.RUN_TO_POSITION);
        
        */
        LeftArmM.setMode(DcMotor.RunMode.RUN_TO_POSITION);
        RightArmM.setMode(DcMotor.RunMode.RUN_TO_POSITION);
        Elbow.setMode(DcMotor.RunMode.RUN_TO_POSITION);

         


        // put initialization code here
        FrontRight.setDirection(DcMotorSimple.Direction.REVERSE);
        BackRight.setDirection(DcMotorSimple.Direction.REVERSE);
        RightArmM.setDirection(DcMotorSimple.Direction.FORWARD);
        
                imu = hardwareMap.get(IMU.class, "imu");
imu.initialize(new IMU.Parameters(new RevHubOrientationOnRobot(RevHubOrientationOnRobot.LogoFacingDirection.UP, RevHubOrientationOnRobot.UsbFacingDirection.LEFT)));

        
    
    

        //wait for start button to be pressed
        waitForStart();
        
        YawPitchRollAngles orientation;
          orientation = imu.getRobotYawPitchRollAngles();
          
          imu.resetYaw();

        if (opModeIsActive()) {
        }

            /*private void drive(int leftTarget, int rightTarget, double speed) {
                LeftPos += leftTarget;
                RightPos += rightTarget;

                FrontLeft.setTargetPosition(LeftPos);
                FrontRight.setTargetPosition(RightPos);
                BackLeft.setTargetPosition(LeftPos);
                BackRight.setTargetPosition(RightPos);

                FrontRight.setMode(DcMotor.RunMode.RUN_TO_POSITION);
                FrontLeft.setMode(DcMotor.RunMode.RUN_TO_POSITION);
                BackRight.setMode(DcMotor.RunMode.RUN_TO_POSITION);
                BackLeft.setMode(DcMotor.RunMode.RUN_TO_POSITION);

                FrontLeft.setPower(speed);
                FrontRight.setPower(speed);
                BackRight.setPower(speed);
                BackLeft.setPower(speed);

                while (opModeIsActive() && FrontRight.isBusy() && FrontLeft.isBusy() && BackLeft.isBusy() && BackRight.isBusy()) {
                    idle();
                   /*
             */

            //put run blocks here
            while (opModeIsActive()) {
                
                
                //Drivetrain
                    double x = -gamepad1.left_stick_x * Drivespeedmult; //forward
                    double y = -gamepad1.left_stick_y  * Drivespeedmult; //strafe
                    double t = -gamepad1.right_stick_x  * Drivespeedmult; //turn
                    double inout = gamepad2.right_stick_y;
                    
                    
                    
                    //IMU
                    orientation = imu.getRobotYawPitchRollAngles();
                  
                   double angle = orientation.getYaw(AngleUnit.RADIANS) + 1.5; //angle offset
                    
                    double x_rotated =x * Math.cos(angle) - y * Math.sin(angle);
                    double y_rotated =x * Math.sin(angle) + y * Math.cos(angle);
                    
                     telemetry.addData("Yaw (Adjusted)", angle);
                  
                   //drive speed slowdown button 
                if(gamepad1.right_bumper) {
                    Drivespeedmult = 0.33;
                } else {
                    Drivespeedmult = 1;
                }
                  

                        FrontLeft.setPower((x_rotated + y_rotated +t));
                        BackLeft.setPower((x_rotated - y_rotated +t));
                        FrontRight.setPower((x_rotated - y_rotated) -t );
                        BackRight.setPower((x_rotated + y_rotated -t));
                        
                        //arm control
                        


                        LeftArmM.setTargetPosition(ArmTarget);
                        RightArmM.setTargetPosition(ArmTarget);
                        Elbow.setTargetPosition(JointTarget);
                    
                        LeftArmM.setMode(DcMotor.RunMode.RUN_TO_POSITION);
                        RightArmM.setMode(DcMotor.RunMode.RUN_TO_POSITION);
                        Elbow.setMode(DcMotor.RunMode.RUN_TO_POSITION);
                        
                         LeftArmM.setPower(ARMSPEED);
                         RightArmM.setPower(ARMSPEED);
                        Elbow.setPower(ARMSPEED);

                        
                        double ArmInputraw = gamepad2.left_stick_y * 50;
                        double JointInputraw = gamepad2.right_stick_y * 50;
                        
                        int ArmInput = (int) Math.floor(ArmInputraw);
                        int JointInput = (int) Math.floor(JointInputraw);
                        
                        ArmTarget = ArmTarget + (ArmInput);
                        JointTarget = JointTarget + (JointInput);
                       
  
                        
                        //Arm control/limit
                if (ArmTarget < ArmLimit) {
                   ArmTarget = ArmTarget;
                } else if(ArmTarget > 0) {
                    ArmTarget = ArmTarget;
                } else {
                    ArmTarget = ArmTarget;
                }
                
                //Joint Control/limit
                if (JointTarget > JointLimit) {
                    JointTarget = JointTarget;
                } else if(JointTarget < 0) {
                   JointTarget = JointTarget;
                } else {
                    JointTarget = JointTarget;
                }
                
                
                    
                
                    //left servo up
                    if (gamepad2.left_bumper) {
                        ServoLeft.setPosition(1);
                        ServoLeft.getPosition();

                    }
                    //left servo down
                    if (gamepad2.left_trigger >0) {
                        ServoLeft.setPosition(0);
                        ServoLeft.getPosition();
                    }


                   //right servo up
                   if(gamepad2.right_bumper) {
                       ServoRight.setPosition(0);
                       ServoRight.getPosition();
                   }
                   //right servo down
                   if (gamepad2.right_trigger > 0){
                        ServoRight.setPosition(1);
                        ServoRight.getPosition();
                    }
                    
                    //Plane reset
                    if (gamepad1.x) {
                        Shooter.setPosition(0);
                    }
                    
                    // Check to see if reset yaw is requested.
                     if (gamepad1.y) {
                    imu.resetYaw();
                      }
                        
                    
                    
                    //shooter shooter
                    if (gamepad1.a) {
                        Shooter.setPosition(0.2);
                    }
                    
                    
                    
                    //telemetry
                       telemetry.update();
                         telemetry.addData("RightArmM Encoder", RightArmM.getCurrentPosition()); 
                        telemetry.addData("Elbow Encoder", Elbow.getCurrentPosition());
                      
                 //preset - zero
                    if (gamepad2.back) {
                  ArmTarget = 0;
                  JointTarget = 0;
                   ServoRight.setPosition(1);
                   ServoLeft.setPosition(0);
                }
                
                //preset - high
                    if (gamepad2.y) {
                  ArmTarget = -1600;
                  JointTarget = 1600;
                   ServoRight.setPosition(1);
                   ServoLeft.setPosition(0);
                }
                
                //preset - pickup
                    if (gamepad2.a) {
                  ArmTarget = -600;
                  JointTarget = 2000;
                   ServoRight.setPosition(1);
                   ServoLeft.setPosition(0);
                }
              
              //preset - trnsit
                    if (gamepad2.b) {
                  ArmTarget = -250;
                  JointTarget = 1000;
                }



                     }
                     
            }
}
               



               
                   

                
                
    