// MADE BY REICHEN MERIT


//* Copyright (c) 2017 FIRST. All rights reserved.
 /*
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted (subject to the limitations in the disclaimer below) provided that
 * the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice, this list
 * of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice, this
 * list of conditions and the following disclaimer in the documentation and/or
 * other materials provided with the distribution.
 *
 * Neither the name of FIRST nor the names of its contributors may be used to endorse or
 * promote products derived from this software without specific prior written permission.
 *
 * NO EXPRESS OR IMPLIED LICENSES TO ANY PARTY'S PATENT RIGHTS ARE GRANTED BY THIS
 * LICENSE. THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
 * THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

 package org.firstinspires.ftc.teamcode;

 import com.qualcomm.robotcore.eventloop.opmode.Disabled;
import com.qualcomm.robotcore.hardware.CRServo;
 import com.qualcomm.robotcore.hardware.IMU;
 import org.firstinspires.ftc.robotcore.external.navigation.Orientation;
 import org.firstinspires.ftc.robotcore.external.JavaUtil;
 import com.qualcomm.hardware.bosch.BNO055IMU;
 import com.qualcomm.hardware.dfrobot.HuskyLens;
 import com.qualcomm.robotcore.hardware.Servo;
 import com.qualcomm.robotcore.eventloop.opmode.Autonomous;
 import com.qualcomm.robotcore.eventloop.opmode.LinearOpMode;
 import com.qualcomm.robotcore.eventloop.opmode.TeleOp;
 import com.qualcomm.robotcore.hardware.DcMotor;
 import com.qualcomm.robotcore.util.ElapsedTime;
 import com.qualcomm.robotcore.util.Range;
 
 import com.qualcomm.robotcore.eventloop.opmode.Disabled;
 import com.qualcomm.robotcore.eventloop.opmode.LinearOpMode;
 import com.qualcomm.robotcore.eventloop.opmode.TeleOp;
 
 
 import org.firstinspires.ftc.robotcore.external.navigation.DistanceUnit;
 import org.firstinspires.ftc.robotcore.internal.system.Deadline;
 
 import org.firstinspires.ftc.robotcore.external.navigation.AngleUnit;
 import org.firstinspires.ftc.robotcore.external.navigation.AngularVelocity;
 import org.firstinspires.ftc.robotcore.external.navigation.YawPitchRollAngles;
 import org.firstinspires.ftc.robotcore.external.navigation.AxesReference;
 import org.firstinspires.ftc.robotcore.external.navigation.AxesOrder;
 import org.firstinspires.ftc.robotcore.external.navigation.Position;
 import org.firstinspires.ftc.robotcore.external.navigation.Velocity;
 import com.qualcomm.hardware.rev.RevHubOrientationOnRobot;
 
 import java.util.concurrent.TimeUnit;
 
 
 /*
  * This file contains an minimal example of a Linear "OpMode". An OpMode is a 'program' that runs in either
  * the autonomous or the teleop period of an FTC match. The names of OpModes appear on the menu
  * of the FTC Driver Station. When a selection is made from the menu, the corresponding OpMode
  * class is instantiated on the Robot Controller and executed.
  *
  * This particular OpMode just executes a basic Tank Drive Teleop for a two wheeled robot
  * It includes all the skeletal structure that all linear OpModes contain.
  *
  * Use Android Studio to Copy this Class, and Paste it into your team's code folder with a new name.
  * Remove or comment out the @Disabled line to add this OpMode to the Driver Station OpMode list
  */
 @Autonomous(name="ThunderAutoBlueLong", group="Linear OpMode")
 
 public class ThunderAutoBlueLong extends LinearOpMode {
 
     // Declare OpMode members.
     private DcMotor LeftFront;
 
     private DcMotor RightFront;
 
     private DcMotor LeftBack;
 
     private DcMotor RightBack;
 
     private DcMotor LeftArmM;
 
     private DcMotor RightArmM;
 
     private DcMotor Elbow;
 
     private DcMotor Wrist;
 
     private ElapsedTime runtime = new ElapsedTime();
 
     private Servo ServoLeft;
 
     private Servo ServoRight;
     
    private BNO055IMU imu_IMU;
 
     static final double ENCODER_CLICKS = 1120;    // REV 40:1  1120
     static final double DRIVE_GEAR_REDUCTION = 1.0;     // This is < 1.0 if geared UP
     static final double WHEEL_CIRC = 2.0;     // For figuring circumference
     static final double COUNTS_PER_INCH = (ENCODER_CLICKS * DRIVE_GEAR_REDUCTION) /
             (WHEEL_CIRC * 3.1415);
     static final double DRIVE_SPEED = 1.0;
     static final double TURN_SPEED = 0.5;
     static final double speed = 0.5;
     
     double LensPosition = 0;
     
     double Yaw_Angle;
 
 
 //Huskylens variables
     private final int READ_PERIOD = 1;
 
     private HuskyLens huskyLens;
 
     @Override
     public void runOpMode() {
         //IMU init
      // define initialization values for IMU, and then initialize it.
         BNO055IMU.Parameters parameters = new BNO055IMU.Parameters();
         parameters.angleUnit            = BNO055IMU.AngleUnit.DEGREES;
         imu_IMU = hardwareMap.get(BNO055IMU.class, "imu");
         imu_IMU.initialize(parameters);
 
         
         
         telemetry.addData("Status", "Initialized");
 
 //Motor statements
         LeftFront = hardwareMap.get(DcMotor.class, "FrontLeft");
         RightFront = hardwareMap.get(DcMotor.class, "FrontRight");
         LeftBack = hardwareMap.get(DcMotor.class, "BackLeft");
         RightBack = hardwareMap.get(DcMotor.class, "BackRight");
         LeftArmM = hardwareMap.get(DcMotor.class, "LeftArmM");
         RightArmM = hardwareMap.get(DcMotor.class, "RightArmM");
         Elbow = hardwareMap.get(DcMotor.class, "Elbow");
         Wrist = hardwareMap.get(DcMotor.class, "Wrist");
         ServoLeft = hardwareMap.get(Servo.class, "ServoLeft");
         ServoRight = hardwareMap.get(Servo.class, "ServoRight");
        
 
 
         LeftArmM.setMode(DcMotor.RunMode.STOP_AND_RESET_ENCODER);
         RightArmM.setMode(DcMotor.RunMode.STOP_AND_RESET_ENCODER);
         RightFront.setMode(DcMotor.RunMode.STOP_AND_RESET_ENCODER);
         LeftFront.setMode(DcMotor.RunMode.STOP_AND_RESET_ENCODER);
         LeftBack.setMode(DcMotor.RunMode.STOP_AND_RESET_ENCODER);
         RightBack.setMode(DcMotor.RunMode.STOP_AND_RESET_ENCODER);
         Elbow.setMode(DcMotor.RunMode.STOP_AND_RESET_ENCODER);
         Wrist.setMode(DcMotor.RunMode.STOP_AND_RESET_ENCODER);
 
         LeftArmM.setMode(DcMotor.RunMode.RUN_USING_ENCODER);
         RightArmM.setMode(DcMotor.RunMode.RUN_USING_ENCODER);
         RightFront.setMode(DcMotor.RunMode.RUN_USING_ENCODER);
         LeftFront.setMode(DcMotor.RunMode.RUN_USING_ENCODER);
         LeftBack.setMode(DcMotor.RunMode.RUN_USING_ENCODER);
         RightBack.setMode(DcMotor.RunMode.RUN_USING_ENCODER);
         Elbow.setMode(DcMotor.RunMode.RUN_USING_ENCODER);
         Wrist.setMode(DcMotor.RunMode.RUN_USING_ENCODER);
 
         LeftBack.setZeroPowerBehavior(DcMotor.ZeroPowerBehavior.BRAKE);
         RightBack.setZeroPowerBehavior(DcMotor.ZeroPowerBehavior.BRAKE);
         RightFront.setZeroPowerBehavior(DcMotor.ZeroPowerBehavior.BRAKE);
         LeftFront.setZeroPowerBehavior(DcMotor.ZeroPowerBehavior.BRAKE);
         Wrist.setZeroPowerBehavior(DcMotor.ZeroPowerBehavior.BRAKE);
         Elbow.setZeroPowerBehavior(DcMotor.ZeroPowerBehavior.BRAKE);
         LeftArmM.setZeroPowerBehavior(DcMotor.ZeroPowerBehavior.BRAKE);
         RightArmM.setZeroPowerBehavior(DcMotor.ZeroPowerBehavior.BRAKE);
    
     
         
     //huskylens statements
          huskyLens = hardwareMap.get(HuskyLens.class, "huskylens");
          
        
         /*
          * This sample rate limits the reads solely to allow a user time to observe
          * what is happening on the Driver Station telemetry.  Typical applications
          * would not likely rate limit.
          */
         Deadline rateLimit = new Deadline(READ_PERIOD, TimeUnit.SECONDS);
 
         /*
          * Immediately expire so that the first time through we'll do the read.
          */
         rateLimit.expire();
 
         /*
          * Basic check to see if the device is alive and communicating.  This is not
          * technically necessary here as the HuskyLens class does this in its
          * doInitialization() method which is called when the device is pulled out of
          * the hardware map.  However, sometimes it's unclear why a device reports as
          * failing on initialization.  In the case of this device, it's because the
          * call to knock() failed.
          */
         if (!huskyLens.knock()) {
             telemetry.addData(">>", "Problem communicating with " + huskyLens.getDeviceName());
         } else {
             telemetry.addData(">>", "Press start to continue");
         }
 
         /*
          * The device uses the concept of an algorithm to determine what types of
          * objects it will look for and/or what mode it is in.  The algorithm may be
          * selected using the scroll wheel on the device, or via software as shown in
          * the call to selectAlgorithm().
          *
          * The SDK itself does not assume that the user wants a particular algorithm on
          * startup, and hence does not set an algorithm.
          *
          * Users, should, in general, explicitly choose the algorithm they want to use
          * within the OpMode by calling selectAlgorithm() and passing it one of the values
          * found in the enumeration HuskyLens.Algorithm.
          */
         huskyLens.selectAlgorithm(HuskyLens.Algorithm.COLOR_RECOGNITION);
 
         telemetry.update();
 
         // Wait for the game to start (driver presses PLAY)
         waitForStart();
         runtime.reset();
         
         
     
       
      
             
             //left distance, right distance, speed, runtime.
             //Distances are in FT.
             
             //step 1 - Drive forward 2 FT and stop.
              telemetry.addData("POSITION", LensPosition);
             driveBot(2,2,speed,3);
             telemetry.addData("status","first run to position called" );
             telemetry.addData("status", LeftFront.getMode() );
             telemetry.addData("status","left motor,  %7d", LeftFront.getCurrentPosition() );
             telemetry.addData("status","right motor,  %7d", RightFront.getCurrentPosition() );
             telemetry.update();
             RightFront.setMode(DcMotor.RunMode.STOP_AND_RESET_ENCODER);
             LeftFront.setMode(DcMotor.RunMode.STOP_AND_RESET_ENCODER);
             RightBack.setMode(DcMotor.RunMode.STOP_AND_RESET_ENCODER);
             LeftBack.setMode(DcMotor.RunMode.STOP_AND_RESET_ENCODER);
             
             //step 2
         //Conditional for camera: 
         //if seen, proceed to Path 1
         // if not, turn left and check there
             Parse1();
             if ((LensPosition > 0.99) && (LensPosition < 1.01)) {
                 //Path 1
                 Path1();
                 
                 
                 } else {
                 right90();
                 Parse2();
                     if ((LensPosition > 1.99) && (LensPosition < 2.01)) {
                         
                 //Path 2
                 Path2();    
                 } else {
                     
                  //path 3
                  LensPosition = 3;
                     Path3();
                 }
                 
                 
                 
                 }
             
             
         
            
             
             
             
          
      
      
      
      
     }
     
     
     /**
     * Turn left with IMU
     * Left travel distance in degrees  +/-, power to both wheels, timeout in seconds
     */
     
     
     //-------------------------//
 
 
 /**********************************************
      * BEGIN METHODS driveDistance, driveBot
      *********************************************/
     
     /**
     * Drive Distance Method
     * Calculate distance from CM to encoder counts
     */  
     public static double driveDistance(double distance)
     {
         double drive  = (ENCODER_CLICKS/ WHEEL_CIRC);
         int outputClicks= (int)Math.floor(drive * distance);
         return outputClicks;
     }
     // END driveDistance() method
 
 
 public void driveBot(double distanceInFEETleft, double distanceInFEETright, double power, double timeoutS) 
     {
         telemetry.addData("status","encoder reset");
         telemetry.update();
         
         int rightTarget;
         int leftTarget;
 
         if(opModeIsActive()) 
         {
             
             telemetry.update();
             
             rightTarget = (int) driveDistance(distanceInFEETright);
             leftTarget = (int) -driveDistance(distanceInFEETleft);
 
             RightFront.setTargetPosition(rightTarget);
             LeftFront.setTargetPosition(leftTarget);
             RightBack.setTargetPosition(rightTarget);
             LeftBack.setTargetPosition(leftTarget);
 
             RightFront.setMode(DcMotor.RunMode.RUN_TO_POSITION);
             LeftFront.setMode(DcMotor.RunMode.RUN_TO_POSITION);
             RightBack.setMode(DcMotor.RunMode.RUN_TO_POSITION);
             LeftBack.setMode(DcMotor.RunMode.RUN_TO_POSITION);
             
             runtime.reset();
 
             RightFront.setPower(power);
             LeftFront.setPower(power);
             RightBack.setPower(power);
             LeftBack.setPower(power);
         
             while (opModeIsActive() &&
                 (runtime.seconds() < timeoutS) &&
                 (LeftFront.isBusy() && RightFront.isBusy() ))
                 {
                    telemetry.addData("POSITION", LensPosition);
                     telemetry.update();
                 }
             LeftFront.setPower(0);
             RightFront.setPower(0);
             LeftBack.setPower(0);
             RightBack.setPower(0);
             LeftFront.setMode(DcMotor.RunMode.RUN_USING_ENCODER);
             RightFront.setMode(DcMotor.RunMode.RUN_USING_ENCODER);
             LeftBack.setMode(DcMotor.RunMode.RUN_USING_ENCODER);
             RightBack.setMode(DcMotor.RunMode.RUN_USING_ENCODER);
             
              RightFront.setMode(DcMotor.RunMode.STOP_AND_RESET_ENCODER);
             LeftFront.setMode(DcMotor.RunMode.STOP_AND_RESET_ENCODER);
             RightBack.setMode(DcMotor.RunMode.STOP_AND_RESET_ENCODER);
             LeftBack.setMode(DcMotor.RunMode.STOP_AND_RESET_ENCODER);
         }   
     }
     // END driveBot method
    
             
       
       
       //Turn left 90
       public void left90() {
           driveBot(-1.5,1.5,speed,3);
             telemetry.addData("status","Check position 2 for object" );
             telemetry.addData("status", LeftFront.getMode() );
             telemetry.addData("status","left motor,  %7d", LeftFront.getCurrentPosition() );
             telemetry.addData("status","right motor,  %7d", RightFront.getCurrentPosition() );
             telemetry.update();
             RightFront.setMode(DcMotor.RunMode.STOP_AND_RESET_ENCODER);
             LeftFront.setMode(DcMotor.RunMode.STOP_AND_RESET_ENCODER);
             RightBack.setMode(DcMotor.RunMode.STOP_AND_RESET_ENCODER);
             LeftBack.setMode(DcMotor.RunMode.STOP_AND_RESET_ENCODER);
           
       }
       
       
       //Turn Right 90
       public void right90() {
           driveBot(1.5,-1.5,speed,3);
             telemetry.addData("status","Check position 2 for object" );
             telemetry.addData("status", LeftFront.getMode() );
             telemetry.addData("status","left motor,  %7d", LeftFront.getCurrentPosition() );
             telemetry.addData("status","right motor,  %7d", RightFront.getCurrentPosition() );
             
       }
       
       //Turn Right 180
       public void right180() {
           driveBot(3,-3,speed,5);
             telemetry.addData("status","Check position 2 for object" );
             telemetry.addData("status", LeftFront.getMode() );
             telemetry.addData("status","left motor,  %7d", LeftFront.getCurrentPosition() );
             telemetry.addData("status","right motor,  %7d", RightFront.getCurrentPosition() );
             
       }
       // Center position
       public void Path1() {
           driveBot(1.5, 1.5, 0.3, 5);
           right180();
           driveBot(0.35, 0.35, 0.3, 2);
           driveBot(-0.45, -0.45, 0.3, 2);
           sleep(200);
           //turn towards the stage door
           right90();
           //drive through the stage door
           driveBot(6, 6, 0.3, 8);
           
           sleep(30000);
       }
       //right Position
        public void Path2() {
             driveBot(0.35, 0.35, 0.3, 2);
           driveBot(-0.45, -0.45, 0.3, 2);
           sleep(200);
           //turn to align with the stage door
           left90();
           //drive to the stage door
           driveBot(1.5, 2, 0.3, 5);
           //drive through the stage door
           left90();
           driveBot(6, 6, 0.3, 8);
           
           sleep(30000);
       }
       //Left position
        public void Path3() {
           right180();
           driveBot(0.35, 0.35, 0.3, 2);
           driveBot(-0.45, -0.45, 0.3, 2);
           sleep(200);
           //turn to align with the stage door
           right90();
           //drive to the stage door
           driveBot(1.5, 1.5, 0.3, 5);
           //drive through the stage door
           left90();
           driveBot(6, 6, 0.3, 8);
         
         
         sleep(30000);
       }
       //check for 1
        public void Parse1() {
            
             HuskyLens.Block[] blocks = huskyLens.blocks();
             telemetry.addData("Block count", blocks.length);
             for (int i = 0; i < blocks.length; i++) {
                 telemetry.addData("Block", blocks[i].toString());
             } 
            
             telemetry.update();
                   telemetry.addData("Block count", blocks.length);
             for (int i = 0; i < blocks.length; i++) {
                 telemetry.addData("Block", blocks[i].toString());
             } 
             if (blocks.length == 1) {
                 telemetry.addData("IT WORKS", "");
                 LensPosition = 1;
                  telemetry.addData("POSITION", LensPosition);
             
                   telemetry.update();
             } 
          
         
       }
       //check for 2
        public void Parse2() {
          
          HuskyLens.Block[] blocks = huskyLens.blocks();
             telemetry.addData("Block count", blocks.length);
             for (int i = 0; i < blocks.length; i++) {
                 telemetry.addData("Block", blocks[i].toString());
             } 
            
             telemetry.update();
                   telemetry.addData("Block count", blocks.length);
             for (int i = 0; i < blocks.length; i++) {
                 telemetry.addData("Block", blocks[i].toString());
             } 
             if (blocks.length == 1) {
                 telemetry.addData("IT WORKS", "");
                 LensPosition = 2;
                  telemetry.addData("POSITION", LensPosition);
             
                   telemetry.update();
             } 
         
       }
       
       //Operational methods
  
  public void RightServoDrop() {
      //open
       ServoRight.setPosition(0);
      sleep(500);
      //close
      ServoRight.setPosition(1);
      
  }        
  
  public void LeftServoDrop() {
      //open
       ServoLeft.setPosition(0);
      sleep(500);
      //close
      ServoLeft.setPosition(1);
      
  }    
  
  //set wrist in transit/drop position
  public void WristTransit() {
   Elbow.setTargetPosition(1750);
   Elbow.setMode(DcMotor.RunMode.RUN_TO_POSITION);
   Elbow.setPower(0.5);
   sleep(1000);
   Elbow.setPower(0);
   
  }
  
  //set arm in drop position
  public void ArmDrop() {
   RightArmM.setTargetPosition(1750);
   LeftArmM.setTargetPosition(1750);
   RightArmM.setMode(DcMotor.RunMode.RUN_TO_POSITION);
   LeftArmM.setMode(DcMotor.RunMode.RUN_TO_POSITION);
   RightArmM.setPower(0.5);
   LeftArmM.setPower(0.5);
   sleep(2000);
   RightArmM.setPower(0);
   LeftArmM.setPower(0);
   
  }
  
  //Zero the arm and wrist
  public void ArmZero() {
   RightArmM.setTargetPosition(0);
   LeftArmM.setTargetPosition(0);
    Elbow.setTargetPosition(0);
   RightArmM.setMode(DcMotor.RunMode.RUN_TO_POSITION);
   LeftArmM.setMode(DcMotor.RunMode.RUN_TO_POSITION);
   Elbow.setMode(DcMotor.RunMode.RUN_TO_POSITION);
   RightArmM.setPower(0.5);
   LeftArmM.setPower(0.5);
   Elbow.setPower(0.5);
   sleep(2000);
   RightArmM.setPower(0);
   LeftArmM.setPower(0);
   Elbow.setPower(0);
   
  }
  
  
  //Operational methods end
 }