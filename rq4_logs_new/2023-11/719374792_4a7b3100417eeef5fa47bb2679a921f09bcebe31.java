package org.firstinspires.ftc.teamcode.downpour;


import com.qualcomm.robotcore.eventloop.opmode.LinearOpMode;
import com.qualcomm.robotcore.hardware.HardwareMap;
import com.qualcomm.hardware.dfrobot.HuskyLens;
import com.qualcomm.robotcore.hardware.DcMotor;
import com.qualcomm.robotcore.hardware.Servo;
import com.qualcomm.robotcore.util.ElapsedTime;
import com.qualcomm.robotcore.eventloop.opmode.TeleOp;
import com.qualcomm.robotcore.hardware.DcMotorSimple;
import com.qualcomm.robotcore.hardware.DcMotor;
import com.qualcomm.robotcore.hardware.DigitalChannel;
import com.qualcomm.robotcore.util.Range;
import com.qualcomm.robotcore.hardware.CRServo;


@TeleOp(name = "DownpourTest")
public class DownpourTest extends LinearOpMode {

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

    static final double COUNTS_PER_MOTOR_REV = 1120;    // REV 40:1  1120
    static final double DRIVE_GEAR_REDUCTION = 1.0;     // This is < 1.0 if geared UP
    static final double WHEEL_DIAMETER_INCHES = 2.0;     // For figuring circumference
    static final double COUNTS_PER_INCH = (COUNTS_PER_MOTOR_REV * DRIVE_GEAR_REDUCTION) /
            (WHEEL_DIAMETER_INCHES * 3.1415);
    static final double DRIVE_SPEED = 1.0;
    static final double TURN_SPEED = 0.5;
    static final double speed = 0.5;
    
    //Huskylens/IMU definitions
        private final int READ_PERIOD = 1;

    private HuskyLens huskyLens;


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
        LeftArmM.setMode(DcMotor.RunMode.RUN_TO_POSITION);
        RightArmM.setMode(DcMotor.RunMode.RUN_TO_POSITION);
        Elbow.setMode(DcMotor.RunMode.RUN_TO_POSITION);
        Wrist.setMode(DcMotor.RunMode.RUN_TO_POSITION);

         */


        // put initialization code here
        FrontRight.setDirection(DcMotorSimple.Direction.REVERSE);
        BackRight.setDirection(DcMotorSimple.Direction.REVERSE);
        LeftArmM.setDirection(DcMotorSimple.Direction.FORWARD);
        
    
    

        //wait for start button to be pressed
        waitForStart();

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
                
                
                
                double forward = gamepad1.left_stick_y;
                double strafe = -gamepad1.left_stick_x;
                double turn = -gamepad1.right_stick_x;
                double inout = -gamepad2.right_stick_y;


                double denominator = Math.max(Math.abs(forward) + Math.abs(strafe) + Math.abs(turn), 2);

                FrontRight.setPower((forward - strafe - turn) / denominator);
                FrontLeft.setPower((forward + strafe + turn) / denominator);
                BackLeft.setPower((forward - strafe + turn) / denominator);
                BackRight.setPower((forward + strafe - turn) / denominator);

                LeftArmM.setPower(gamepad2.right_stick_y);
                RightArmM.setPower(gamepad2.right_stick_y);

               if (gamepad2.left_stick_y >0) {
                   Elbow.setPower(.5);
               } else if (gamepad2.left_stick_y <0) {
                   Elbow.setPower(-.5);
               }
               else Elbow.setPower(0);


                if (gamepad2.dpad_right) {
                    Wrist.setPower(1);
                } else if (gamepad2.dpad_down) {
                    Wrist.setPower(-1);
                } else {
                    Wrist.setPower(0);

                if (gamepad2.y) {
                    preset(0.7, 5.0);
                    
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
                    
                    //Plane shooter
                    if (gamepad2.x) {
                        Shooter.setPosition(0);
                        
                    }
                    
                    
                    
                    //shooter reset
                    if (gamepad2.b) {
                        Shooter.setPosition(0.2);
                    }
                    
                    
                    
                    //telemetry
                       telemetry.update();
                         telemetry.addData("RightArmM Encoder", RightArmM.getCurrentPosition()); 
                        telemetry.addData("Elbow Encoder", Elbow.getCurrentPosition());







                     }
                    }
                }
                
                public void preset(double speed, double timeoutS) {
                    int newArmTarget;
                    int newWristTarget;
                    
                    if (opModeIsActive()) {
                        
                        //determine new target position
                        newArmTarget = -350;
                        newWristTarget = 2050;
                        
                        RightArmM.setTargetPosition(newArmTarget);
                        LeftArmM.setTargetPosition(newArmTarget);
                        Elbow.setTargetPosition(newWristTarget);
                    
                        RightArmM.setMode(DcMotor.RunMode.RUN_TO_POSITION);
                        LeftArmM.setMode(DcMotor.RunMode.RUN_TO_POSITION);
                        Elbow.setMode(DcMotor.RunMode.RUN_TO_POSITION);
                        
                        //reset runtime and start
                        runtime.reset();
                        
                        //keep looping while opmode is active
                        //isBusy && isbusy means when one motor hits its position, all motors will stop.
                        //If you require that all motors reach their position, use isbusy || isbusy.
                        while (opModeIsActive() && (runtime.seconds() < timeoutS) && ((RightArmM.isBusy() || (LeftArmM.isBusy() ||Elbow.isBusy())))) {
                            
                            //Setting Power to its respective speed variable
                            RightArmM.setPower(speed);
                            LeftArmM.setPower(speed);
                            Elbow.setPower(speed);
                            
                        }
                        
                        // stop all motion
                            RightArmM.setPower(0);
                            LeftArmM.setPower(0);
                            Elbow.setPower(0);
                            
                        // Turn off RUN_TO_POSITION
                        
                         RightArmM.setMode(DcMotor.RunMode.RUN_USING_ENCODER);
                        LeftArmM.setMode(DcMotor.RunMode.RUN_USING_ENCODER);
                        Elbow.setMode(DcMotor.RunMode.RUN_USING_ENCODER);
                    }
                    return;
                    
                    
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
                
                
}