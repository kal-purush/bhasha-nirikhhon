package RobotCode;

import com.qualcomm.robotcore.eventloop.opmode.Disabled;
import com.qualcomm.robotcore.hardware.CRServo;
import com.qualcomm.robotcore.hardware.DcMotorEx;
import com.qualcomm.robotcore.eventloop.opmode.Autonomous;
import com.qualcomm.robotcore.eventloop.opmode.LinearOpMode;
import com.qualcomm.robotcore.eventloop.opmode.TeleOp;
import com.qualcomm.robotcore.hardware.DcMotor;
import com.qualcomm.robotcore.util.ElapsedTime;



@Autonomous(name="AutonomousTeam2", group="Linear Opmode")


public class AutonomousTeam2 extends LinearOpMode {

    // Declare OpMode members for each of the 4 motors.
    private ElapsedTime runtime = new ElapsedTime();
    private DcMotorEx leftFrontDrive = null;
    private DcMotorEx leftBackDrive = null;
    private DcMotorEx rightFrontDrive = null;
    private DcMotorEx rightBackDrive = null;
    private DcMotorEx slideArm = null;
    private DcMotorEx intakePivotMotor = null;
    private CRServo upperServo = null;
    private CRServo lowerServo = null;

    // Math for wheel movement
    private final double wheelCircumference = 75*3.14;
    private final double gearReduction = 3.61*5.23;
    private final double counts = 28.0;
    
    private final double rev = counts*gearReduction;
    private final int revPerMM = (int)rev/(int)wheelCircumference;
    private final double inches = revPerMM*25.4;

    // Declare OpMode members for each of the moving parts.

    @Override
    public void runOpMode() {
        
        // Initialize the hardware variables. Note that the strings used here must correspond
        // to the names assigned during the robot configuration step on the DS or RC devices.

        leftFrontDrive  = hardwareMap.get(DcMotorEx.class, "fLeft");
        leftBackDrive  = hardwareMap.get(DcMotorEx.class, "bLeft");
        rightFrontDrive = hardwareMap.get(DcMotorEx.class, "fRight");
        rightBackDrive = hardwareMap.get(DcMotorEx.class, "bRight");
        slideArm = hardwareMap.get(DcMotorEx.class, "slideMotor");
        upperServo = hardwareMap.get(CRServo.class, "topIntake");
        lowerServo = hardwareMap.get(CRServo.class, "bottomIntake");
        intakePivotMotor = hardwareMap.get(DcMotorEx.class, "intakePivotMotor");

        // Set Directions
        
        leftFrontDrive.setDirection(DcMotor.Direction.REVERSE);
        leftBackDrive.setDirection(DcMotor.Direction.REVERSE);
        rightFrontDrive.setDirection(DcMotor.Direction.REVERSE);
        
        slideArm.setMode(DcMotor.RunMode.STOP_AND_RESET_ENCODER);
        slideArm.setMode(DcMotor.RunMode.RUN_WITHOUT_ENCODERS);

        // Wait for the game to start (driver presses PLAY)
        telemetry.addData("Status", "Initialized");
        telemetry.update();
        int count=0;
        waitForStart();
        runtime.reset();
        // run until the end of the match (driver presses STOP)
        while (opModeIsActive()&&count==0) {
            // Gallop on Rocinante!!!
            observation();
            count++;
        }
    }
    public void input(double leftFront, double rightFront, double leftBack, double rightBack)
    {
        leftFrontDrive.setPower(leftFront);
        rightFrontDrive.setPower(rightFront);
        leftBackDrive.setPower(leftBack);
        rightBackDrive.setPower(rightBack);
        sleep(500);
    }
    public void turnLeft(int angle)
    {
        int convert=revPerMM*angle*(38/10)+(angle*12/10);
        encoders(-convert, convert, -convert, convert);
    }
    public void turnRight(int angle)
    {
        int convert=revPerMM*angle*(38/10)+(angle*12/10);
        encoders(convert, -convert, convert, -convert);
    }
    public void driveEncoders(int target)
    {
        encoders(target, target, target, target);
    }
    public void backEncoders(int target)
    {
        encoders(-target, -target, -target, -target);
    }
    public void rightEncoders(int target)
    {
        encoders(target, -target, -target, target);
    }
    public void leftEncoders(int target)
    {
        encoders(-target, target, target, -target);
    }
    public void leftTopDiagonal(int target)
    {
        encoders(0, target, target, 0);
    }
    public void rightTopDiagonal(int target)
    {
        encoders(target, 0, 0, target);
    }
    public void leftBottomDiagonal(int target)
    {
        encoders(-target, 0, 0, -target);
    }
    public void rightBottomDiagonal(int target)
    {
        encoders(0, -target, -target, 0);
    }

    // Encoders is a function that makes the four wheels move in a direction by the distance inputted. (100, 100, 0, 0) would make lF and rF go 100mm forwards.
    // The rest of the movement functions rely on this!!!

    public void encoders(int leftFront, int rightFront, int leftBack, int rightBack)
    {
        leftFrontDrive.setMode(DcMotor.RunMode.STOP_AND_RESET_ENCODER);
        rightFrontDrive.setMode(DcMotor.RunMode.STOP_AND_RESET_ENCODER);
        leftBackDrive.setMode(DcMotor.RunMode.STOP_AND_RESET_ENCODER);
        rightBackDrive.setMode(DcMotor.RunMode.STOP_AND_RESET_ENCODER);
        
        leftFrontDrive.setTargetPosition(leftFront*revPerMM);
        rightFrontDrive.setTargetPosition(rightFront*revPerMM);
        leftBackDrive.setTargetPosition(leftBack*revPerMM);
        rightBackDrive.setTargetPosition(rightBack*revPerMM);
        
        leftFrontDrive.setMode(DcMotor.RunMode.RUN_TO_POSITION);
        rightFrontDrive.setMode(DcMotor.RunMode.RUN_TO_POSITION);
        leftBackDrive.setMode(DcMotor.RunMode.RUN_TO_POSITION);
        rightBackDrive.setMode(DcMotor.RunMode.RUN_TO_POSITION);
        
        double power=0.2;
        
        leftFrontDrive.setVelocity(1500);
        rightFrontDrive.setVelocity(1500);
        leftBackDrive.setVelocity(1500);
        rightBackDrive.setVelocity(1500);
        
        while (opModeIsActive()&&leftFrontDrive.isBusy()||rightFrontDrive.isBusy()||leftBackDrive.isBusy()||rightBackDrive.isBusy())
        {
        }
        leftFrontDrive.setPower(0);
        rightFrontDrive.setPower(0);
        leftBackDrive.setPower(0);
        rightBackDrive.setPower(0);
    }

    public void linearSlideEncoders(int goalPos)
    {
        slideArm.setTargetPosition(goalPos);
        slideArm.setMode(DcMotor.RunMode.RUN_TO_POSITION);
        
        double power=0.2;
        
        slideArm.setVelocity(1500);
        
        
        while (opModeIsActive()&&slideArm.isBusy())
        {
        }

        slideArm.setPower(0);
    }

    public void intakeEncoders(String state)
    {
        if(state == "In"){
            upperServo.setPower(1.0);
            lowerServo.setPower(-1.0);
        } else if(state == "Out"){
            upperServo.setPower(-1.0);
            lowerServo.setPower(1.0);
        } else if(state == "Stop"){
            upperServo.setPower(0.0);
            lowerServo.setPower(0.0);
        }
    }

    public void intakePivotEncoders(String states)
    {
        if(states == "Up"){
            intakePivotMotor.setPower(-0.7);
        } else if (states == "Down"){
            intakePivotMotor.setPower(0.5);
        } else if (states == "Hold"){
            intakePivotMotor.setPower(-0.2);
        }
    }
    
    public void basket()
    {
        driveEncoders(305);
        
        leftEncoders(1220);
        turnLeft(135);
        driveEncoders(200);

        //score start :o
        linearSlideEncoders(-3000);
        slideArm.setPower(-0.15);
        intakePivotEncoders("Down");
        sleep(300);
        intakePivotEncoders("Hold");
        intakeEncoders("Out");
        sleep(2500);
        intakeEncoders("Stop");
        
        intakePivotEncoders("Up");
        sleep(2000);
        intakePivotMotor.setPower(0.0);
        slideArm.setPower(0.0);
        linearSlideEncoders(0);
        //score end :)

        turnRight(135);
        rightEncoders(3050);
        backEncoders(305);
    }

    public void observation()
    {
        driveEncoders(305);
        leftEncoders(1830);
        turnLeft(135);
        driveEncoders(200);

        //score start :o
        linearSlideEncoders(-3000);
        slideArm.setPower(-0.15);

        intakePivotEncoders("Down");
        sleep(300);
        intakePivotEncoders("Hold");
        intakeEncoders("Out");
        sleep(2500);
        intakeEncoders("Stop");
        
        
        intakePivotEncoders("Up");
        sleep(2000);
        intakePivotMotor.setPower(0.0);
        slideArm.setPower(0.0);
        linearSlideEncoders(0);

        //score end :)
        
        //score end
        turnRight(135);
        rightEncoders(3050);
        backEncoders(305);
    }

}