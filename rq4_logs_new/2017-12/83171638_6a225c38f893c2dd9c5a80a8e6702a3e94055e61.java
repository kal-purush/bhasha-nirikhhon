package org.firstinspires.ftc.teamcode.ftc11587;

import com.qualcomm.robotcore.eventloop.opmode.LinearOpMode;
import com.qualcomm.robotcore.eventloop.opmode.TeleOp;
import com.qualcomm.robotcore.eventloop.opmode.Disabled;
import com.qualcomm.robotcore.eventloop.hardware.DcMotor;
import com.qualcomm.robotcore.util.Range;

@TeleOp(name="Relic Recovery: Basic Holonomic Drive", group="Relic Recovery")
@Disabled //comment out this line to add this OpMode to the Driver Station select list

public class BasicHolonomicDrive extends LinearOpMode {

	/*Declarations*/
	DcMotor lfMotor;
	DcMotor rfMotor;
	DcMotor lrMotor;
	DcMotor rrMotor;

	@Override
	public void runOpMode() {

		/*Telemetry data for driver feedback*/
		telemetry.addData("Status","Holonomic Drive Initialized");
		telemetry.update();

		/*Hardware mapping pulls the motor/servo names from the configuration on the robot-side controller phone*/
		lfMotor = hardwareMap.dcMotor.get("lfmotor");
		rfMotor = hardwareMap.dcMotor.get("rfmotor");
		lrMotor = hardwareMap.dcMotor.get("lrmotor");
		rrMotor = hardwareMap.dcMotor.get("rrmotor");

		/*Quickly change motor polarity, if needed, by changing FORWARD to REVERSE*/
		lfMotor.setDirection(DcMotor.Direction.FORWARD);
		rfMotor.setDirection(DcMotor.Direction.FORWARD);
		lrMotor.setDirection(DcMotor.Direction.FORWARD);
		rrMotor.setDirection(DcMotor.Direction.FORWARD);

		/*Wait until the driver presses PLAY*/
		waitForStart();

		while (opModeIsActive()) {

		public void moveRobot() {
			/* Robot Drivetrain Configuration
			 *
			 *           _______
			 * lfMotor \\       // rfMotor
			 * 			|		|
			 * 			|		|
			 * lrMotor //_______\\ rrMotor
			 *
			 *
			 */

			/* Gamepad Controller Configuration
			 *
			 *              LEFT JOYSTICK       						RIGHT JOYSTICK
			 *
			 *              Move Forward								    Null
			 *       		      ^										     ^
			 *       			  |										     |
			 * Strafe Left <-----   -----> Strafe Right   Rotate Left <-----   -----> Rotate Right
			 *       			  |											 |
			 *       			  v											 v
			 *              Move Backward									Null
			 *
			 */

			/*Assign gamepad inputs*/
			float gp1_ljoy_y = -gamepad1.left_stick_y;	//Forward Y normally yields negative value - reverse to make more sense
			float gp1_ljoy_x = gamepad1.left_stick_x;
			float gp1_rjoy_x = gamepad1.right_stick_x;

			/*Basic holonomic drive formulas based on motor output matrix*/
			float lfPwr = -gp1_ljoy_y - gp1_ljoy_x - gp1_rjoy_x;
			float rfPwr = gp1_ljoy_y - gp1_ljoy_x - gp1_rjoy_x;
			float lrPwr = gp1_ljoy_y + gp1_ljoy_x - gp1_rjoy_x;
			float rrPwr = -gp1_ljoy_y + gp1_ljoy_x - gp1_rjoy_x;

			/*The above matrix could return values outside [-1,1]...if so, scale values*/
			//TO-DO...until we work this out, use the clipping method//
			//lfPwr_scale = Range.scale(lfPwr, -2, 2, -1, 1);
			//rfPwr_scale = Range.scale(rfPwr, -2, 2, -1, 1);
			//lrPwr_scale = Range.scale(lrPwr, -2, 2, -1, 1);
			//rrPwr_scale = Range.scale(rrPwr, -2, 2, -1, 1);
			//Can it really be this easy?

			/*Clipping method -- UGLY!*/
			lfPwr_clip = Range.clip(lfPwr, -1, 1);
			rfPwr_clip = Range.clip(rfPwr, -1, 1);
			lrPwr_clip = Range.clip(lrPwr, -1, 1);
			rrPwr_clip = Range.clip(rrPwr, -1, 1);

			/*Set motor power - replace clipped values w/ scaled once scaling algorithm complete*/
			lfMotor.setPower(lfPwr_clip);
			rfMotor.setPower{rfPwr_clip);
			lrMotor.setPower(lrPwr_clip);
			rrMotor.setPower(rrPwr_clip);

			/*Send telemetry to driver station for feedback*/
			telemetry.addData("Motors","LF (%.2f) | RF (%.2f) | LR (%.2f) | RR (%.2f)", lfPwr_clip, rfPwr_clip, lrPwr_clip, rrPwr_clip);
			telemetry.update();
			}

		}

	}





}