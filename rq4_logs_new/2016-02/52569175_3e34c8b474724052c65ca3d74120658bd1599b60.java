package com.qualcomm.ftcrobotcontroller.Ftc9015;

import com.qualcomm.robotcore.util.Range;

/**
 * Created by Serena on 2/28/2016.
 */
public class T9015TeleOp2 extends T9015Hardware{

    // amount to change the pulling guide servo position.
    final static double sPullDelta = 0.005;
    double sPullPosition = 0.0;
    boolean lock_puller = false;

    @Override public void init (){
        super.init();

        // initialize the statues of servos, motors, sensors and so on.
        set_direction_forward(true);
        set_hang_backward();
        set_hang_encoders();
        //reset_hang_encoders();

        reset_climber();
        sPullPosition = v_servo_puller.getPosition();

        telemetry.addData("Text", "*** 9015 Robot Init Data***");
        telemetry.addData("i01", "sPull:  " + String.format("%.2f", sPullPosition));
    }

    @Override
    public void loop() {

        // ======== Control Driving Wheel ===================================
        // note that if y equal -1 then joystick is pushed all of the way forward.
        float left = -gamepad1.left_stick_y;
        float right = -gamepad1.right_stick_y;

        // clip the right/left values so that the values never exceed +/- 1
        right = Range.clip(right, -1, 1);
        left = Range.clip(left, -1, 1);

        // scale the joystick value to make it easier to control
        // the robot more precisely at slower speeds.
        right = (float) scaleInput(right);
        left = (float) scaleInput(left);

        // write the values to the motors
        set_drive_power(left, right);

        // ======== Control Back Slider ===================================
        // moves slider, x to the left, b to the right
        if (gamepad1.x || gamepad2.x) {
            v_motor_slider.setPower(-1);
        } else if (gamepad1.b || gamepad2.b) {
            v_motor_slider.setPower(1);
        } else {
            v_motor_slider.setPower(0);
        }

        // ========= Control the Hanger ===================================
        // power for the hangers motors
        double hpower = 0.0;

        //moves hanging arms
        if (gamepad1.dpad_up) {
            if (have_hang_encoders_reached(convertHangerDegreesToTicks(120)))
                hpower = 0.05;
            else
                hpower = -0.20;
        } else if (gamepad1.dpad_down) {
            if (have_hang_encoders_reached(convertHangerDegreesToTicks(10)))
                hpower = -0.05;
            else
                hpower = 0.20;
        }

        set_hang_power(hpower);

        //=========== Control the Pulling Tap ==============================
        if (gamepad1.left_bumper) {
            lock_puller = !lock_puller;     // taggle the state
        }

        if (lock_puller == true) {
            v_motor_puller.setPower(0.25);
        } else {
            // moves puller, x is up, y is down
            if (gamepad1.y) {
                v_motor_puller.setPower(-0.5);
            } else if (gamepad1.a) {
                v_motor_puller.setPower(1);
            } else {
                v_motor_puller.setPower(0);
            }
        }

        // ========== Control the Pulling Guide Servo ============================
        // if the bumper is pushed on gamepad1, increment the position of
        // the pull servo.
        if (gamepad1.right_bumper || gamepad2.right_bumper) {
            sPullPosition -= sPullDelta;
        }

        // if the trigger is pushed on gamepad1, decrease the position of
        // the arm servo.
        if (gamepad1.right_trigger > 0.25 || gamepad2.right_trigger > 0.25) {
            sPullPosition += sPullDelta;
        }

        // ========== Control the Back Blocking Bar Servo ============================
        // if the bumper is pushed on gamepad1, increment the position of
        // the arm servo.
        if (gamepad1.dpad_right || gamepad2.dpad_down) {
            v_servo_back.setPosition(H_BACK_MIN_RANGE);
        }

        // if the trigger is pushed on gamepad1, decrease the position of
        // the arm servo.
        if (gamepad1.dpad_left || gamepad2.dpad_up) {
            v_servo_back.setPosition(H_BACK_MAX_RANGE);
        }

        // =========== Control the Pulling Guide Servo ===============================
        // clip the position values so that they never exceed their allowed range.
        sPullPosition = Range.clip(sPullPosition, SPULL_MIN_RANGE, SPULL_MAX_RANGE);
        // write position values to the wrist and claw servo
        v_servo_puller.setPosition(sPullPosition);


        // ============ Control the Climber Dropping Servo ============================
        if (gamepad2.y) {
            // if the gamepad2.y, drop the climber
            v_servo_climber.setPosition(RobotInfo.CLIMBER_LOCK_POSITION);
        } else if (gamepad2.a) {
            // if the gamepad2.y, drop the climber
            v_servo_climber.setPosition(RobotInfo.CLIMBER_RELEASE_POSITION);
        }



        telemetry.addData("Text", "*** 9015 Robot Data***");
        telemetry.addData("01", "sPull:  " + String.format("%.2f", sPullPosition));
        telemetry.addData("02", "pwr left: " + String.format("%.2f ", left) +
                "right: " + String.format("%.2f", right));
        telemetry.addData("03", "pwr hang: " + String.format("%.2f", hpower));
    }

    /*
     * This method scales the joystick input so for low joystick values, the
     * scaled value is less than linear.  This is to make it easier to drive
     * the robot more precisely at slower speeds.
     */
    double scaleInput(double dVal)  {
        double[] scaleArray = { 0.0, 0.05, 0.09, 0.10, 0.12, 0.15, 0.18, 0.24,
                0.30, 0.36, 0.43, 0.50, 0.60, 0.72, 0.85, 1.00, 1.00 };

        // get the corresponding index for the scaleInput array.
        int index = (int) (dVal * 16.0);

        // index should be positive.
        if (index < 0) {
            index = -index;
        }

        // index cannot exceed size of array minus 1.
        if (index > 16) {
            index = 16;
        }

        // get value from the array.
        double dScale = 0.0;
        if (dVal < 0) {
            dScale = -scaleArray[index];
        } else {
            dScale = scaleArray[index];
        }

        // return scaled value.
        return dScale;
    }
}

