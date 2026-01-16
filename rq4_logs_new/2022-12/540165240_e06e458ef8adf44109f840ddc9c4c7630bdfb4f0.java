
package Team7159.ComplexRobots;


import com.qualcomm.robotcore.hardware.HardwareMap;
import com.qualcomm.robotcore.hardware.*;


import Team7159.BasicRobots.BasicMecanum;
import Team7159.Enums.Direction;

public class Christwopher extends BasicMecanum {

    public DcMotor linearSlidesMotor;
    public Servo servoClaw;

    public double servoClawOpen = 0.85;
    public double servoClawGrab = 0.6;

    public void init(HardwareMap Map) {

        super.init(Map);

        linearSlidesMotor = Map.dcMotor.get("linearSlidesMotor");
        servoClaw = Map.servo.get("servoClaw");

        linearSlidesMotor.setMode(DcMotor.RunMode.STOP_AND_RESET_ENCODER);
        linearSlidesMotor.setMode(DcMotor.RunMode.RUN_USING_ENCODER);
        linearSlidesMotor.setZeroPowerBehavior(DcMotor.ZeroPowerBehavior.BRAKE);
        linearSlidesMotor.setPower(0);

        servoClaw.setPosition(servoClawGrab);
    }

}