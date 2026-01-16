package org.usfirst.frc.team5112.robot;

import com.kylecorry.frc.vision.MultiCameraSource;

import edu.wpi.cscore.UsbCamera;
import edu.wpi.cscore.VideoCamera;
import edu.wpi.first.wpilibj.IterativeRobot;

/**
 * The VM is configured to automatically run this class, and to call the
 * functions corresponding to each mode, as described in the IterativeRobot
 * documentation. If you change the name of this class or the package after
 * creating this project, you must also update the manifest file in the resource
 * directory.
 */
public class Robot extends IterativeRobot {
	
	private MultiCameraSource camera;

	@Override
	public void robotInit() {
		camera = new MultiCameraSource();
		
		VideoCamera cam0 = new UsbCamera("PegCamera", 0);
		VideoCamera cam1 = new UsbCamera("BoilerCamera", 1);
		
		camera.addCamera(cam0, "PegCamera");
		camera.addCamera(cam1, "BoilerCamera");
		
		camera.switchToCamera("PegCamera");
		camera.start();
		
	}

	@Override
	public void autonomousInit() {
		camera.switchToCamera("PegCamera");
	}

	@Override
	public void autonomousPeriodic() {

	}

	@Override
	public void teleopInit() {
		camera.switchToCamera("BoilerCamera");
	}
	
	@Override
	public void teleopPeriodic() {
	}

	@Override
	public void testPeriodic() {
	}
}
