/*----------------------------------------------------------------------------*/
/* Copyright (c) 2018 FIRST. All Rights Reserved.                             */
/* Open Source Software - may be modified and shared by FRC teams. The code   */
/* must be accompanied by the FIRST BSD license file in the root directory of */
/* the project.                                                               */
/*----------------------------------------------------------------------------*/

package frc.robot.subsystems;

import edu.wpi.first.cameraserver.CameraServer;
import edu.wpi.first.wpilibj.command.Subsystem;

public class Camera extends Subsystem {
    private CameraServer server;

    public void initDefaultCommand() {}

    public Camera() {
            server = CameraServer.getInstance();
            enableStreaming();
    }
    public void enableStreaming() {
            server.startAutomaticCapture();
    }

}