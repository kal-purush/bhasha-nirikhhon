package frc.lib.util;

import edu.wpi.first.wpilibj.DriverStation;

public final class AllianceCheck {
  public static boolean isBlue() {
    var alliance = DriverStation.getAlliance();
    if (alliance.isPresent()) {

        return alliance.get() == DriverStation.Alliance.Blue;
    }
    return false;
  }
}