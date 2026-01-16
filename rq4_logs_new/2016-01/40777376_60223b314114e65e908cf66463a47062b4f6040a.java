package org.usfirst.frc4904.robot.leds;


import org.usfirst.frc4904.standard.custom.CustomNeoPixels;

public class OffseasonLEDs extends CustomNeoPixels {
	public enum LEDMode {
		SOLID, SWEEP, PULSE;
	}
	
	public OffseasonLEDs(int id) {
		super("LEDs", id);
	}
	
	public void setMode(LEDMode mode) {
		switch (mode) {
			case SOLID:
				super.setMode(0);
				break;
			case SWEEP:
				super.setMode(1);
				break;
			case PULSE:
				super.setMode(2);
				break;
			default:
				super.setMode(0);
				break;
		}
	}
}