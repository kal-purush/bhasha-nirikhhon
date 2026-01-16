package org.usfirst.frc.team997.robot;

import edu.wpi.first.wpilibj.GenericHID.Hand;
import edu.wpi.first.wpilibj.Joystick;

/**
 * @author Chris G.
 */
public class Controoooler {
	/**
	 * Da joystick
	 */
	private Joystick j;
	
	/**
	 * Give the dang port for da Controooler
	 * @param porterino
	 */
	public Controoooler(int porterino) {
		j = new Joystick(porterino);
	}
	
	/**
	 * Left x
	 */
	public double getX() {
		return j.getX();
	}
	
	/**
	 * Left y
	 */
	public double getY() {
		return j.getY();
	}
	
	/**
	 * Right x
	 */
	public double getRX() {
		return j.getX(Hand.kRight);
	}

	/**
	 * Right y
	 */
	public double getRY() {
		return j.getY(Hand.kRight);
	}
	
	/**
	 * Left trigger
	 */
	public boolean getLT() {
		return j.getTrigger(Hand.kLeft);
	}
	
	/**
	 * Right trigger
	 */
	public boolean getRT() {
		return j.getTrigger(Hand.kRight);
	}

	/**
	 * Left Button (above trigger)
	 */
	public boolean getLB() {
		return j.getTop(Hand.kLeft);
	}
	
	/**
	 * Right button (above trigger)
	 */
	public boolean getRB() {
		return j.getTop(Hand.kRight);
	}
}