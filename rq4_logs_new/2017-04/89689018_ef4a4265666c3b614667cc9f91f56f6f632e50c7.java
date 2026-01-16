package liikkuvat;

import lejos.hardware.motor.EV3LargeRegulatedMotor;
import lejos.hardware.motor.EV3MediumRegulatedMotor;
import lejos.hardware.port.Port;
import lejos.hardware.sensor.EV3IRSensor;
import lejos.utility.Delay;

public class Tykki extends Thread {
	
	public EV3MediumRegulatedMotor alusta;
	private EV3LargeRegulatedMotor tykki;
	
	private int rotaatio;
	
	private boolean paalla;
	
	public Tykki(Port porttiAlusta, Port porttiTykki) {
		this.alusta = new EV3MediumRegulatedMotor(porttiAlusta);
		this.alusta.setSpeed(50);
		
		this.tykki = new EV3LargeRegulatedMotor(porttiTykki);
		this.tykki.setSpeed(800);
		
		this.rotaatio = 0;
		
		this.paalla = true;
	}
	
	public void run() {
		while (this.paalla) {
			
		}
	}
	
	public void pyoritaAlustaa(int asteet) {
		if (Math.abs(this.rotaatio + asteet) <= 180) {
			this.rotaatio += asteet;
			this.alusta.rotate(asteet);
		}
	}
	
	public void pyoritaAlustaaSulavasti(int lukema) {
		if (lukema == 1) {
			this.alusta.rotateTo(-120, true);
		} else {
			this.alusta.rotateTo(120, true);
		}
	}
	
	public void lopetaAlustanPyoriminen() {
		this.alusta.stop();
		while (this.alusta.isMoving()) {
			
		}
	}
	
	public void ammuTykilla() {
		this.tykki.rotate(360);
	}
	
	public void lopeta() {
		this.paalla = false;
	}

}