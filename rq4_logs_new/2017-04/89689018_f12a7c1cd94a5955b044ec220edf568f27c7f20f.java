package sensorit;

import lejos.hardware.Button;
import lejos.hardware.lcd.LCD;
import lejos.hardware.port.MotorPort;
import lejos.hardware.sensor.EV3IRSensor;
import lejos.hardware.sensor.SensorMode;
import lejos.utility.Delay;
import liikkuvat.Tykki;

public class BeaconTracker extends Thread {
	private EV3IRSensor sensor;
	private float direction;
	private Tykki tykki;
	
	private boolean paalla;
	
	public BeaconTracker(EV3IRSensor sensor) {
		this.sensor = sensor;
		this.direction = 0;
		
		this.tykki = new Tykki(MotorPort.D, MotorPort.B);
		this.tykki.start();
		
		this.paalla = true;
	}
	
	public void run() {
		this.sensor.setCurrentMode(1);
		while(!Button.ESCAPE.isDown()) {
			
			SensorMode seek = this.sensor.getSeekMode();
			float seekSample[] = new float[seek.sampleSize()];
			
			this.sensor.fetchSample(seekSample, 0);
			this.direction = seekSample[0];
			
			LCD.drawString("" + this.direction, 0, 1);
			if (this.direction > 2) {
				this.tykki.pyoritaAlustaaSulavasti(0); // Vasemmalle
			} else if (this.direction < -2) {
				this.tykki.pyoritaAlustaaSulavasti(1); // Oikealle
			} else {
				this.tykki.lopetaAlustanPyoriminen();
			}
		}
	}
	
	public void lopeta() {
		this.tykki.lopeta();
		this.paalla = false;
	}
}