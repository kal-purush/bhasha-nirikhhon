import java.net.MalformedURLException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import lejos.hardware.Button;
import lejos.hardware.motor.EV3LargeRegulatedMotor;
import lejos.hardware.motor.Motor;
import lejos.hardware.port.MotorPort;
import lejos.hardware.port.SensorPort;
import lejos.hardware.sensor.EV3GyroSensor;
import lejos.hardware.sensor.EV3TouchSensor;
import lejos.hardware.sensor.EV3UltrasonicSensor;
import lejos.hardware.sensor.SensorMode;
import lejos.remote.ev3.RemoteEV3;
import lejos.robotics.RegulatedMotor;
import lejos.robotics.SampleProvider;
import lejos.utility.Delay;

public class Robot {
	private RemoteEV3 ev3;
    private RegulatedMotor moteurGauche;
    private RegulatedMotor moteurDroit;
    private EV3GyroSensor gyro;
    private EV3UltrasonicSensor senseurGauche;
    private EV3UltrasonicSensor senseurDroit;
    private EV3TouchSensor bouton;
    
    private float distanceRight;
    private float distanceLeft;
    private float touchValue;

    public Robot() throws RemoteException, MalformedURLException, NotBoundException {
        RemoteEV3 ev3 = new RemoteEV3("10.0.1.1");
        ev3.isLocal();
        moteurGauche = Motor.A;
        moteurDroit = Motor.D;
    }

    public void test(){
        /*moteurGauche.setSpeed(720);
        moteurDroit.setSpeed(720);*/
        moteurGauche.forward();
        moteurDroit.forward();
        //Delay.msDelay(1000);
        /*moteurGauche.stop();
        moteurDroit.stop();
        moteurGauche.rotate(90);
        moteurGauche.forward();*/
        Button.waitForAnyPress();
        moteurGauche.stop();
        moteurDroit.stop();
    }

    public void explore() {
        System.out.println("hello");
        Sensor sensor = new Sensor();
        Thread t = new Thread(sensor);
        t.start();
        for (int i = 0; i < 10; i++) {
            if (sensor.distanceRight > 20) {
                moteurGauche.rotate(90);
            } else if (touchValue == 1) {
                moteurGauche.backward();
                moteurDroit.backward();
                Delay.msDelay(100);
                moteurGauche.stop();
                moteurDroit.stop();
            } else if (sensor.distanceLeft > 20) {
                moteurGauche.rotate(200);
            } else {
                moteurGauche.forward();
                moteurDroit.forward();
                if (sensor.distanceRight > 20 && touchValue == 1) {
                	moteurGauche.stop();
                	moteurDroit.stop();
                }
            }
        }
        Button.waitForAnyPress();
        moteurGauche.stop();
        moteurDroit.stop();
        sensor.close();
        close();
    }

    public void close() {
        moteurDroit.close();
        moteurGauche.close();
        senseurDroit.close();
        senseurGauche.close();
        bouton.close();
        gyro.close();
    }
    
    public void distance(){
        senseurGauche.getDistanceMode();
    }
    
    public static void main(String[] args) throws RemoteException, MalformedURLException, NotBoundException {
        //Robot EV3 = new Robot();
        //EV3.test();
    	//moteurGauche.forward();
        Motor.A.forward();
     
        Motor.D.forward();
        Button.waitForAnyPress();
        Motor.A.stop();
        Motor.D.stop();
    }
}