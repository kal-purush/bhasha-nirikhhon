package test.java.funda.prog101;

import static org.junit.Assert.*;
import org.junit.Test;

import main.java.funda.prog101.Container;
import main.java.funda.prog101.ContentType;
import main.java.funda.prog101.Pot;
import main.java.funda.prog101.PotSensor;
import main.java.funda.prog101.Sensor;

public class PotSensorTest {

	@Test
	public void isAboveHeater_True_PutOn() {
		Pot pot = new Pot();
		Sensor sensorpot = new PotSensor();
		boolean expected = true;
		boolean actual = sensorpot.isAboveHeater(pot);
		
		assertEquals(expected, actual);
	}
	
	@Test
	public void isAboveHeater_False_TakeOut() {
		Pot pot = new Pot();
		pot.putOnTakeOutPot();
		Sensor sensorpot = new PotSensor();
		boolean expected = false;
		boolean actual = sensorpot.isAboveHeater(pot);
		
		assertEquals(expected, actual);
	}
	
	@Test
	public void isEmpty_True_PotIsEmtpy() {
		byte quantity = 12;
		Container pot = new Pot(ContentType.EMPTY, quantity, quantity, true);
		Sensor sensorpot = new PotSensor();
		
		boolean expected = true;
		boolean actual = sensorpot.isEmpty(pot);
		
		assertEquals(expected, actual);
	}
	public void isEmpty_False_PotIsNotEmtpy() {
		byte quantity = 12;
		Container pot = new Pot(ContentType.WATER, quantity, quantity, true);
		Sensor sensorpot = new PotSensor();
		
		boolean expected = false;
		boolean actual = sensorpot.isEmpty(pot);
		
		assertEquals(expected, actual);
	}

}