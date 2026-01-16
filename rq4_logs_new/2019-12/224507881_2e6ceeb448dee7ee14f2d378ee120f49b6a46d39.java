package test.java.funda.prog101;

import static org.junit.Assert.*;

import org.junit.Test;

import main.java.funda.prog101.Boiler;
import main.java.funda.prog101.BoilerSensor;
import main.java.funda.prog101.Container;
import main.java.funda.prog101.ContentType;
import main.java.funda.prog101.Pot;
import main.java.funda.prog101.Sensor;

public class BoilerSensorTest {

	@Test
	public void isEmtpy_False_BoilerIsNotEmpty() {
		byte quantity = 12;
		Sensor sensorboiler = new BoilerSensor();
		Container boiler = new Boiler(ContentType.WATER, quantity, quantity, true);
		
		boolean expected = false;
		boolean actual = sensorboiler.isEmpty(boiler);
		
		assertEquals(expected, actual);
		
	}
	@Test
	public void isEmtpy_True_BoilerIsEmpty() {
		byte quantity = 12;
		Sensor sensorboiler = new BoilerSensor();
		Container boiler = new Boiler(ContentType.EMPTY, quantity, quantity, true);
		
		boolean expected = true;
		boolean actual = sensorboiler.isEmpty(boiler);
		
		assertEquals(expected, actual);
		
	}

}