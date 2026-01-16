package com.mockito.test;

import static org.junit.Assert.assertNotNull;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.runners.MockitoJUnitRunner;

import testClases.PlantWaterer;
import testClases.WaterSource;
import testClases.WateringScheduler;

/**
 * injectMocks annotation simplifies mock and spy injection. It can inject
 * objects using constructor injection, setter injection or field injection.
 * 
 * @author MXPCL
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class MockInjectingTest {

	/**
	 * Creates a mock of a given type
	 */
	@Mock
	private WaterSource waterSourceMock;

	/**
	 * Creates a spy of a given object
	 */
	@Spy
	private WateringScheduler wateringSchedulerSpy;

	/**
	 * Creates an object of a given type and injects mocks and spies existing in
	 * a test
	 */
	@InjectMocks
	private PlantWaterer plantWaterer;

	@Test
	public void shouldInjectMocks() {
		assertNotNull(plantWaterer.getWateringScheduler());
		assertNotNull(plantWaterer.getWaterSource());
	}

}