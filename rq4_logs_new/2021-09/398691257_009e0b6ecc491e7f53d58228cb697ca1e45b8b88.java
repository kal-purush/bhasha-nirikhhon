import static org.junit.Assert.*;

import org.junit.Test;

public class Cars {

	@Test
	public void canDriveBicycle() {
		Bicycle bicycle= new Bicycle();
		assertTrue("Inconsistent initialization results. ",bicycle.getOdometer()==0);
		assertTrue("Can't ride the bike. ",bicycle.isDriveable()==true);	
	}
	
	@Test
	public void canIncreaOdometer() {
		String model="Corvette";
		IEngine engine = new LargeEngine();
		Car car = new Car(model, engine);
		assertTrue("Engine not start",car.isStart==true);
		assertEquals("Inconsistent initialization results.",0,car.getOdometer());
		car.drive(10);
		assertEquals("No increase in mileage. ",10,car.getOdometer());			
	}
	
	@Test
	public void canBuildCar() {
		int small=4;
		int large=8;
		SmallEngine smallEngine = new SmallEngine();
		LargeEngine largeengine = new LargeEngine();
		assertTrue("small engine is not 4",smallEngine.getCylinderCount()==small);
		assertTrue("large engine is not 8",largeengine.getCylinderCount()==large);
	}
	
	@Test
	public void canStartSmallConvertibleCar() {
		int small=4;
		String model="Convertible";
		SmallEngine smallEngine = new SmallEngine();
		Convertible_Car convertCar = new Convertible_Car(model,smallEngine);
		convertCar.start(smallEngine);
		assertTrue("cylider count not match",smallEngine.getCylinderCount()==small);
		assertTrue("model name not match",convertCar.model==model);
		assertTrue("convertible car start",convertCar.isStart==true);	
	}
	
	@Test
	public void canSwitchConvertible() {
		boolean switchStates=false;
		String model = "convert";
		SmallEngine smallEngine = new SmallEngine();
		Convertible_Car convertCar = new Convertible_Car(model ,switchStates ,smallEngine);
		convertCar.start(smallEngine);
		assertTrue("not raise", convertCar.getconvertibleStatus()==switchStates);
	}
}