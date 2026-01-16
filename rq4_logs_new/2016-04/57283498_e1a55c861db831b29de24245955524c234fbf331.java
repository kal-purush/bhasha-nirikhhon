package car;

public class Car {
	
	int maxSpeed = 100;
	int minSpeed = 0;
	
	double weight = 4079;
	
	boolean isTheCarOn = false;
	char condition = 'A';
	String nameOfCar = "Lucy";

	double maxFuel = 16;
	double currentFuel = 8;
	double mpg = 26.4;
	
	int numberOfPeopleInCar = 1;
	int maxNumberOfPeopleInCar = 6;
	
	
	public Car() {
		
	}
	
	public Car(int customMaxSpeed, double customWeight, boolean customIsTheCarOn) {
		maxSpeed = customMaxSpeed;
		weight = customWeight;
		isTheCarOn = customIsTheCarOn;
	}
	
	public void printVariables() {
		System.out.println("This is maxSpeed: " + maxSpeed);
		System.out.println(minSpeed);
		System.out.println(weight);
		System.out.println(isTheCarOn);
		System.out.println(condition);
		System.out.println(nameOfCar);
		System.out.println(numberOfPeopleInCar);
	}
	
	public void upgradeMinSpeed() {
		minSpeed = maxSpeed;
		maxSpeed++;
	}
	
	public void getIn() {
		// If there aren't too many people in the car
		if (numberOfPeopleInCar < maxNumberOfPeopleInCar) {
			// then someone can get in
			numberOfPeopleInCar++;
			System.out.println("Someone got in");
		} else {
			// otherwise print out the fact the car is full.
			System.out.println("The car is full! " + numberOfPeopleInCar + " = " + maxNumberOfPeopleInCar);
		}
	}
	
	public void getOut() {
		// If there's people in the car
		if (numberOfPeopleInCar > 0) {
			// then tell one person to get out
			numberOfPeopleInCar--;
		} else {
			// otherwise no one can get out and we'll print that.
			System.out.println("No one is in the Car " + numberOfPeopleInCar);
		}
	}
	
	public double howManyMilesTillOutOfGas() {
		return currentFuel * mpg;
	}
	
	public double maxMilesPerFillUp() {
		return maxFuel * mpg;
	}
	
	public void turnTheCarOn() {
		// If the car isn't on...
		if (!isTheCarOn) {
			// turn it on
			isTheCarOn = true;
		} else {
			// otherwise print out the fact it's on
			System.out.println("The Car is already on " + isTheCarOn);
		}
	}
	
	public static void main(String[] args) {

		Car tommyCar = new Car();
		tommyCar.getOut();
		tommyCar.getOut();
		tommyCar.getIn();
		tommyCar.getIn();
		tommyCar.getIn();
		tommyCar.getIn();
		tommyCar.getIn();
		tommyCar.getIn();
		tommyCar.getIn();
		tommyCar.turnTheCarOn();
		tommyCar.turnTheCarOn();
		
	}

}