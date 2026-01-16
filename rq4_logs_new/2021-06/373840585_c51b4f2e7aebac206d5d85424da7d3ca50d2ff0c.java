package client;
import domain.VehicleType;
import factory.VehicleFactory;

public class App {
	public static void main(String[] args) {
		
		VehicleFactory factory = new VehicleFactory();
		factory.getVehicle(VehicleType.BUS).startEngine();
	}
}