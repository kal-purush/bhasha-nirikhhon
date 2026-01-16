
public class Car extends Vehicle {
	private int odometer=0;
	public String model;
	public boolean isStart;
	
	Car(IEngine Iengine){
		Iengine=new SmallEngine();
	}
	
	Car(String model, IEngine Iengine){
		 this.model=model;
		 Iengine=new LargeEngine();
	}
	
	public int getOdometer() {
		return this.odometer;
	}
	
	public void drive(int distance) {
		this.odometer+=distance;
	}
	
	public void start(IEngine IEngine) {
		IEngine.start();
		isStart=true;
	}
	public boolean getIsStart() {
		return this.isStart;
	}
}