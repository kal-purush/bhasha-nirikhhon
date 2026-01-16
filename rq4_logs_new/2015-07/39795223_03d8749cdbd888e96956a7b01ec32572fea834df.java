
public class RaceDemo {

	public static void main(String args[]){
		
		Racer racer = new Racer();
		Thread tortoise = new Thread(racer,"Tortoise");
		Thread hare = new Thread(racer,"Hare");
		
		tortoise.start();
		hare.start();
	}
}