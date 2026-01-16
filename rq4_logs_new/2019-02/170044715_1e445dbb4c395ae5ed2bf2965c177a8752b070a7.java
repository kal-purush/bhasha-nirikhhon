
public class Bob {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.println(enough(10, 5, 5));
		System.out.println(enough(10, 10, 5));
		System.out.println(enough(50, 45, 10));
		
	}
	
	public static int enough(int cap, int on, int wait) {
		int passengers = 0;
		
		if(on + wait > cap) {
			int spaces = cap - on;
			passengers = wait - spaces;
		}
		else {
			passengers = 0;
		}
		
		
		return passengers;
	}

}