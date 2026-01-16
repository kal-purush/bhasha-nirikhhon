
import lockVersion.Bathroom;

public class MainLock {

	public static void main(String[] args) throws InterruptedException {
		if(args.length != 1) {
			System.out.println("./main <BATHROOM_CAPACITY> <PROGRAM_TYPE>");
		}
		int bathroomCapacity = Integer.parseInt(args[0]);
		Bathroom b = new Bathroom(bathroomCapacity);
		System.out.println(args[0]);
		b.start();
		
		while(true) {
			continue;
		}

	}
}