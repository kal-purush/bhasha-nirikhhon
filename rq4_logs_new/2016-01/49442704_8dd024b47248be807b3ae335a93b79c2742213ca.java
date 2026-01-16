public class Ex_1_1_03 {
	public static void main (String[] args) {
		if (args.length != 3){
			System.out.println("No three arguments!");
			return;
		}

		if (args[0].equals(args[1])) {
			if (args[0].equals(args[2])){
				System.out.println("Equal.");
				return;
			}
		}

		System.out.println("Not Equal.");
	}
}