
//simple encryption/decryption program
public class Encryption {

	public static void main(String[] args) {
		
		int key = 6;
		
		String message = "Nk2&nu}-y&oz&muotmE";
		//"Hey, how's it going?"
		
		System.out.println(message);
		
		char[] chars = message.toCharArray();
		
		for(char c : chars) {
			//c += key; 
			c -= key;
			System.out.print(c);
		}
		
		

	}

}