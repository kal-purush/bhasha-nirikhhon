import java.math.BigInteger;
import java.util.Scanner;
//liczby pierwsze
class liczby_pierwsze
{
        public static void main(String[] args)
        {
			Scanner scanner = new Scanner(System.in);
			
			int ile_liczb = scanner.nextInt();
			int liczba, num = 0, num2 = 0;
			boolean test = false;
			double pzx,x;
			int ograniczenie = 0;
			
			//System.out.println("zatem bede sprawdzac " + ile_liczb + " liczb:");
				
				
			for (int i = 0; i < ile_liczb; i++) { 
				liczba = (int)scanner.nextInt();
				if (liczba == 1) {
					System.out.println("NIE");
					continue;
				}
				ograniczenie = (int)Math.sqrt(liczba);
				//System.out.println("wczytalem liczbe: " + liczba);
				test = true;
				for (int j = 2; j <= ograniczenie; j++) {
					
					if ( (liczba % j) == 0) {
						test = false;
						System.out.println("NIE");
						break;						
					}
							
				}
			
				if (test == true) {
					System.out.println("TAK");					
				}
				
			}
				
		}    
}