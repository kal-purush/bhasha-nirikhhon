import java.io.*;
import java.util.Scanner;

public class prog213a {

	static Scanner input = null;
	public static void main(String[] args){

		int hoursx = 0;
		int morethan8 = 0;
		int count = 0;
		double pay = 0;
		int week = 0;
		try {

			input = new Scanner(new File("213.txt"));
			//BufferedReader input = new BufferedReader(new FileReader("213.txt"));
			//String line = input.readLine();

			do {
				//hours += input.nextInt();

				int num = input.nextInt();
				System.out.print(num + " ");
			
				//week += num;

				if(num > 8) {
					morethan8 += 1;
				}

					
				//if(num == 0)
					
			//	if(week )
					
				count++;

					
				if(count == 6) 
					pay += ((num * 10) + ((num * 10) * .125));

				
				else if(count == 7) {

					
				//	pay *= 10;
					pay += ((num * 10) + ( (num * 10) * .50));
					
							
					
					week++;
					//pay *= 10;
					pay += ( morethan8 * 1.50);
				
					pay *= 10;
					
					System.out.printf("\nWeek %d: %.2f\n", week, pay);
					System.out.println();
					if(input.hasNext() == false)
						System.out.print("Hours Worked: End of reading from file");

					else
						System.out.print("Hours Worked: ");

					count = 0;
				}

				//count++;
				//line = input.readLine();
							
			} while(input.hasNext() != false);
		} catch(IOException e){ System.exit(0); }
	}
}