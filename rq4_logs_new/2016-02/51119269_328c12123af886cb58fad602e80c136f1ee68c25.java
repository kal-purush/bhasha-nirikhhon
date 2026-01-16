import java.util.Scanner;
/**
 * The Class FireDangerRatingSystem.
 */
public class FireDangerRatingSystem {
	
	/**
	 * The main method.
	 *
	 * @param args the arguments
	 */
	public static void main(String[] args) {
		//Start Fire Danger Rating Process
		System.out.println("Welcome To Fire Danger Rating System.");
		Subroutine.initDimensions();
		System.out.println();
		Scanner console = new Scanner(System.in);
		System.out.println("Is there snow on the ground. Enter 1 for Yes and 2 for No");
		int input = console.nextInt();
		if(input == 1)
			Subroutine.snow = true;
		//Get dry and wet temp.
		System.out.println("What is the dry bulb temp: ");
		double drytemp = console.nextDouble();
		System.out.println("What is the wet bulb temp: ");
		double wettemp = console.nextDouble();
		
		Subroutine.dry = drytemp;
		Subroutine.wet = wettemp;
		
		//Check for snow
		if(Subroutine.snow){
			Subroutine.initAllIndex(0);
			System.out.println("What is the precipitation. ");
			input = console.nextInt();
			//Check for rain
			if(Subroutine.isRain()){
				Subroutine.adjustBUI();
				System.out.println("Fire Load Index is: "+ Subroutine.fireLoadRating);
				return;
				
			}
			System.out.println("Fire Load Index is: "+ Subroutine.fireLoadRating);
			return;
			
		}else{
			//There is no snow on ground so calculate spread indexes and fire load
			for(int i = 0; i < 3; i++){
				if(Subroutine.diff < Subroutine.C[i]){
					Subroutine.calcFineFuelMoisture((i), Stage.NONE);
				}
			}
			//Find drying factor for the day
			for(int i = 1; i < 6; i++){
				if(Subroutine.fineFuelMoist > Subroutine.D[i]){
					Subroutine.dryFactor = i-1;
				}else{
					Subroutine.dryFactor = 7;
				}
			}
			//Testing if fine fuel moisture is one or less
			if(Subroutine.fineFuelMoist > 1){
				Subroutine.fineFuelMoist = 1;	
			}else{
					Subroutine.fineFuelMoist = 1;
					Subroutine.calcFineFuelMoisture(1, Stage.HERB);
					//Adjust BUI for Precipitation before adding drying factor
					if(Subroutine.precip > 0.1){
						Subroutine.adjustBUI();
						if(Subroutine.bUOIndex > 0){
							Subroutine.bUOIndex = 0;
						}else{
							Subroutine.bUOIndex = Subroutine.bUOIndex + Subroutine.dryFactor;
							Subroutine.calcAdjustedFuelMoisture(); 
						}
					}else{
						Subroutine.bUOIndex = Subroutine.bUOIndex + Subroutine.dryFactor;
						Subroutine.calcAdjustedFuelMoisture(); 
					}
				}
			
			if(Subroutine.adjustedFuelMoisture > 30){
				if(Subroutine.fineFuelMoist > 30){
						Subroutine.initAllIndex(1);
						System.out.println("Fire Load Index is: "+ Subroutine.fireLoadRating);
						return;
					}else{
						System.out.println("What is the wind speed: ");
						input = console.nextInt();
						//Test to see if wind speed is greater than 14
						if(Subroutine.wind < 14){
						Subroutine.calcGrassSpreadIndex(6);
						Subroutine.calcTimberSpreadIndex(6);
						}else{
							Subroutine.calcGrassSpreadIndex(14);
							Subroutine.calcTimberSpreadIndex(14);
						}
						
						if((Subroutine.bUIndex == 0) && (Subroutine.tImberIndex == 0)){
							System.out.println("Fire Load Index is: "+ Subroutine.fireLoadRating);
							return;
						}else{
							Subroutine.calcFireLoadIndex();
							System.out.println("Fire Load Index is: "+ Subroutine.fireLoadRating);
							return;
						}
					}
				}
			System.out.println("Fire Load Index is: "+ Subroutine.fireLoadRating);
			}
		}

	}