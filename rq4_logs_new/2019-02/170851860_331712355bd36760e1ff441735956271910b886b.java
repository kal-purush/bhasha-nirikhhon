import java.util.Scanner;

import br.com.testetw.trains.controller.RailroadManager;

public class Main {
	public static void main(String[] args){
		Scanner scanner = new Scanner(System.in);
		String[] routesArray;
		int validatedRoutes = 0;
		
		do{
			System.out.println("Informe as rotas:");
			String inputRoute = scanner.nextLine();
			inputRoute = inputRoute.replace(" ","");
			routesArray = inputRoute.split(",");
			
			validatedRoutes = Main.getValidatedRoutes(routesArray);
		}while(routesArray.length <= 1 || validatedRoutes != routesArray.length);
		
		RailroadManager railroad = new RailroadManager(routesArray);
		
		System.out.println("Output #1: "+railroad.getRouteDistance("abc"));
		System.out.println("Output #2: "+railroad.getRouteDistance("ad"));
		System.out.println("Output #3: "+railroad.getRouteDistance("adc"));
		System.out.println("Output #4: "+railroad.getRouteDistance("aebcd"));
		System.out.println("Output #5: "+railroad.getRouteDistance("aed"));
		System.out.println("Output #6: "+railroad.getQtdRoutesByStops("C", "C", 1, 3));
		System.out.println("Output #7: "+railroad.getQtdRoutesByStops("A", "C", 0, 4));
		System.out.println("Output #8: "+railroad.getShortestRouteDistance("A", "C"));
		System.out.println("Output #9: "+railroad.getShortestRouteDistance("B", "B"));
		System.out.println("Output #10: "+railroad.getQtdRoutesByDistance("C", "C", 1, 30));
	}
	
	public static int getValidatedRoutes(String[] routesArray){
		String invalidRoutes = "";
		int validatedRoutes = 0;
		int index = 0;
		String route;
		
		do{
			route = routesArray[index];
			
			for (int i = index+1; i < routesArray.length; i++) {
				if(route.equals(routesArray[i])){
					System.out.println("N�o � poss�vel cadastrar rotas repetidas!");
					return validatedRoutes;
				}
			}
			
			if(route.matches("[a-eA-E]{2}[1-9]+") && !route.substring(0,1).equals(route.substring(1,2)) && !route.substring(2,3).equals("0")){
				validatedRoutes++;
			}else{
				invalidRoutes += " "+route;
			}
			index++;
		}while(index < routesArray.length && validatedRoutes<routesArray.length);
		
		if(!invalidRoutes.isEmpty()){
			System.out.println("Rotas inv�lidas: "+invalidRoutes);
		}
		
		return validatedRoutes;
	}
}