import java.io.Console;
import java.util.List;
import java.util.ArrayList;

public class App {
 public static void main(String[] args) {
   Console myConsole = System.console();
   boolean programRunning = true;

   Vehicle hatchback = new Vehicle(1994, "Subaru", "Legacy", 170000, 4000);
   Vehicle suv = new Vehicle(2002, "Ford", "Explorer", 100000, 7000);
   Vehicle sedan = new Vehicle(2015, "Toyota", "Camry", 50000, 30000);
   Vehicle truck = new Vehicle(1999, "Ford", "Ranger", 100000, 4000);
   Vehicle crossover = new Vehicle(1998, "Toyota", "Rav-4", 200000, 3500);

    List<Vehicle> allVehicles = new ArrayList<Vehicle>();
    allVehicles.add(hatchback);
    allVehicles.add(suv);
    allVehicles.add(sedan);
    allVehicles.add(truck);
    allVehicles.add(crossover);

    while(programRunning) {
      System.out.println("Welcome to our car dealership. What would you like to do? Enter one of the following options: All Vehicles, Search Price, Add Vehicle, or Exit");
      String navigationChoice = myConsole.readLine();

      if (navigationChoice.equals("All Vehicles")) {
        for ( Vehicle individualVehicle : allVehicles ) {
            System.out.println( "----------------------" );
            System.out.println( individualVehicle.mYear );
            System.out.println( individualVehicle.mBrand );
            System.out.println( individualVehicle.mModel );
            System.out.println( individualVehicle.mMiles );
            System.out.println( individualVehicle.getPrice(); );
        }
      } else if (navigationChoice.equals("Search Price")) {
        System.out.println("What is your maximum budget for a vehicle?");
        String stringUserMaxBudget = myConsole.readLine();
        int userMaxBudget = Integer.parseInt(stringUserMaxBudget);

        System.out.println("Alright, here's what we have in your price range:");

        for ( Vehicle individualVehicle : allVehicles ) {
          if (individualVehicle.worthBuying(userMaxBudget)){
            System.out.println( "----------------------" );
            System.out.println( individualVehicle.mYear );
            System.out.println( individualVehicle.mBrand );
            System.out.println( individualVehicle.mModel );
            System.out.println( individualVehicle.mMiles );
            System.out.println( individualVehicle.getPrice(); );
          }
        }
      } else if (navigationChoice.equals("Add Vehicle")){
          System.out.println("Alright, let's add a vehicle! What year was this vehicle made?");
          int userVehicleYear = Integer.parseInt(myConsole.readLine());
          System.out.println("Great! What make or brand is the vehicle?");
          String userVehicleBrand = myConsole.readLine();
          System.out.println("Got it! What model is it?");
          String userVehicleModel = myConsole.readLine();
          System.out.println("And how many miles does it have on it?");
          int userVehicleMiles = Integer.parseInt(myConsole.readLine());
          System.out.println("Finally, what's its price?");
          int userVehiclePrice = Integer.parseInt(myConsole.readLine());
          Vehicle userVehicle = new Vehicle(userVehicleYear, userVehicleBrand, userVehicleModel, userVehicleMiles, userVehiclePrice);
          allVehicles.add(userVehicle);
          System.out.println("Alright, here's your new vehicle:");
          System.out.println( "----------------------" );
          System.out.println( userVehicle.mYear );
          System.out.println( userVehicle.mBrand );
          System.out.println( userVehicle.mModel );
          System.out.println( userVehicle.mMiles );
          System.out.println( userVehicle.getPrice(); );
      } else if (navigationChoice.equals("Exit")){
        programRunning = false;
      } else {
        System.out.println("I'm sorry, we don't recognize your input");
      }
    }
  }
}