/*
 * This Class is all car settings for the 3 cars that the user is prompted to 
 * enter in the CarTester.java client program
 */
package cartester;
/**
 *
 * @author Jorda
 */
public class Car {

    
    
    
    // A constructor that sets all defualts of the car if the user's input contains nothing
    public static void CarDefualts(int year, double price, int horsepower, double weight, String make, String model){
        make = "Ford";
        model = "Mustang";
        year = 2019;
        price = 63000;
        horsepower = 550;
        weight = 3705;
    } 
    // this is the assining constructor for the first car, this takes all inputs and stores them to be used later
    public static void Car1Characteristics(String make, String model){
        String []Car1 = {make, model};
    }
    public static void Car2Characteristics(String make1, String model1){
        
    }
    
}
