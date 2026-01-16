/*
By: Shahmeir Shaikh
Date: February 28th 2024
Description: This is a very simple calculator that takes 2 inputs only and provides an appropriate output.
 */
import java.util.Scanner;
public class SimpleCalculator {
    private Scanner scanner =  new Scanner(System.in);
    private float a = 0;
    private float b = 0;
    private float output;
    float aInput;
    float bInput;
    String userInput;
    public SimpleCalculator(){
        System.out.println("Add\nSubtract\nMultiply\nDivide\nPercentage\n Please enter the operation you want to perform: ");
        userInput = scanner.nextLine().trim();
    }
    public void Add(){
        System.out.print("Please enter your first number you would like to add: ");
        aInput = scanner.nextFloat();
        System.out.print("Please enter your second number you would like to add: ");
        bInput = scanner.nextFloat();

        output = aInput + bInput;
        System.out.println("Result: "+output);
    }
    public void Multiply(){
        System.out.print("Please enter your first number you would like to Multiply: ");
        aInput = scanner.nextFloat();
        System.out.print("Please enter your second number you would like to Multiply: ");
        bInput = scanner.nextFloat();

        output = aInput * bInput;
        System.out.println("Result: "+output);
    }
    public void Divide(){
        System.out.print("Please enter your first number you would like to Divide: ");
        aInput = scanner.nextFloat();
        System.out.print("Please enter your second number you would like to Divide: ");
        bInput = scanner.nextFloat();

        output = aInput / bInput;
        System.out.println("Result: "+output);
    }
    public void Precentage(){
        System.out.print("Please enter your first number you would like to get a Precentage: ");
        aInput = scanner.nextFloat();
        System.out.print("Please enter your second number you would like to get a Precentage: ");
        bInput = scanner.nextFloat();

        output = (aInput * bInput)/100;
        System.out.println("Result: "+output);
    }
    public void Subtract(){
        System.out.print("Please enter your first number you would like to Subtract: ");
        aInput = scanner.nextFloat();
        System.out.print("Please enter your second number you would like to Subtract: ");
        bInput = scanner.nextFloat();

        output = aInput - bInput;
        System.out.println("Result: "+output);
    }


    public static void main(String[] args){

while (true) {


    SimpleCalculator calculator = new SimpleCalculator();
    if (calculator.userInput.equalsIgnoreCase("add")) {
        calculator.Add();
        break;

    } else if (calculator.userInput.equalsIgnoreCase("subtract")) {
        calculator.Subtract();
        break;

    } else if (calculator.userInput.equalsIgnoreCase("multiply")) {
        calculator.Multiply();
        break;

    } else if (calculator.userInput.equalsIgnoreCase("divide")) {
        calculator.Divide();
        break;

    } else if (calculator.userInput.equalsIgnoreCase("percentage")) {
        calculator.Precentage();
        break;

    } else {
        System.out.println("Invalid, Please Enter A Valid Option");
        System.out.println("Please try again");
    }


}

    }
}