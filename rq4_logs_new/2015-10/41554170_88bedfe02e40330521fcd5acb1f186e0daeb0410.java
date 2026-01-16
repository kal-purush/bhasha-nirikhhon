////////////////////////////////////////////////////
//Madison Licht
//10/19/15
//CSE2
//Lab 08
//Area
//
//   first compile the program
//      javac Area.java
//   run the program
//      java Area//

import java.util.Scanner;
//   define a class
public class Area {
    //   add main method
    public static void main(String[] args) {
            Scanner myScanner = new Scanner(System.in);

            //ask for user to enter the type of shape
            String triangle = "triangle";
            String square = "square";
            String circle = "circle";
            boolean acceptable = false;
            double baseLength = 0.0;
            double triHeight = 0.0;
            double squareLength = 0.0;
            double radius = 0.0;
            double area = 0.0;

            System.out.print("Enter the shape: ");
            String shape = myScanner.next();

            //enter the height and base length if the shape is a triangle
            boolean okay = false;
            while (!okay) {
                if (shape.equals(triangle)) {
                    baseLength = checkInput (myScanner);
                    triHeight = checkInput (myScanner);
                    okay = true;
                    area = findTriArea(baseLength, triHeight);
                }
                else if (shape.equals(square)) {
                    squareLength = checkInput (myScanner);
                    okay = true;
                    area = findSqArea(squareLength);
                }
                else if (shape.equals(circle)) {
                    radius = checkInput (myScanner);
                    okay = true;
                    area = findCirArea(radius);
                }
                else{
                    System.out.println("ERROR! You need a triangle, square, or circle.");
                    System.out.print("Enter the shape: ");
                }
            }
            printInfo(area);
        }
        //method to calculate the area for a triangle
    public static double findTriArea(double baseLength, double triHeight) {
            double areaTri = .5 * baseLength * triHeight;
            return areaTri;
        }
        //method to calculate the area for a square
    public static double findSqArea(double squareLength) {
            double areaSq = Math.pow(squareLength, 2);
            return areaSq;
        }
        //method to calculate the area for a circle
    public static double findCirArea(double radius) {
            double areaCir = Math.PI * Math.pow(radius, 2);
            return areaCir;
        }
        //method to print result
    public static void printInfo(double area) {
        System.out.println("The area is " + area);

    }
    public static double checkInput(Scanner input) {
        boolean acceptable = false;
        double dataInput = 0.0;
        System.out.print("Enter a double: ");
        while (!acceptable) {
            if (input.hasNextDouble()) {
                dataInput = input.nextDouble();
                acceptable = true;
            }
            else {
                System.out.println("ERROR: need an double");
                System.out.print("Enter a double: ");
                dataInput = input.nextDouble();
            }
        }
        return dataInput;
    }
}