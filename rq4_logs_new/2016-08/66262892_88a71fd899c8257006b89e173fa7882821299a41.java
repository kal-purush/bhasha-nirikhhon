package Chapter2ProgrammingProjects;

import java.util.Scanner;


 /*****************************
 * COSC 1173 Programming Lab
 * Name: John Jenkins
 * Data: 8/30/2016
 * Console Input/Output and Computation
 *******************************/
 /** Created by jjenkins on 8/30/2016.
 * Chapter 3 programming project 3
 * Write a java program that reads in the radius and length of a cylinder and computes the area and the
 *volume using the following formulas:
 *Area = radius * radius * pi
 *Volume = area * length
 */
public class ConsoleInputOutput {
    public static void main(String[] args) {

        //create a Scanner object
        Scanner input = new Scanner(System.in);

        // get the user input for the radius
        System.out.print("Enter the radius of a cylinder: ");
        double radius = input.nextDouble();

        // get the user input for the length
        System.out.print("Enter the length of a cylinder: ");
        int length = input.nextInt();

        computeVolume(computeArea(radius),length);


    }

     public static double computeArea (double rad){

         double area = rad * rad * Math.PI;

         System.out.println("The area is: " + (int)area*1000/1000.0);

         return area;


     }

     public static void computeVolume (double area , double length){

         double volume = area * length;

         System.out.print("The volume is: " + (int)volume*1000/1000.0);


     }
}