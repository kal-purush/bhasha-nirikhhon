package Ch1ProgrammingProjects;

/**
 * Created by jjenkins on 8/24/2016.
 * Chapter 1 programming project 2
 *(Average speed in kilometers) Assume a runner runs 24 miles in 1 hour, 40 minutes, and 35 seconds.
 *Write a program that displays the average speed in kilometers per hour.
 *(Note that 1 mile is 1.6 kilometers)
 */
public class Project2 {
    public static void main(String[] args) {
        int miles = 24;
        double Kilometers = miles * 1.6;//miles converted to kilometers
        double timeInMinutes = 1.0 * 60.0 + 40.0 + 35.0 / 60.0; // convert time to minutes

        //convert kilometers per hour formula
        double kilometersPerHour = 60.0 * Kilometers / timeInMinutes;

        System.out.printf("%.14f%n",kilometersPerHour);//formatted to get rid of final number to match books input


    }
}