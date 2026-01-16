package gui3_app;

import java.awt.*;
import javax.swing.*;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class Calculator {

    public Calculator() {
        // Create a frame
        // JFrame frame = new JFrame("Graph");

        // Create an instance of GraphDrawer
        this.graphDrawer = new GraphDrawer();

        // Add the graphDrawer to the frame
        /*
         * frame.getContentPane().add(graphDrawer, BorderLayout.CENTER);
         * 
         * // Set frame properties
         * frame.setSize(800, 600);
         * frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
         * frame.setVisible(true);
         */
    }

    private GraphDrawer graphDrawer;
    private float grader_ = 0;
    private float gradtimmar_ = 0;

    public float getGrader() {
        return grader_;
    }

    public float getGradtimmar() {
        return gradtimmar_;
    }

    // Method to perform addition
    public int add(int num1, int num2, int num3) {
        int sum = 0;
        for (int i = 0; i < num1; i++) {
            sum = sum + i;
        }
        return sum;
    }

    // Method to perform subtraction
    public int subtract(int num1, int num2) {
        return num1 - num2;
    }

    // Method to perform multiplication
    public int multiply(int num1, int num2) {
        return num1 * num2;
    }

    // Method to perform division
    public int divide(int num1, int num2) {
        return num1 / num2;
    }

    // Method to print result to file
    public void generateGraph(float temp) {

        float gradtimmar = 0;
        float grader;
        try (BufferedWriter writer = new BufferedWriter(new FileWriter("tempTable.txt"))) {
            for (int timmar = 0; timmar < 8760; timmar++) {
                grader = calculateHours(timmar, temp);
                gradtimmar = gradtimmar + grader;
                gradtimmar_ = gradtimmar;
                // Write the results to a text file
                writer.write("For timmar = " + timmar + ", grader = " + grader + "\n");
                graphDrawer.addDataPoint(timmar, grader);
            }
        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }

        // Create a frame
        JFrame frame = new JFrame("Graph");

        // Create an instance of GraphDrawer
        GraphDrawer graphDrawer = new GraphDrawer();

        // Add the graphDrawer to the frame
        frame.getContentPane().add(graphDrawer, BorderLayout.CENTER);

        // Set frame properties
        frame.setSize(800, 600);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.setVisible(true);
    }

    // Method to do a calculation
    /*
     * public float calculateHours(int timmar, float temp) {
     * float grader = (float) ((timmar - 4380) * (3.9 - 0.086 * temp) * 0.001 +
     * temp * Math.pow(timmar * (1 + (8 - temp) / 586) / 8300, 38) -
     * Math.pow(1550 / (700 + timmar), 3) +
     * 1.5 * (temp * 1200 / ((500 + timmar) * 8)) * Math.pow(Math.cos((900 - timmar)
     * / 585), 2));
     * grader_ = grader;
     * return grader;
     * }
     */

    public float calculateHours(int timmar, float temp) {
        float grader = (float) ((timmar - 4380) * (3.9 - 0.086 * temp) * 0.001 +
                temp * Math.pow(timmar * (1 + (8 - temp) / 586.0) / 8300.0, 38) -
                Math.pow(1550.0 / (700.0 + timmar), 3) +
                1.5 * (temp * 1200.0 / ((500.0 + timmar) * 8)) * Math.pow(Math.cos((900.0 - timmar) / 585.0), 2));
        grader_ = grader;
        return grader;
    }

}

/*
 * package gui3_app;
 * import java.awt.Graphics2D;
 * import java.io.BufferedWriter;
 * import java.io.FileWriter;
 * import java.io.IOException;
 * 
 * //import gui3_app.*;
 * 
 * 
 * 
 * public class Calculator {
 * 
 * Graphics2D g2;
 * 
 * private float grader_ = 0;
 * private float gradtimmar_ = 0;
 * 
 * public float getGrader() {
 * return grader_;
 * }
 * public float getGradtimmar() {
 * return gradtimmar_;
 * }
 * 
 * // Method to perform addition
 * public int add(int num1, int num2, int num3) {
 * int sum = 0;
 * for (int i = 0; i < num1; i++) {
 * sum = sum + i;
 * }
 * return sum;
 * }
 * 
 * // Method to perform subtraction
 * public int subtract(int num1, int num2) {
 * return num1 - num2;
 * }
 * 
 * // Method to perform multiplication
 * public int multiply(int num1, int num2) {
 * return num1 * num2;
 * }
 * 
 * // Method to perform division
 * public int divide(int num1, int num2) {
 * return num1 / num2;
 * }
 * 
 * // Method to print result to file
 * public void GenerateGraph (float temp) {
 * float gradtimmar = 0;
 * float grader;
 * try (BufferedWriter writer = new BufferedWriter(new
 * FileWriter("tempTable.txt"))) {
 * for (int timmar = 0; timmar < 8760; timmar++) {
 * grader = calculateHours(timmar, temp);
 * gradtimmar = gradtimmar + grader;
 * gradtimmar_ = gradtimmar;
 * // Write the reults to a text file
 * writer.write("For timmar = " + timmar + ", grader = " + grader + "\n");
 * }
 * } catch (IOException e) {
 * System.out.println("An error occurred.");
 * e.printStackTrace();
 * }
 * }
 * 
 * // Method to do a calulation
 * public float calculateHours(int timmar, float temp) {
 * float grader = (float)((timmar - 4380) * (3.9 - 0.086 * temp) * 0.001 +
 * temp * Math.pow(timmar * (1 + (8 - temp) / 586) / 8300, 38) -
 * Math.pow(1550 / (700 + timmar), 3) +
 * 1.5 * (temp * 1200 / ((500 + timmar) * 8)) * Math.pow(Math.cos((900 - timmar)
 * / 585), 2));
 * grader_ = grader;
 * return grader;
 * }
 * 
 * }
 * // Print or store the result for each value of timmar
 * //System.out.println("For timmar = " + timmar + ", grader = " + grader);
 * 
 */