package Problem_analysis;

import java.util.*;

public class for_aly {
    public static void main(String[] args) {
        Scanner in = new Scanner(System.in);
        System.out.print("\n\n");

        System.out.print("Enter package weight (in kg; max = 20 kg): ");// max is only 20 kg
        double weight = in.nextDouble();

        System.out.print("Enter package type (A|B): ");// package type A or B
        char type = in.next().charAt(0);

        System.out.print("Enter distance (in km): ");// 1 km and above
        double distance = in.nextDouble();

        double shippingDistance = 0;// will be used repetitively so need to initialize to 0
        double charge = 0;// will be used repetitively so need to initialize to 0

        if (distance > 0 && distance <= 500) {
            shippingDistance = 1.0;// 500 or less is 1 shipping distance
            if (type == 'A' || type == 'a') {
                System.out.print("\nPackage type: A");
                if (weight > 0 && weight <= 5) {
                    charge = weight * 10.50 * shippingDistance;// rate = P10.5 per kg and shipping distance
                    System.out.printf("%nCharge: P %.3f", charge);
                }
                if (weight > 5 && weight <= 10) {
                    charge = weight * 15.75 * shippingDistance;// rate = P15.75 per kg and shipping distance
                    System.out.printf("%nCharge: P %.3f", charge);
                }
                if (weight > 10 && weight <= 20) {
                    charge = weight * 20.65 * shippingDistance;// rate = P20.65 per kg and shipping distance
                    System.out.printf("%nCharge: P %.3f", charge);
                }
            }
            if (type == 'B' || type == 'b') {
                System.out.print("\nPackage type: B");
                if (weight > 0 && weight <= 5) {
                    charge = weight * 11.75 * shippingDistance;// rate = P11.75 per kg and shipping distance
                    System.out.printf("%nCharge: P %.3f", charge);
                }
                if (weight > 5 && weight <= 10) {
                    charge = weight * 18.50 * shippingDistance;// rate = P18.5 per kg and shipping distance
                    System.out.printf("%nCharge: P %.3f", charge);
                }
                if (weight > 10 && weight <= 20) {
                    charge = weight * 23.65 * shippingDistance;// rate = P23.65 per kg and shipping distance
                    System.out.printf("%nCharge: P %.3f", charge);
                }
            }
        }
        if (distance > 500)// rate in given shows 500 km and under
        {
            shippingDistance = distance / 500.0;
            if (type == 'A' || type == 'a') {
                System.out.print("\nPackage type: A");
                if (weight > 0 && weight <= 5) {
                    charge = weight * 10.50 * shippingDistance;// rate = P10.5 per kg and shipping distance
                    System.out.printf("%nCharge: P %.3f", charge);
                }
                if (weight > 5 && weight <= 10) {
                    charge = weight * 15.75 * shippingDistance;// rate = P15.75 per kg and shipping distance
                    System.out.printf("%nCharge: P %.3f", charge);
                }
                if (weight > 10 && weight <= 20) {
                    charge = weight * 20.65 * shippingDistance;// rate = P20.65 per kg and shipping distance
                    System.out.printf("%nCharge: P %.3f", charge);
                }
            }
            if (type == 'B' || type == 'b') {
                System.out.print("\nPackage type: B");
                if (weight > 0 && weight <= 5) {
                    charge = weight * 11.75 * shippingDistance;// rate = P11.75 per kg and shipping distance
                    System.out.printf("%nCharge: P %.3f", charge);
                }
                if (weight > 5 && weight <= 10) 
                {
                    charge = weight * 18.50 * shippingDistance;// rate = P18.5 per kg and shipping distance
                    System.out.printf("%nCharge: P %.3f", charge);
                }
                if (weight > 10 && weight <= 20) {
                    charge = weight * 23.65 * shippingDistance;// rate = P23.65 per kg and shipping distance
                    System.out.printf("%nCharge: P %.3f", charge);
                }
            }
        }

        if ((weight > 0 && weight <= 20) && ((type == 'A' || type == 'a')|| (type == 'B' || type == 'b')) && (distance > 0)) {
            double totalCharge = charge * 1.10;// charge * 10% eVAT
            System.out.printf("%nTotal charge: P %.3f", totalCharge);
        }

        if (!(weight > 0 && weight <= 20))
            System.out.println("\nInvalid package weight");
        if (type != 'A' && type != 'a' && type != 'B' && type != 'b')
            System.out.println("\nInvalid package type");
        if (distance < 1)// 0 km is considered "not travelled"
            System.out.println("\nInvalid distance");
        if (!(weight > 0 || weight <= 20 && type == 'A' || type == 'a' || type == 'B' || type == 'b' || distance > 0))
            System.out.println("\nInvalid total charge");

        in.close();
        System.out.print("\n\n");
    }
}