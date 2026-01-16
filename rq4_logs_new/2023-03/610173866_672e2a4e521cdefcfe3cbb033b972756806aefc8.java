package Video21ZnacenjeStatic_VezbanjeZadataka.Vezbanje;

import java.util.Scanner;

public class DrugiSaTest {
    public static void main(String[] args) {


        /*
        Izracunati potrosnju goriva
        distanca, tip (tip se unosi)
        motor: 5l / 100 km
        auto: 7l / 100 km
        kombi: 11l / 100 km

        preko SWITCHA
         */

        Scanner sc = new Scanner(System.in);
        double distanca = sc.nextDouble();
        String tip = sc.next();
        double potrosnja; // distanca / 100 km * potrosnja
        switch (tip){
            case "motor":
                potrosnja = (distanca / 100) * 5;
                System.out.println(potrosnja);
                break;
            case "auto":
                potrosnja = (distanca / 100) * 7;
                System.out.println(potrosnja);
                break;
            case "kombi":
                potrosnja = (distanca / 100) * 11;
                System.out.println(potrosnja);
                break;
            default:
                System.out.println("Niste uneli validan tip vozila.");
                break;
        }

    }
}