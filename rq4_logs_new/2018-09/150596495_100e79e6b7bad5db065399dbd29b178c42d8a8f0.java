import java.util.Scanner;

public class Main {

    public static void main(String[] args) {
        String continueLoop = "ne";
        Scanner keyboard = new Scanner(System.in);
        System.out.println("Įveskite masyvo dydį: ");
        int masyvoDydis = keyboard.nextInt();
        int [] skaiciai = new int[masyvoDydis];

        for (int i = 0; i < skaiciai.length;i++){
            System.out.println("Įveskite skaičių: ");
            skaiciai [i] = keyboard.nextInt();
        }
            /*System.out.println("Ar norite tęsti skaičių įvedimą?");
            continueLoop = keyboard.next();
            if (continueLoop.equalsIgnoreCase("taip")){
                continueLoop = "taip";
            } else
                break;
            */
        grazintiMasyvoNarius(skaiciai);
        grazintiMasyvoDydi(skaiciai);
        sudetiMasyvoNarius(skaiciai);
    }

    public static void grazintiMasyvoNarius (int skaiciai[]) {
        System.out.println("Grąžinti masyvo narius: ");
        for (int i=0;i<skaiciai.length;i++){
            System.out.println("Nr." + (i + 1) + " reikšmė = " + skaiciai[i]);
        }
    }
    public static void grazintiMasyvoDydi (int skaiciai[]) {
        System.out.println("Skaičių kiekis masyve: " + skaiciai.length);
    }
    public static void sudetiMasyvoNarius (int skaiciai[]) {
        int sum = 0;
        for (int i = 0; i < skaiciai.length; i++) {
            sum = sum + skaiciai[i];
        }
        System.out.println("Masyvo narių suma: " + sum);
    }
}