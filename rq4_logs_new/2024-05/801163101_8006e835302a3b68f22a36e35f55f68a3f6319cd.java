package models;

import java.util.Scanner;

import org.mindrot.jbcrypt.BCrypt;

public class util {
    //Methode pour obtenir la date sous format AAAA-MM-JJ
    public static String Date(){
        Scanner entree = new Scanner(System.in);
        //Jour de naissance
        boolean isIntJour;
        int jourNaissance = 0;
        do {
            System.out.println("Veuillez donner le jour :");
            if (entree.hasNextInt()) {
                jourNaissance = entree.nextInt();
                isIntJour = true;
                if (jourNaissance > 31) {
                    System.out.println("Veuillez donner un nombre inférieur ou égal à 31");
                    isIntJour = false;
                }
            } else {
                System.out.println("Veuillez donner un entier");
                entree.next(); // Consomme l'entrée invalide pour éviter une boucle infinie
                isIntJour = false;
            }
        } while (!isIntJour);
        //Mois de naissance
        boolean isIntMois;
        int moisNaissance = 0;
        do {
            System.out.println("Veuillez donner le mois :");
            if (entree.hasNextInt()) {
                moisNaissance = entree.nextInt();
                isIntMois = true;
                if (moisNaissance > 12) {
                    System.out.println("Veuillez donner un nombre inférieur ou égal à 12");
                    isIntMois = false;
                }
            } else {
                System.out.println("Veuillez donner un entier");
                entree.next(); // Consomme l'entrée invalide pour éviter une boucle infinie
                isIntMois = false;
            }
        } while (!isIntMois);
        //Annee de naissance
        boolean isIntAnnee;
        int anneeNaissance = 0;
        do {
            System.out.println("Veuillez donner l'année :");
            if (entree.hasNextInt()) {
                anneeNaissance = entree.nextInt();
                isIntAnnee = true;
                if (anneeNaissance > 2024) {
                    System.out.println("Veuillez donner un nombre inférieur ou égal à 2024");
                    isIntAnnee = false;
                }
            } else {
                System.out.println("Veuillez donner un entier");
                entree.next(); // Consomme l'entrée invalide pour éviter une boucle infinie
                isIntAnnee = false;
            }
        } while (!isIntAnnee);
        //Fusion Date
        return anneeNaissance+"-"+moisNaissance+"-"+jourNaissance;
    }

    // Methode pour hasher le mote de passe
    public static String hasherMotDePasse(){
        Scanner entree = new Scanner(System.in);
        String motDePasse = entree.nextLine();
        return BCrypt.hashpw(motDePasse, BCrypt.gensalt());
    }


}