package models;

import java.sql.Connection;
import java.util.Scanner;

public class ModePaiement {

    public static void AjouterModePaiement(Connection connection, Scanner scanner) {
        System.out.println("1:OM , 2:CArte Bancaire : ");
        int idModepaiement = scanner.nextInt();

        if (idModepaiement == 1) {
            System.out.println("Numéro de téléphone OM : ");

            String tel = scanner.nextLine();

            // Traitement pour le mode de paiement OM
        } else if (idModepaiement == 2) {
            String numeroCarte; // Déclarer la variable en dehors de la boucle
            do {
                System.out.print("Entrez votre numéro de carte bancaire : ");
                numeroCarte = scanner.next();

                if (numeroCarte.length() != 14) {
                    System.out.println("Erreur : Le numéro de carte bancaire doit comporter 14 chiffres.");
                }
            } while (numeroCarte.length() != 14);

            System.out.println("Numéro de carte bancaire valide : " + numeroCarte);
            // Traitement pour le mode de paiement par carte bancaire
        } else {
            System.out.println("Mode de paiement invalide");
        }



    }




}