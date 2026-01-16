package models;

import models.Escale;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class VolEscale {

    public static void ajouterVolEscale(Connection connection, Scanner scanner) {
        System.out.println("Veuillez saisir les informations de vol :");

        try {
            System.out.println("Les avions disponibles : ");
            Avion.listeDAvion(Connexion.con);
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.print("Immatriculation de l'avion : ");
        String imma = scanner.nextLine();

        System.out.print("Ville de départ : ");
        String villeDepart = scanner.nextLine();

        System.out.print("Ville d'arrivée : ");
        String villeArrivee = scanner.nextLine();

        System.out.print("Entrez la date de départ au format (dd-MM-yyyy) : ");
        String dateDepart = util.Date();

        System.out.print("Entrez la date d'arrivée au format (dd-MM-yyyy) : ");
        String dateArrivee = util.Date();

        int nbr_de_escale = 0;
        boolean isInt;

        do {
            System.out.print("Nombre d'escale : ");
            if (scanner.hasNextInt()) {
                nbr_de_escale = scanner.nextInt();
                isInt = true;
            } else {
                System.out.println("Veuillez saisir un entier.");
                scanner.next(); // Pour vider le scanner et éviter une boucle infinie
                isInt = false;
            }
        } while (!isInt);

        scanner.nextLine(); // Consommer la fin de ligne

        System.out.print("Tarif : ");
        int tarif = scanner.nextInt();
        scanner.nextLine(); // Consommer la fin de ligne

        String sqlVol = "INSERT INTO vol (immatriculation, villeDeDepart, villeDArrive, dateDeDepart, dateDArrive, nombreDEscale, tarif) VALUES (?, ?, ?, ?, ?, ?, ?)";
        int idVol = -1;
        try (PreparedStatement statementVol = connection.prepareStatement(sqlVol, PreparedStatement.RETURN_GENERATED_KEYS)) {
            statementVol.setString(1, imma);
            statementVol.setString(2, villeDepart);
            statementVol.setString(3, villeArrivee);
            statementVol.setString(4, dateDepart);
            statementVol.setString(5, dateArrivee);
            statementVol.setInt(6, nbr_de_escale);
            statementVol.setInt(7, tarif);

            // Exécutez la requête pour ajouter le vol
            int lignesModifiees = statementVol.executeUpdate();
            if (lignesModifiees > 0) {
                System.out.println("Les données du vol ont été ajoutées avec succès !");
                try (ResultSet generatedKeys = statementVol.getGeneratedKeys()) {
                    if (generatedKeys.next()) {
                        idVol = generatedKeys.getInt(1);
                    } else {
                        throw new SQLException("Échec de la création du vol, aucun ID obtenu.");
                    }
                }
            } else {
                System.out.println("Erreur lors de l'ajout des données du vol.");
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
            return;
        }

        try {
            Ville.afficherToutesLesVilles();
        } catch (Exception e) {
            e.printStackTrace();
        }

        List<Escale> escalesList = new ArrayList<>();
        for (int i = 0; i < nbr_de_escale; i++) {
            int idVille = 0;
            do {
                System.out.println("Id Ville : ");
                if (scanner.hasNextInt()) {
                    idVille = scanner.nextInt();
                    isInt = true;
                } else {
                    System.out.println("Veuillez saisir un entier.");
                    scanner.next(); // Pour vider le scanner et éviter une boucle infinie
                    isInt = false;
                }
            } while (!isInt);


            scanner.nextLine();

            System.out.println("Date et Heure : ");
            String dateHeure = util.Date();

            Escale escale = new Escale(idVille, idVol, imma, dateHeure);
            escalesList.add(escale);
        }

        // Ajout des escales
        String sqlEscale = "INSERT INTO escale (idVille, idVol, immatriculation, dateEtHeure) VALUES (?, ?, ?, ?)";
        try (PreparedStatement statementEscale = connection.prepareStatement(sqlEscale)) {
            for (Escale escale : escalesList) {
                statementEscale.setInt(1, escale.getIdVille());
                statementEscale.setInt(2, escale.getIdVol());
                statementEscale.setString(3, escale.getImmatriculation());
                statementEscale.setString(4, escale.getDateHeure());
                statementEscale.executeUpdate();
            }
            System.out.println("Les données des escales ont été ajoutées avec succès !");
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }
}