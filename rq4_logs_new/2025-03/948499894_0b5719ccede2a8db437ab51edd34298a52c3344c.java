package main;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.*;

public class GestionLogin {
    private static final String FICHIER_BASE_DONNEES = "base_de_donnees.txt";
    /** Expression régulière pour valider un email*/
    private static final String EMAIL_REGEX = "^[a-zA-Z0-9_+&*-]+(?:\\.[a-zA-Z0-9_+&*-]+)*@(?:[a-zA-Z0-9-]+\\.)+[a-zA-Z]{2,7}$";
    private static final Pattern EMAIL_PATTERN = Pattern.compile(EMAIL_REGEX);

    public GestionLogin() {
        initialiserBaseDeDonnees();
    }

    private void initialiserBaseDeDonnees() {
        File fichier = new File(FICHIER_BASE_DONNEES);
        if (!fichier.exists()) {
            try (PrintWriter writer = new PrintWriter(new FileWriter(fichier))) {
                writer.println("admin:admin123:Admin:admin@example.com");
                writer.println("user:user123:User:user@example.com");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /** Méthode pour valider le format de l'email*/
    public boolean validerEmail(String email) {
        if (email == null) {
            return false;
        }
        Matcher matcher = EMAIL_PATTERN.matcher(email);
        return matcher.matches();
    }

    /** Méthode pour ajouter un utilisateur*/
    public void ajouterUtilisateur(String utilisateur, String motDePasse, String nomComplet, String email) {
        if (!validerEmail(email)) {
            throw new IllegalArgumentException("Format d'email invalide.");
        }
        try (PrintWriter writer = new PrintWriter(new FileWriter(FICHIER_BASE_DONNEES, true))) {
            writer.println(utilisateur + ":" + motDePasse + ":" + nomComplet + ":" + email);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /** Méthode pour vérifier le login*/
    public boolean verifierLogin(String utilisateur, String motDePasse) {
        Map<String, String> baseDeDonnees = chargerBaseDeDonnees();
        return baseDeDonnees.containsKey(utilisateur) && baseDeDonnees.get(utilisateur).equals(motDePasse);
    }

    /** Méthode pour charger la base de données*/
    private Map<String, String> chargerBaseDeDonnees() {
        Map<String, String> baseDeDonnees = new HashMap<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(FICHIER_BASE_DONNEES))) {
            String ligne;
            while ((ligne = reader.readLine()) != null) {
                String[] parts = ligne.split(":");
                if (parts.length >= 2) {
                    baseDeDonnees.put(parts[0], parts[1]); /** Utilisateur et mot de passe seulement*/
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return baseDeDonnees;
    }
}