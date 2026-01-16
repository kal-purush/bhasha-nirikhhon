package models;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Admin extends Personne {
    private int idAdmin;

    // Constructeur
    public Admin(String nom, String prenom, String email, String numeroDeTelephone, String dateDeNaissance, String motDePasse, int idAdmin) {
        super(nom, prenom, email, numeroDeTelephone, dateDeNaissance, motDePasse);
        this.idAdmin = idAdmin;
    }

    // Getter et Setter spécifique à Admin
    public int getIdAdmin() {
        return idAdmin;
    }

    public void setIdAdmin(int idAdmin) {
        this.idAdmin = idAdmin;
    }


    public void ajouterCompagnie(CompagnieAerienne compagnie) throws SQLException {
        String ajoutComp = "INSERT INTO Compagnie (nomCompagnie, motDePasse, siteWeb, idAdmin) VALUES (?, ?, ?, ?)";
        try (Connection connection = Connexion.con;
            PreparedStatement statement = connection.prepareStatement(ajoutComp)) {
            statement.setString(1, compagnie.getNomCompagnie());
            statement.setString(3, compagnie.getMotDePasse());
            statement.setString(4, compagnie.getSiteWeb());
            statement.setInt(5, this.getIdAdmin()); // Utilise l'ID de l'admin courant
            statement.executeUpdate();
        }
    }

    public void supprimerCompagnie(int idCompagnie) throws SQLException {
        String supprComp = "DELETE FROM Compagnie WHERE idCompagnie = ?";
        try (Connection connection = Connexion.con;
            PreparedStatement statement = connection.prepareStatement(supprComp)) {
            statement.setInt(1, idCompagnie);
            statement.executeUpdate();
        }
    }

    public void modifierCompagnie(CompagnieAerienne compagnie) throws SQLException {
        String modifComp = "UPDATE Compagnie SET nomCompagnie = ?, motDePasse = ?, siteWeb = ? WHERE idCompagnie = ?";
        try (Connection connection = Connexion.con;
            PreparedStatement statement = connection.prepareStatement(modifComp)) {
            statement.setString(1, compagnie.getNomCompagnie());
            statement.setString(3, compagnie.getMotDePasse());
            statement.setString(4, compagnie.getSiteWeb());
            statement.setInt(5, compagnie.getIdAdmin());
            statement.executeUpdate();
        }
    }

    public boolean seConnecter(String email, String motDePasse) {
        String selectAdminQuery = "SELECT idPersonne FROM Personne WHERE email = ? AND motDePasse = ?";
        try (Connection connection = Connexion.con;
            PreparedStatement statement = connection.prepareStatement(selectAdminQuery)) {
            // Paramètres pour la requête SELECT
            statement.setString(1, email);
            statement.setString(2, motDePasse);

            // Exécution de la requête SELECT
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    int idPersonne = resultSet.getInt("idAdmin");
                    System.out.println("Vous êtes connecté en tant qu'administrateur (ID : " + idPersonne + ")");
                    return true; // Connexion réussie
                } else {
                    System.out.println("Identifiants incorrects. Connexion échouée.");
                    return false; // Connexion échouée (identifiants incorrects)
                }
            }
        } catch (SQLException e) {
            System.err.println("Erreur lors de la connexion : " + e.getMessage());
            return false; // Connexion échouée en raison d'une exception SQL
        }
    }
}