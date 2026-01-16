package app.repositories;

import app.Database;
import app.entities.Livre;

import java.sql.*;
import java.util.ArrayList;

public class LivreRepository {

    Database db  = new Database();

    public Livre ajouter(Livre livre) throws SQLException {
        String sql = "INSERT INTO livre(auteur, titre, isbn, status, quantite) VALUES(?,?,?,?,?)";

        try(Connection connection = db.getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement(sql)){
            preparedStatement.setString(1, livre.getTitre());
            preparedStatement.setString(2, livre.getAuteur());
            preparedStatement.setInt(3, livre.getIsbn());
            preparedStatement.setInt(4, livre.getStatus());
            preparedStatement.setInt(5, livre.getQuantite());

          if(preparedStatement.executeUpdate() > 0){
              System.out.println("Record created successfully .");
          }else {
              System.out.println("Insert record failed ....");
          }
        }
        return livre;
    }

    public Livre modifier(Livre livre) throws SQLException {
        String sql = "UPDATE livre SET auteur = ?, titre = ?, isbn = ?, status = ?, quantite = ?  WHERE id = ? ";

        try(Connection connection = db.getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement(sql)){
            preparedStatement.setString(1, livre.getAuteur());
            preparedStatement.setString(2, livre.getTitre());
            preparedStatement.setInt(3, livre.getIsbn());
            preparedStatement.setInt(4, livre.getStatus());
            preparedStatement.setInt(1, livre.getQuantite());
        }
        return livre;
    }

    public boolean supprimer(Livre livre) throws SQLException {
        String sql = "INSERT INTO livre(auteur, titre, isbn, status, quantite) VALUES(?,?,?,?,?)";

        try(Connection connection = db.getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement(sql)){
            preparedStatement.setString(1, livre.getAuteur());
            preparedStatement.setString(2, livre.getTitre());
            preparedStatement.setInt(3, livre.getIsbn());
            preparedStatement.setInt(4, livre.getStatus());
            preparedStatement.setInt(1, livre.getQuantite());
        }
        return true;
    }

    public Livre rechercher(String slag) throws SQLException {
        String sql = "INSERT INTO livre(auteur, titre, isbn, status, quantite) VALUES(?,?,?,?,?)";

        try(Connection connection = db.getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement(sql)){
        }
        return new Livre();
    }

    public Livre afficherLivre(int id) throws SQLException {
        String sql = "INSERT INTO livre(auteur, titre, isbn, status, quantite) VALUES(?,?,?,?,?)";

        try(Connection connection = db.getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement(sql)){
        }
        return new Livre();
    }

    public ArrayList<Livre> afficherLivres() throws SQLException {
        ArrayList<Livre> livres = new ArrayList<Livre>();
        String sql = "SELECT * FROM `livre`";

        try(Connection connection = db.getConnection();
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql)){

            while (resultSet.next()){
                Livre livre = new Livre();
                livre.setId(resultSet.getInt("id"));
                livre.setTitre(resultSet.getString("titre"));
                livre.setAuteur(resultSet.getString("auteur"));
                livre.setIsbn(resultSet.getInt("isbn"));
                livre.setStatus(resultSet.getInt("status"));
                livre.setQuantite(resultSet.getInt("quantite"));
                livres.add(livre);
            }
        }
        return livres;
    }
}