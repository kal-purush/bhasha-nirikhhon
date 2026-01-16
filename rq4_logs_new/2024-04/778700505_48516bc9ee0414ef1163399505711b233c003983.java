/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package database;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import model.Intervention;
import model.Pompier;

/**
 *
 * @author zakina
 */
public class DaoIntervention {
    
    Connection cnx;
    static PreparedStatement requeteSql = null;
    static ResultSet resultatRequete = null;
    
    public static ArrayList<Intervention> getLesInterventions(Connection cnx){
        
        ArrayList<Intervention> lesInterventions = new ArrayList<Intervention>();
        try{
            requeteSql = cnx.prepareStatement("SELECT intervention.id AS i_id, intervention.lieu AS i_lieu, intervention.date AS i_date, intervention.heureAppel AS i_heureAppel, intervention.heureArrivee AS i_heureArrivee, intervention.duree AS i_duree FROM intervention INNER JOIN pompier_intervention ON intervention.id = pompier_intervention.intervention_id INNER JOIN pompier ON pompier.id = pompier_intervention.pompier_id;");
            resultatRequete = requeteSql.executeQuery();
            
           while (resultatRequete.next()) {
            Intervention i = new Intervention();
            i.setId(resultatRequete.getInt("i_id"));
            i.setLieu(resultatRequete.getString("i_lieu"));
            
            Date sqlDate = resultatRequete.getDate("i_date");
            i.setDate(sqlDate.toLocalDate());
                    
            i.setHeureAppel(resultatRequete.getString("i_heureAppel"));
            i.setHeureArrivee(resultatRequete.getString("i_heureArrivee"));
            i.setDuree(resultatRequete.getString("i_duree"));
             
                
                lesInterventions.add(i);
            }
           
        }
        catch (SQLException e){
            e.printStackTrace();
            System.out.println("La requête de getLesFonctions e généré une erreur");
        }
        return lesInterventions;
    }
    
    public static Intervention getInterventionById(Connection cnx, int idIntervention){
        
        Intervention i = null ;
        try{
            requeteSql = cnx.prepareStatement("SELECT intervention.id AS i_id, intervention.lieu AS i_lieu, intervention.date AS i_date, intervention.heureAppel AS i_heureAppel, intervention.heureArrivee AS i_heureArrivee, intervention.duree AS i_duree FROM intervention INNER JOIN pompier_intervention ON intervention.id = pompier_intervention.intervention_id INNER JOIN pompier ON pompier.id = pompier_intervention.pompier_id where intervention.id = ?;");
            requeteSql.setInt(1, idIntervention);
            resultatRequete = requeteSql.executeQuery();
            
            if (resultatRequete.next()){
                
            i = new Intervention();
            i.setId(resultatRequete.getInt("i_id"));
            i.setLieu(resultatRequete.getString("i_lieu"));
            
            Date sqlDate = resultatRequete.getDate("i_date");
            i.setDate(sqlDate.toLocalDate());
                    
            i.setHeureAppel(resultatRequete.getString("i_heureAppel"));
            i.setHeureArrivee(resultatRequete.getString("i_heureArrivee"));
            i.setDuree(resultatRequete.getString("i_duree"));
             
              
            }
           
        }
        catch (SQLException e){
            e.printStackTrace();
            System.out.println("La requête de getInterventionById  a généré une erreur");
        }
        return i ;
    }
    
    public static ArrayList<Pompier> getLesPompiersByIntervention(Connection cnx, int idFonction){
        
        ArrayList<Pompier> lesPompiers = new ArrayList<Pompier>();
        Pompier p = null;
        
        try{
            requeteSql = cnx.prepareStatement ("SELECT pompier.id AS p_id, pompier.nom AS p_nom, pompier.prenom AS p_prenom FROM pompier INNER JOIN pompier_fonction ON pompier.id = pompier_fonction.pompier_id WHERE pompier_fonction.fonction_id = ?;");
            requeteSql.setInt(1, idFonction);
            resultatRequete = requeteSql.executeQuery();
        
        if (resultatRequete.next()){
            
            p = new Pompier();
            p.setId(resultatRequete.getInt("p_id"));
            p.setNom(resultatRequete.getString("p_nom"));
            p.setPrenom(resultatRequete.getString("p_prenom"));
            
            lesPompiers.add(p);
            }
        }
        
        
        catch (SQLException e){
            e.printStackTrace();
            System.out.println("La requête de getInterventionById  a généré une erreur");
        }
        return lesPompiers;
    }
    
    public static Intervention addIntervention(Connection connection, Intervention i){      
        int idGenere = -1;
        try
        {
            //preparation de la requete
            // id (clé primaire de la table client) est en auto_increment,donc on ne renseigne pas cette valeur
            // la paramètre RETURN_GENERATED_KEYS est ajouté à la requête afin de pouvoir récupérer l'id généré par la bdd (voir ci-dessous)
            // supprimer ce paramètre en cas de requête sans auto_increment.
            requeteSql=connection.prepareStatement("INSERT INTO Intervention (lieu, date, heureAppel, heureArrivee, duree)\n" +
                    "VALUES (?, ?, ?, ?, ?)", requeteSql.RETURN_GENERATED_KEYS );
            requeteSql.setString(1, i.getLieu());
            requeteSql.setDate(2, Date.valueOf(i.getDate()));
            requeteSql.setString(3, i.getHeureAppel());
            requeteSql.setString(4, i.getHeureArrivee());
            requeteSql.setString(5, i.getDuree());

           /* Exécution de la requête */
            requeteSql.executeUpdate();
            
             // Récupération de id auto-généré par la bdd dans la table client
            resultatRequete = requeteSql.getGeneratedKeys();
            while ( resultatRequete.next() ) {
                idGenere = resultatRequete.getInt( 1 );
                i.setId(idGenere);
                
                i = DaoIntervention.getInterventionById(connection, i.getId());
            }
            
         
        }   
        catch (SQLException e) 
        {
            e.printStackTrace();
            //out.println("Erreur lors de l’établissement de la connexion");
        }
        return i ;    
    }
    
}