/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package database;

/**
 *
 * @author ts1sio
 */
/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */


import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import model.Situation;
import model.Intervention;

/**
 *
 * @author mahdi.ashuri
 */
public class DaoSituation {
    
    Connection cnx;
    static PreparedStatement requeteSql = null;
    static ResultSet resultatRequete = null;
    
    public static ArrayList<Situation> getLesSituations(Connection cnx){
        
        ArrayList<Situation> lesSituations = new ArrayList<Situation>();
        try{
            requeteSql = cnx.prepareStatement("SELECT situation.id AS s_id,"
                    + " situation.libelle AS s_libelle"
                    + " FROM situation;");
            resultatRequete = requeteSql.executeQuery();
            
            while (resultatRequete.next()){
                
                Situation s = new Situation();
                    s.setId(resultatRequete.getInt("s_id"));
                    s.setLibelle(resultatRequete.getString("s_libelle"));
                     
                lesSituations.add(s);
            }
           
        }
        catch (SQLException e){
            e.printStackTrace();
            System.out.println("La requête de getLesSituations a généré une erreur");
        }
        return lesSituations;
    }
    
    
    public static ArrayList<Intervention> getInterventionsBySituationId(Connection cnx, int idIntervention){
        
        ArrayList<Intervention> lesInterventions = new ArrayList<Intervention>();
        Intervention i = null;
        
        try{
            requeteSql = cnx.prepareStatement("SELECT intervention.id AS i_id, intervention.lieu AS i_lieu, intervention.date AS i_date, intervention.heureAppel AS i_heureAppel, intervention.heureArrivee AS i_heureArrivee, intervention.duree AS i_duree FROM intervention WHERE intervention.situation_id = ?;");
            requeteSql.setInt(1, idIntervention);
            resultatRequete = requeteSql.executeQuery();
        
        while (resultatRequete.next()){
            
             i = new Intervention();
            i.setId(resultatRequete.getInt("i_id"));
            i.setLieu(resultatRequete.getString("i_lieu"));
            
            Date date = resultatRequete.getDate("i_date");
            i.setDate(date.toLocalDate());
                    
            i.setHeureAppel(resultatRequete.getString("i_heureAppel"));
            i.setHeureArrivee(resultatRequete.getString("i_heureArrivee"));
            i.setDuree(resultatRequete.getString("i_duree"));
            
            lesInterventions.add(i);
        }
        }
        
        
        catch (SQLException e){
            e.printStackTrace();
            System.out.println("La requête de getInterventionsBySituationId  a généré une erreur");
        }
        return lesInterventions;
    }
       
    public static Situation getSituationById(Connection cnx, int idSituation){
        
        Situation s = null ;
        try{
            requeteSql = cnx.prepareStatement("SELECT situation.id AS s_id,"
            + " situation.libelle AS s_libelle"
            + " FROM situation"
            + " WHERE situation.id = ?;");
            requeteSql.setInt(1, idSituation);
            resultatRequete = requeteSql.executeQuery();
            
            if (resultatRequete.next()){
                
                    s = new Situation();
                    s.setId(resultatRequete.getInt("s_id"));
                    s.setLibelle(resultatRequete.getString("s_libelle"));
                    
                    ArrayList<Intervention> lesInterventions = DaoSituation.getInterventionsBySituationId(cnx, idSituation);
                    s.setLesInterventions(lesInterventions);
            }
           
        }
        catch (SQLException e){
            e.printStackTrace();
            System.out.println("La requête de getCaserneById a généré une erreur");
        }
        return s ;
    }
    
}