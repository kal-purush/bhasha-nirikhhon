package DAO;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import beans.Adresse;

public class AdresseDAO extends DAO<Adresse> {
	
	public  Adresse create(Adresse adresse){
		try {
		      ResultSet result = this.connect
                  .createStatement(
                  		ResultSet.TYPE_SCROLL_INSENSITIVE, 
                  		ResultSet.CONCUR_UPDATABLE
                  ).executeQuery(
                  		"SELECT MAX(id) AS id FROM Adresse"
                  );
		      if(result.first()){
		        long id = result.getLong("id") + 1;
		        
				PreparedStatement prepare = this.connect.prepareStatement("INSERT INTO Adresse (id, numero, rue, ville, codePostal) VALUES(?, ?, ?, ?, ?)");
				prepare.setLong(1, id);
				prepare.setLong(2, adresse.getNumero());
				prepare.setString(3, adresse.getRue());
				prepare.setString(4, adresse.getVille());
				prepare.setLong(5, adresse.getCodePostal());
				prepare.executeUpdate();
				
				adresse = this.find(id);	
			}	
	    } catch (SQLException e) {
	            e.printStackTrace();
	    }
		
		return adresse;
	}
	
	public Adresse find(long id){
		Adresse adresse = new Adresse();
		try {
		    ResultSet result = this.connect.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE)
		    		.executeQuery("SELECT * FROM Adresse WHERE id = " + id);
		    if(result.first())
		    	adresse = new Adresse();
		    	adresse.setId(id);
		    	adresse.setNumero(result.getInt("numero"));
		    	adresse.setRue(result.getString("rue"));
		    	adresse.setVille(result.getString("ville"));
		    	adresse.setCodePostal(result.getInt("codePostal"));

		    
		    } catch (SQLException e) {
		            e.printStackTrace();
		    }
		
		   return adresse;
	}

	public Adresse find(long numero, String rue, String ville, long codePostal){
		Adresse adresse = new Adresse();
		try {
		    ResultSet result = this.connect.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE)
		    		.executeQuery("SELECT * FROM Adresse WHERE numero = " + numero + " AND rue= '" + rue + "' AND ville='" + ville + "' AND codePostal = " + codePostal);
		    if(result.first())
		    	
		    	adresse = new Adresse();
		    	adresse.setId(result.getInt("id"));
		    	adresse.setNumero(result.getInt("numero"));
		    	adresse.setRue(result.getString("rue"));
		    	adresse.setVille(result.getString("ville"));
		    	adresse.setCodePostal(result.getInt("codePostal"));

		    
		    } catch (SQLException e) {
		            e.printStackTrace();
		    }
		
		   return adresse;
	}

	public  Adresse update(Adresse adresse){
		try {
            this .connect	
                 .createStatement(
                	ResultSet.TYPE_SCROLL_INSENSITIVE, 
                    ResultSet.CONCUR_UPDATABLE
                 ).executeUpdate(
                     	"UPDATE Adresse SET numero = '" + adresse.getNumero() + "',"+
                            	" rue = '" + adresse.getRue() + "',"+
                            	" ville = '" + adresse.getVille() + "',"+
                            	" codePostal = '" + adresse.getCodePostal() + "'"+
                            	" WHERE id = " + adresse.getId()
                            	);
            adresse = this.find(adresse.getId());
		} catch (SQLException e) {
            e.printStackTrace();
            return null;
		}
    
		return adresse;
	}
	
	public void delete(Adresse adresse){
		try {
            this    .connect
                	.createStatement(
                         ResultSet.TYPE_SCROLL_INSENSITIVE, 
                         ResultSet.CONCUR_UPDATABLE
                    ).executeUpdate(
                    	"DELETE FROM Adresse"+
                                	" WHERE id = " + adresse.getId()                        		
                    );
	    } catch (SQLException e) {
	            e.printStackTrace();
	    }
	}
}