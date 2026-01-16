package DataBase;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;

import Model.Juego;
import Model.JuegoPeer;

public class DataManager {
	
	  private String dbURL = "";
	  private String dbUserName = "";
	  private String dbPassword = "";

	  public void setDbURL(String dbURL) {
	    this.dbURL = dbURL;
	    }
	  public String getDbURL() {
	    return dbURL;
	    }

	  public void setDbUserName(String dbUserName) {
	    this.dbUserName = dbUserName;
	    }
	  public String getDbUserName() {
	    return dbUserName;
	    }

	  public void setDbPassword(String dbPassword) {
	    this.dbPassword = dbPassword;
	    }
	  public String getDbPassword() {
	    return dbPassword;
	    }

	  public Connection getConnection() {
	    Connection conn = null;
	    try {
	      conn = DriverManager.getConnection(getDbURL(), getDbUserName(),
	          getDbPassword());
	      }
	    catch (SQLException e) {
	      System.out.println("Could not connect to DB: " + e.getMessage());
	      }
	    return conn;
	    }
	  public void putConnection(Connection conn) {
	    if (conn != null) {
	      try { conn.close(); }
	      catch (SQLException e) { }
	      }
	    }
	  
	  /////////////////////////////////////////////////////////////////////////////////////
	  
	  public boolean insertarJuego(String consola, String titulo, String genero) {
		  
		return JuegoPeer.insertarJuego(consola, titulo, genero, this);
		  
	  }
	  
	  public ArrayList<Juego> obtenerJuegos(){
		  
		return JuegoPeer.devolverJuegos(this);
		  
	  }
	  
	  
}