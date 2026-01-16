package controller;

import database.DatabaseConnectionHandler;
import model.Place;
import org.apache.ibatis.session.SqlSessionException;

import java.sql.SQLException;

public class VirusDatabase {
    private DatabaseConnectionHandler dbHandler = null;

    public VirusDatabase() { dbHandler = new DatabaseConnectionHandler();}

    // connects to Oracle database with supplied username and password
    public boolean login(String username, String password) {
        boolean didConnect = dbHandler.login(username, password);

        if (didConnect) {
            return true;
        } else {
            return false;
        }
    }

    public boolean createDatabase() {

        try {
            dbHandler.createDatabase();
        }
        catch (SQLException se) {
            se.printStackTrace( );
            return false;
        }
        return true;
    }

    public Place[] getPlaceInfo() {

        return dbHandler.getPlaceInfo();
    }
}