package app.ticketing.td.movieticketingsystem;

import android.annotation.SuppressLint;
import android.os.StrictMode;
import android.util.Log;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
/**
 * Created by cosmi on 4/17/2016.
 */
public class ConnectionClass {
    private static final String connectionString = "jdbc:jtds:sqlserver://movieticketing.database.windows.net:1433/MovieTicketingDB;user=cosmin.leferman@movieticketing;password=L3monjuice;";

    public Connection Connect() {
        StrictMode.ThreadPolicy policy = new StrictMode.ThreadPolicy.Builder().permitAll().build();
        StrictMode.setThreadPolicy(policy);
        Connection conn = null;
        try {
            Class.forName("net.sourceforge.jtds.jdbc.Driver").newInstance();
            conn = DriverManager.getConnection(connectionString);
        }
        catch(SQLException ex) {
            Log.e("ERROR ", ex.getMessage());
        }
        catch(ClassNotFoundException ex) {
            Log.e("ERROR", ex.getMessage());
        }
        catch (Exception ex) {
            Log.e("ERROR", ex.getMessage());
        }

        return conn;
    }
}