package pl.edu.nauka;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ConnectionDb {

    private static ConnectionDb connectionDb;

    private Connection connection;


    public  static Connection getConnection(){
        if(connectionDb==null){
            connectionDb = new ConnectionDb();
        }
        return connectionDb.connection;
    }

    private ConnectionDb(){
            connection = createConnection();
        }



    private Connection createConnection(){
        try{
            Class.forName("org.sqlite.JDBC");
            var connection = DriverManager.getConnection("jdbc:sqlite:Sekretariat.db");
            System.out.println("Udalo sie nawiazac polaczenie");
            return connection;
        }catch  (Exception e) {
            throw new CreateDBConnectionExpection(e);

        }
    }
}