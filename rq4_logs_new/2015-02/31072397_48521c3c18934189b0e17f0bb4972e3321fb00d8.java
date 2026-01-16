import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by viktorstrate on 20/02/15.
 */
public class Example {
    public static void main(String[] args) {
        // Connects with the database named "database",
        // at the host "localhost" with the username "root" and no password.
        Mysql testMysql = new Mysql("localhost", "root", "", "database");

        // Gets the results of a simple MYSQL query
        ResultSet result = testMysql.query("SELECT * FROM table");

        // Lists out all the usernames for the table
        try {
            while(result.next())
                System.out.println(result.getString("username"));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}