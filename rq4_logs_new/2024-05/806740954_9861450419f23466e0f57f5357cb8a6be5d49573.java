import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class JdbcConnection {

    private JdbcConnection() {

    }
    public static  Connection getJdbcConnection() throws SQLException {
        Connection connection = null;
        String url = "jdbc:mysql://localhost:3306/ineuron";
        String user = "root";
        String password = "root";


        connection = DriverManager.getConnection(url, user, password);
        if (connection != null) {
                return connection;
        }
        return connection;
    }
}