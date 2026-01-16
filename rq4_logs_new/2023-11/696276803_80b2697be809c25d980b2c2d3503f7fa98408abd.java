import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class MySQLJDBCUtil {

    /**Get database connection
     * @return Connection
     * @throws SQLException
     */

    public static Connection getConnection() throws SQLException {
        Connection conn = null;

        try (FileInputStream f = new FileInputStream("D:\\FABIO\\INFORMATICA\\Develhope\\Java\\Java-Exercises\\Lessons\\JDBCPractice\\src\\credentials.txt")) {

            Properties pros = new Properties();
            pros.load(f);

            String url = pros.getProperty("url");
            String user = pros.getProperty("user");
            String password = pros.getProperty("password");

            System.out.println(url);
            System.out.println(user);
            System.out.println(password);

            conn = DriverManager.getConnection(url, user, password);
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
        return conn;
    }
}