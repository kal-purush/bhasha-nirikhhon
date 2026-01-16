package resttest.sql;

import javax.naming.InitialContext;
import javax.sql.DataSource;
import java.sql.*;

/**
 * Created by Mark on 1/6/2015.
 */
public abstract class TestConnector {
    static final String URL = "jdbc:mysql://localhost/test";
    static final String USER = "sqlUser";
    static final String PASS = "swag100";

    private static Connection conn;
    private static Statement stmt;

    static {
        try {
            InitialContext cxt = new InitialContext();
            DataSource ds = (DataSource) cxt.lookup( "java:/comp/env/jdbc/TestDB" );

            conn =  ds.getConnection();
            stmt = conn.createStatement();
        }catch (Exception e){
            System.out.println(e);
        }
    }

    public static String getAll(){
        try {
            String total = "";
            ResultSet rs = stmt.executeQuery("SELECT test_swagcol FROM test.test_swag;");
            while(rs.next()){
                total += rs.getString("test_swagcol");
            }
            return total;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return "ERROR";
    }



}