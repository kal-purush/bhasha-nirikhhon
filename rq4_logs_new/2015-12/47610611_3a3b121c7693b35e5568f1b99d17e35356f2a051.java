package dbtask;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;

public class Connector {

    public static void init() throws ClassNotFoundException, IllegalAccessException, InstantiationException, SQLException {
        DriverManager.registerDriver((Driver) Class.forName("com.mysql.jdbc.Driver").newInstance());
    }

    @NotNull
    public static Connection getConnection() throws SQLException {
        StringBuilder mySqlUrl = new StringBuilder();
        mySqlUrl.
                append("jdbc:mysql://").        //db type
                append("localhost:").            //host name
                append("3306/").                //port
                append("db_example?").            //db name
                append("user=test&").            //login
                append("cachePrepStmts=true&").            //login
                append("useServerPrepStmts=true&").
                append("password=test");        //password

        return DriverManager.getConnection(choose(mySqlUrl));
    }

    private static String choose(StringBuilder builder) {
        System.out.append("URL: ").append(builder).append('\n');
        return builder.toString();
    }
}