//Java SQL Test
//Meas, Perry
//This is a Java template for accessing SQLServer that you can use for future projects
//For Java 1.8, make sure you use the version 4.0 sqljdbc4 library or else it won't work
//This template was created in IntelliJ

import java.sql.* ;  // for standard JDBC programs
import java.math.*;

public class info340_test {
    public static void main(String[] args){
        try{
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");

            //Set login info here
            String url = "jdbc:sqlserver://is-fleming.ischool.uw.edu";
            String user = "perry";
            String pass = "Info340C";

            //Set the SQL query here
            String query = "SELECT drinker.name FROM drinker";

            Connection conn = DriverManager.getConnection(url, user, pass);

            //Set database here
            conn.setCatalog("HW3Bars");

            //Call query and store in memory as rs
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(query);

            //While results has next, print name
            while(rs.next()){
                System.out.print(rs.getString("name"));
                System.out.println();
            }

        }catch (Exception e) {
            e.printStackTrace();
        }
    }

}