/**
 * ApplicationMain.java
 * 7/18/17
 * Swan Ronson
 *
 * ApplicationMain will serve as the entry point to our LinkedIn application in the final implementation.  This
 * iteration is a simplified version that shows Java code to connect to the database and execute a few queries.
 * The database was build outside of this file, and will be provided in the submission.  The SQL queries to construct
 * the database will also be provided in the file "db_schema.txt". For more examples of the SQL queries we plan
 * to implement, including some more complex ones, see section 3.1 of our application write-up.
 */
package src;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class ApplicationMain
{
    public static void main ( String[] args )
    {
        Connection con = null;

        try
        {
            // Connect to the database
            Class.forName( "org.h2.Driver" );
            con = DriverManager.getConnection( "jdbc:h2:~/Dropbox/CSCI 320/LinkedIn/db/linkedin", "admin", "admin" );
            Statement stmt = con.createStatement();

            // Below are a few example queries that will be used in functions in the fully-implemented release
            // Get a list of all companies that are currently hiring and print out their information
            ResultSet rs = stmt.executeQuery( "SELECT * FROM Company WHERE hiring='true' ORDER BY name ");
            while ( rs.next() )
            {
                int companyID = rs.getInt( "companyID" );
                String name = rs.getString( "name" );
                String desc = rs.getString( "description" );
                System.out.println( "ID: " + companyID + ", Name: " + name + ", Description: " + desc );
            }

            // a user updates their profile to indicate they are no longer searching for a job
            int userID = 3; // this will be pulled from the database when the user logs in
            stmt.executeUpdate( "UPDATE User SET searching='false' WHERE userID='" + userID + "'" );

            // User A (id=1) wants to see user B's(id=2) skills, and potentially endorse one
            // query for user B's skills:
            int A_ID = 1;
            int B_ID = 2;
            rs = stmt.executeQuery( "SELECT name, description FROM Skill WHERE userID='2'" );
            while( rs.next() )
            {
                String name = rs.getString( "name" );
                String desciption = rs.getString( "description" );
                System.out.println( "Skill: " + name + ", Description: " + desciption );
            }
            // User A sees B's skill and wants to endorse user B's skill in Java:
            stmt.executeUpdate( "INSERT INTO Endorses VALUES ('" + A_ID + "', '" + B_ID + "', 'Java' )"  );

            // A user wants a listing of all of Google's California offices
            rs = stmt.executeQuery( "SELECT C.companyID, Name, city, state " +
                                        "FROM Company AS C NATURAL JOIN Office " +
                                        "WHERE State='CA' AND Name='Google' ;" );
            while( rs.next() )
            {
                String name = rs.getString( "Name" );
                String city = rs.getString( "City" );
                String state = rs.getString( "State" );
                System.out.println( "Name: " + name + ", City: " + city + ", State: " + state );
            }

            // Again, the example queries above will be separated into functions for the final implementation
            // For more examples, including some complex ones, see section 3.1 of our phase 2 write-up

            // close the connect to the database
            if ( con != null ) con.close();

        } catch ( Exception e ) {
            System.out.println("Error: " + e.getMessage());
        }
    }
}