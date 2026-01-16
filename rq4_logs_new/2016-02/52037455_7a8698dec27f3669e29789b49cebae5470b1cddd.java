import java.sql.*;
import java.io.*;
import java.util.Scanner;

class query
{
  public static void main (String args [])
       throws SQLException
  {
    int a;
    int n;
    float b;
    Scanner reader = new Scanner(System.in);  // Reading from System.in

    System.out.println("\nPlease choose from the selection below");
    System.out.println("Query 1: ");
    System.out.println("Query 2: ");
    System.out.println("Query 3: ");
    System.out.println("Query 4: ");
    System.out.println("Query 5: ");
    System.out.println("Query 6: ");
    System.out.println("Query 7: ");
    System.out.println("Query 8: ");
    System.out.println("Query 9: ");
    System.out.println("Query 10: ");
    System.out.println("Query 11: ");
    System.out.println("Query 12: ");
    //System.out.println("Query 13: ");
    //System.out.println("Query 14: ");
    System.out.println("Entering 0 will quit the program");


    while(true) {


    System.out.print("\nEnter the number of the desired query: ");
    //System.out.println("Enter a number: ");

    n = reader.nextInt(); // Scans the next token of the input as an int.
 
        String inString;
        switch (n) {
            case 0:  inString = "Option 0";  //quits the program
                     System.out.println("System exit");
                     System.exit(0);
                     break;
            case 1:  inString = "Query 1";
                     break;
            case 2:  inString = "Query 2";
                     break;
            case 3:  inString = "Query 3";
                     break;
            case 4:  inString = "Query 4";
                     break;
            case 5:  inString = "Query 5";
                     break;
            case 6:  inString = "Query 6";
                     break;
            case 7:  inString = "Query 7";
                     break;
            case 8:  inString = "Query 8";
                     break;
            case 9:  inString = "Query 9";
                     break;
            case 10: inString = "Query 10";
                     break;
            case 11: inString = "Query 11";
                     break;
            case 12: inString = "Query 12";
                     break;
            default: inString = "Invalid selection";
                     break;
        }
        System.out.println(inString);
        System.out.println();

    }

  
  }
}








class region
{
  public static void main (String args [])
       throws SQLException
  {

    String connstr = "jdbc:sqlite:states.db";
    Connection conn = DriverManager.getConnection (connstr);
				   
    Statement stmt = conn.createStatement ();
    String query = "select region from regions";

    ResultSet rs = stmt.executeQuery (query);

    // Iterate through the result and display the results
    System.out.println("The regions are:");
    while (rs.next ()) {
      System.out.println (rs.getString (1));
    }

    rs.close(); stmt.close(); conn.close();
  }
}

class state
{
  public static void main (String args [])
       throws SQLException
  {
    String connstr = "jdbc:sqlite:states.db";
    Connection conn = DriverManager.getConnection (connstr);
           
    Statement stmt = conn.createStatement ();
    String query = "select state from states";

    ResultSet rs = stmt.executeQuery (query);

    // Iterate through the result and display the results
    System.out.println("The states are:");
    while (rs.next ()) {
      System.out.println (rs.getString (1));
    }

    rs.close(); stmt.close(); conn.close();
  }
}

class player_region
{
  public static void main (String args [])
       throws SQLException
  {
    String connstr = "jdbc:sqlite:states.db";
    Connection conn = DriverManager.getConnection (connstr);
           
    Statement stmt = conn.createStatement ();
    String query = "select r.region, players  from regions r join (select s.regcode as REGION, count(*) as PLAYERS from streg s join roster x on s.stcode = x.stcode group by s.regcode order by 2 desc, 1) s where r.regcode=s.region";

    ResultSet rs = stmt.executeQuery (query);

    // Iterate through the result and display the results
    System.out.println("The number of players from each region are:");
    while (rs.next ()) {
      System.out.print (rs.getString (1));
      System.out.print (":  ");

      System.out.println (rs.getString (2));
    }

    rs.close(); stmt.close(); conn.close();
  }
}
