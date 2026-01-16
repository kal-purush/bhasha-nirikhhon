/*
 * Template JAVA User Interface
 * =============================
 *
 * Database Management Systems
 * Department of Computer Science &amp; Engineering
 * University of California - Riverside
 *
 * Target DBMS: 'Postgres'
 *
 */


import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.ArrayList;
import java.io.IOException;

/**
 * This class defines a simple embedded SQL utility class that is designed to
 * work with PostgreSQL JDBC drivers.
 *
 */
public class Cafe {

   // reference to physical database connection.
   private Connection _connection = null;

   private String authorisedUser = null;

   public String getAuthorisedUser(){return authorisedUser;}
   public void setAuthorisedUser(String a){authorisedUser = a;}

   // handling the keyboard inputs through a BufferedReader
   // This variable can be global for convenience.
   static BufferedReader in = new BufferedReader(
                                new InputStreamReader(System.in));

   /**
    * Creates a new instance of Cafe
    *
    * @param hostname the MySQL or PostgreSQL server hostname2
    * @param database the name of the database
    * @param username the user name used to login to the database
    * @param password the user login password
    * @throws java.sql.SQLException when failed to make a connection.
    */
   public Cafe(String dbname, String dbport, String user, String passwd) throws SQLException {

      System.out.print("Connecting to database...");
      try{
         // constructs the connection URL
         String url = "jdbc:postgresql://localhost:" + dbport + "/" + dbname + "?gssEncMode=disable";
         System.out.println ("Connection URL: " + url + "\n");

         // obtain a physical connection
         this._connection = DriverManager.getConnection(url, user, passwd);
         System.out.println("Done");
      }catch (Exception e){
         System.err.println("Error - Unable to Connect to Database: " + e.getMessage() );
         System.out.println("Make sure you started postgres on this machine");
         System.exit(-1);
      }//end catch
   }//end Cafe

   /**
    * Method to execute an update SQL statement.  Update SQL instructions
    * includes CREATE, INSERT, UPDATE, DELETE, and DROP.
    *
    * @param sql the input SQL string
    * @throws java.sql.SQLException when update failed
    */
   public void executeUpdate (String sql) throws SQLException {
      // creates a statement object
      Statement stmt = this._connection.createStatement ();

      // issues the update instruction
      stmt.executeUpdate (sql);

      // close the instruction
      stmt.close ();
   }//end executeUpdate

   /**
    * Method to execute an input query SQL instruction (i.e. SELECT).  This
    * method issues the query to the DBMS and outputs the results to
    * standard out.
    *
    * @param query the input query string
    * @return the number of rows returned
    * @throws java.sql.SQLException when failed to execute the query
    */
   public int executeQueryAndPrintResult (String query) throws SQLException {
      // creates a statement object
      Statement stmt = this._connection.createStatement ();

      // issues the query instruction
      ResultSet rs = stmt.executeQuery (query);

      /*
       ** obtains the metadata object for the returned result set.  The metadata
       ** contains row and column info.
       */
      ResultSetMetaData rsmd = rs.getMetaData ();
      int numCol = rsmd.getColumnCount ();
      int rowCount = 0;

      // iterates through the result set and output them to standard out.
      boolean outputHeader = true;

      while (rs.next()){
		 if(outputHeader){
			for(int i = 1; i <= numCol; i++){
			System.out.print(rsmd.getColumnName(i) + "\t");
			}
			System.out.println();
			outputHeader = false;
		 }
         for (int i=1; i<=numCol; ++i)
            System.out.print (rs.getString (i));
         System.out.println ();
         ++rowCount;
      }//end while
      stmt.close ();
      return rowCount;
   }//end executeQuery

   /**
    * Method to execute an input query SQL instruction (i.e. SELECT).  This
    * method issues the query to the DBMS and returns the results as
    * a list of records. Each record in turn is a list of attribute values
    *
    * @param query the input query string
    * @return the query result as a list of records
    * @throws java.sql.SQLException when failed to execute the query
    */
   public List<List<String>> executeQueryAndReturnResult (String query) throws SQLException {
      // creates a statement object
      Statement stmt = this._connection.createStatement ();

      // issues the query instruction
      ResultSet rs = stmt.executeQuery (query);

      /*
       ** obtains the metadata object for the returned result set.  The metadata
       ** contains row and column info.
       */
      ResultSetMetaData rsmd = rs.getMetaData ();
      int numCol = rsmd.getColumnCount ();
      int rowCount = 0;

      // iterates through the result set and saves the data returned by the query.
      boolean outputHeader = false;
      List<List<String>> result  = new ArrayList<List<String>>();
      while (rs.next()){
        List<String> record = new ArrayList<String>();
		for (int i=1; i<=numCol; ++i)
			record.add(rs.getString (i));
        result.add(record);
      }//end while
      stmt.close ();
      return result;
   }//end executeQueryAndReturnResult

   /**
    * Method to execute an input query SQL instruction (i.e. SELECT).  This
    * method issues the query to the DBMS and returns the number of results
    *
    * @param query the input query string
    * @return the number of rows returned
    * @throws java.sql.SQLException when failed to execute the query
    */
   public int executeQuery (String query) throws SQLException {
       // creates a statement object
       Statement stmt = this._connection.createStatement ();

       // issues the query instruction
       ResultSet rs = stmt.executeQuery (query);

       int rowCount = 0;

       // iterates through the result set and count nuber of results.
       while (rs.next()){
          rowCount++;
       }//end while
       stmt.close ();
       return rowCount;
   }

   /**
    * Method to fetch the last value from sequence. This
    * method issues the query to the DBMS and returns the current
    * value of sequence used for autogenerated keys
    *
    * @param sequence name of the DB sequence
    * @return current value of a sequence
    * @throws java.sql.SQLException when failed to execute the query
    */
   public int getCurrSeqVal(String sequence) throws SQLException {
	Statement stmt = this._connection.createStatement ();

	ResultSet rs = stmt.executeQuery (String.format("Select currval('%s')", sequence));
	if (rs.next())
		return rs.getInt(1);
	return -1;
   }

   /**
    * Method to close the physical connection if it is open.
    */
   public void cleanup(){
      try{
         if (this._connection != null){
            this._connection.close ();
         }//end if
      }catch (SQLException e){
         // ignored.
      }//end try
   }//end cleanup

   /**
    * The main execution method
    *
    * @param args the command line arguments this inclues the <mysql|pgsql> <login file>
    */
   public static void main (String[] args) {
      if (args.length != 3) {
         System.err.println (
            "Usage: " +
            "java [-classpath <classpath>] " +
            Cafe.class.getName () +
            " <dbname> <port> <user>");
         return;
      }//end if

      Greeting();
      Cafe esql = null;
      try{
         // use postgres JDBC driver.
         Class.forName ("org.postgresql.Driver").newInstance ();
         // instantiate the Cafe object and creates a physical
         // connection.
         String dbname = args[0];
         String dbport = args[1];
         String user = args[2];
         esql = new Cafe (dbname, dbport, user, "");

         boolean keepon = true;
         while(keepon) {
            // These are sample SQL statements
            System.out.println("MAIN MENU");
            System.out.println("---------");
            System.out.println("1. Create user");
            System.out.println("2. Log in");
            System.out.println("9. < EXIT");
            String tempuser = null;
            switch (readChoice()){
               case 1: CreateUser(esql); break;
               case 2: tempuser = LogIn(esql); 
                       esql.setAuthorisedUser(tempuser);
                       break;
               case 9: keepon = false; break;
               default : System.out.println("Unrecognized choice!"); break;
            }//end switch
            if (esql.getAuthorisedUser() != null) {
              boolean usermenu = true;
              while(usermenu) {
                System.out.println("MAIN MENU");
                System.out.println("---------");
                System.out.println("1. Goto Menu");
                System.out.println("2. Update Profile");
                System.out.println("3. Place a Order");
                System.out.println("4. Update a Order");
                System.out.println(".........................");
                System.out.println("9. Log out");
                switch (readChoice()){
                   case 1: Menu(esql); break;
                   case 2: UpdateProfile(esql); break;
                   case 3: PlaceOrder(esql); break;
                   case 4: UpdateOrder(esql); break;
                   case 9: usermenu = false; break;
                   default : System.out.println("Unrecognized choice!"); break;
                }
              }
            }
         }//end while
      }catch(Exception e) {
         System.err.println (e.getMessage ());
      }finally{
         // make sure to cleanup the created table and close the connection.
         try{
            if(esql != null) {
               System.out.print("Disconnecting from database...");
               esql.cleanup ();
               System.out.println("Done\n\nBye !");
            }//end if
         }catch (Exception e) {
            // ignored.
         }//end try
      }//end try
   }//end main

   public static void Greeting(){
      System.out.println(
         "\n\n*******************************************************\n" +
         "              User Interface      	               \n" +
         "*******************************************************\n");
   }//end Greeting

   /*
    * Reads the users choice given from the keyboard
    * @int
    **/
   public static int readChoice() {
      int input;
      // returns only if a correct value is given.
      do {
         System.out.print("Please make your choice: ");
         try { // read the integer, parse it and break.
            input = Integer.parseInt(in.readLine());
            break;
         }catch (Exception e) {
            System.out.println("Your input is invalid!");
            continue;
         }//end try
      }while (true);
      return input;
   }//end readChoice

   /*
    * Creates a new user with privided login, passowrd and phoneNum
    **/
   public static void CreateUser(Cafe esql){
      try{
         System.out.print("\tEnter user login: ");
         String login = in.readLine();
         System.out.print("\tEnter user password: ");
         String password = in.readLine();
         System.out.print("\tEnter user phone: ");
         String phone = in.readLine();
         
	    String type="Customer";
	    String favItems="";

				 String query = String.format("INSERT INTO USERS (phoneNum, login, password, favItems, type) VALUES ('%s','%s','%s','%s','%s')", phone, login, password, favItems, type);

         esql.executeUpdate(query);
         System.out.println ("User successfully created!");
      }catch(Exception e){
         System.err.println (e.getMessage ());
      }
   }//end CreateUser


   /*
    * Check log in credentials for an existing user
    * @return User login or null is the user does not exist
    **/
   public static String LogIn(Cafe esql){
      try{
         System.out.print("\tEnter user login: ");
         String login = in.readLine();
         System.out.print("\tEnter user password: ");
         String password = in.readLine();

         String query = String.format("SELECT * FROM USERS WHERE login = '%s' AND password = '%s'", login, password);
         int userNum = esql.executeQuery(query);
	 if (userNum > 0)
		return login;
         return null;
      }catch(Exception e){
         System.err.println (e.getMessage ());
         return null;
      }
   }//end

// Rest of the functions definition go in here

 public static void Menu(Cafe esql){
   try{
      List<List<String>> result = esql.executeQueryAndReturnResult(String.format("SELECT type FROM USERS WHERE login = '%s'", esql.getAuthorisedUser()));
      if(result.isEmpty()){
         System.out.println("User does not exist");
         return;
      }
      boolean isManager = result.get(0).get(0).contains("Manager") ? true : false;
      String[] options = {"1. Search item by name", "2. Search items by type", "3. Add item", "4. Delete item"};
      int optionsToShow = isManager ? 4 : 2;

      boolean done = false;

      while(!done){
         esql.executeQueryAndPrintResult("SELECT itemName, type, description, price FROM Menu");
         for(int i = 0; i < optionsToShow; i++){
            System.out.println(options[i]);
         }
         System.out.println(String.format("%d. Main Menu", optionsToShow + 1));

         System.out.print("Enter your selection: ");
         int selection = 0;
         try{
            selection = Integer.parseInt(in.readLine());
         }catch(Exception e){
            selection = 0;
         }

         String input = "";
         int queryCount = 0;
         switch(selection){
            case 1:
               while(true){
                  System.out.print("[Empty to return]\nEnter the item name: ");
                  input = in.readLine();
                  if(input.isEmpty()){ break; }
                  queryCount = esql.executeQueryAndPrintResult(String.format("SELECT itemName, type, description, price FROM Menu WHERE itemName = '%s'", input));
               }
               break;
            case 2:
               while(true){
                  System.out.print("[Empty to return]\nEnter the item type: ");
                  input = in.readLine();
                  if(input.isEmpty()){ break; }
                  queryCount = esql.executeQueryAndPrintResult(String.format("SELECT itemName, type, description, price FROM Menu WHERE type = '%s'", input));
               }
               break;
            case 3:
               if(!isManager){ done = true; break;}

               String query = "";
               while(true){
                  query = "INSERT INTO Menu (itemName, type, price, description, imageURL) VALUES(";
                  System.out.print("Enter item name: ");
                  query += String.format("'%s', ",in.readLine());
                  System.out.print("\nEnter item type: ");
                  query += String.format("'%s', ",in.readLine());
                  System.out.print("\nEnter item price: ");
                  query += String.format("%s, ",in.readLine());
                  System.out.print("\nEnter item description: ");
                  query += String.format("'%s', ",in.readLine());
                  System.out.print("\nEnter item's image URL: ");
                  query += String.format("'%s')",in.readLine());


                  esql.executeUpdate(query);

                  System.out.print("Add another item [y/n]: ");
                  input = in.readLine();
                  if(input.contains("n") || input.contains("N")){ break; }
               }
               break;
            case 4:
               if(isManager){
                  System.out.print("[Empty to return]\nEnter the item name: ");
                  input = in.readLine();
                  if(input.isEmpty()){ break; }

                  esql.executeUpdate(String.format("DELETE FROM Menu WHERE itemName = '%s'", input));
               }
            case 5:  
               if(isManager){
                  done = true;
                  break;
               }
            default:
               System.out.println("Invalid Choice");
               break;
            
         }
      }

   }catch(Exception e){
      System.err.println(e.getMessage());
   }
  }

  public static void UpdateProfile(Cafe esql) throws IOException {
     System.out.print("\n(1) to update login \n(2) to update phone \n(3) to update password\n\n");
     int user_choice = esql.readChoice();
     String new_entry = "";
     String query = "UPDATE users SET ";

     
     switch(user_choice){
        case 1:
            System.out.println("Enter new login: ");
            new_entry = in.readLine();
            query += "login = '" + new_entry + "' WHERE login = '" + esql.getAuthorisedUser() +"'";
            try{
               esql.executeUpdate(query);
            }
            catch(Exception e){System.out.println(e);}
        break;
        case 2:
            System.out.println("Enter new phone: ");
            new_entry = in.readLine();
            query += "phoneNum = " + new_entry + " WHERE login = '" + esql.getAuthorisedUser() +"'";
            try{
               esql.executeUpdate(query);
            }
            catch(Exception e){System.out.println(e);}
            
        break;
        case 3:
            System.out.println("Enter new password: ");
            new_entry = in.readLine();
            query += "password = '" + new_entry + "' WHERE login = '" + esql.getAuthorisedUser() +"'";
            try{
               esql.executeUpdate(query);
            }
            catch(Exception e){System.out.println(e);}
            
        break;
            
        default:
             System.out.println(String.valueOf(user_choice) + " is not an option.");
        break;
        
     }
     
  }

  public static void PlaceOrder(Cafe esql) throws IOException, SQLException{
      
      int user_choice = 1;
      String user_choice2;
      String user_entry = "";
      String query = "";
      List<String> orders = new ArrayList<String>();
      List<List<String>> result;
      Double total = 0.0;
      String itemname = "";
      while(user_choice == 1 || user_choice == 2){
         System.out.print("\n(1) to enter itemName \n(2) to enter item type \n(3) to check out: ");
         user_choice = Integer.parseInt(in.readLine());
         switch(user_choice){
            case 1:
               System.out.print("\nEnter itemName: ");
               user_entry = in.readLine();
               System.out.println("\nResult: \n");
               query = "SELECT * from Menu WHERE itemName = '" + user_entry + "'";
               try{
                  esql.executeQueryAndPrintResult(query);
                  System.out.println();
               }
               catch(Exception e){System.out.println(e);}
               System.out.print("Order Item? (y)es or (n)o : ");
               user_choice2 = in.readLine();
               System.out.println();
               switch(user_choice2){
                  case "y":
                     result = esql.executeQueryAndReturnResult(query);
                     itemname = result.get(0).get(0);
                     itemname = itemname.replaceAll("\\s+","");
                     total += Double.parseDouble(result.get(0).get(2));
                     orders.add(itemname);
                     System.out.println(itemname + " added. Total: " + total);


                  break;

                  case "n":
                     System.out.println(itemname + " not added. Total: " + total);
                  break;

                  default:
                     System.out.println(String.valueOf(user_choice) + " is not an option.");
                  break;

               }
            break;

            case 2:

            break;

            case 3:
               int order_id = esql.executeQuery("SELECT * FROM ORDERS");
               String user_login = esql.getAuthorisedUser();
               
               System.out.println("\n\nChecking out...\n");
               System.out.println("---------------------");
               System.out.println("Items in cart: ");
               orders.forEach(order -> {
                  System.out.println('\t' + "*" + order);
               });
               System.out.println("\nTotal: " + total + "\n");
               System.out.println("---------------------\n");


               


            break;

            default:
               System.out.println(Integer.valueOf(user_choice) + " is not an option.");
            break;
      }
      }

  }
      
  public static void UpdateOrder(Cafe esql){}

}//end Cafe
