/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Connection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Vector;

/**
 *
 * @author Pasindu Tennage
 */
public class DBConnector {
    //this class is a singleton class that is used to connect to the data base
    //when implementing follow the singleton class structure

    private static DBConnector dbConnector = null;                             //the instance to access this object
    //static for global access

    // JDBC driver name and database URL
    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    static final String DB_URL = "jdbc:mysql://localhost:3306/nts_database";

    //  Database credentials
    static final String USER = "username";
    static final String PASS = "password";
    
    //check and avoid making a public constructor in a singleton
    public DBConnector() {
    }                                                     //make this private to make sure only one instance is there

    public static DBConnector getConnection() {                                  //if the object is never needed. it is never gonna be created
        if (dbConnector == null) {
            dbConnector = new DBConnector();
        }
        return dbConnector;
    }

    //---------------------Changed the return type of this method--------------------------------//
    public Connection connectToDatabase() throws ClassNotFoundException, SQLException {
        //establish the connection to the database

        Connection connection = null;

        //resgister JDBC Driver
        Class.forName("com.mysql.jdbc.Driver");
        //open a connection
        connection = DriverManager.getConnection(DB_URL, USER,PASS );
        
        //get statement
        return connection;

    }

    public void updateTable(String sqlStatement) throws ClassNotFoundException, SQLException {
        //sends data to a table 
        //insert into tablename values = ___;
        
        //estabilish a connection
        Connection connection = connectToDatabase();
        Statement statement = null;
        
        //create statement on connection
        statement = connection.createStatement();
        
        //execute a query
        statement.executeUpdate(sqlStatement);
       
    }

    public void createNewTable(String sqlStatement) throws ClassNotFoundException, SQLException {
        //create table tableName(name VARCHAR(20))
        
        Connection connection = connectToDatabase();
        Statement statement = null;
        
        //create statement on connection
        statement = connection.createStatement();
        
        //execute a query
        statement.executeUpdate(sqlStatement);      
        
        
    }

    public ResultSet getQuerry(String sqlStatement) throws ClassNotFoundException, SQLException {
        //select * from table ___
        Connection connection = connectToDatabase();
        Statement statement = null;
        
        //create statement on connection
        statement = connection.createStatement();
        
        //execute a query
        ResultSet rs=statement.executeQuery(sqlStatement); 
        
        
        return rs;
        /*Vector v = new Vector();
        while (rs.next()) {                
                v.add(rs.getInt("student_id"));
                v.add(rs.getString("name"));
                v.add(rs.getDate("date_of_birth"));
                v.add(rs.getInt("batch"));
                v.add(rs.getString("address"));
                v.add(rs.getString("nic"));
                v.add(rs.getInt("phone_number"));
                v.add(rs.getDate("registration_date"));
                v.add(rs.getString("guardian_one_name"));
                v.add(rs.getInt("guardian_one_telephone"));
                v.add(rs.getString("guardian_one_address"));
                v.add(rs.getString("guardian_two_name"));
                v.add(rs.getInt("guardian_two_telephone"));
                v.add(rs.getString("guardian_two_address"));
                v.add(rs.getBoolean("hostel_student"));
                v.add(rs.getInt("level"));
                v.add(rs.getString("picture"));
            }
        return v;*/
        
    }

}