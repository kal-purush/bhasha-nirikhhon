package warsztatsamochodowy.database;
import java.sql.*;

public class DatabaseConnection {
   public DatabaseConnection(){
    }

    public static void connectDatabase(){

                try{
                    Class.forName("com.mysql.jdbc.Driver");
                    Connection con=DriverManager.getConnection("jdbc:mysql://localhost:3306/warsztat","root","");

                    Statement stmt=con.createStatement();
                    ResultSet rs=stmt.executeQuery("select * from pracownik");
                    while(rs.next())
                        System.out.println(rs.getInt(1)+"  "+rs.getString(2)+ "  " + rs.getString(3)+ "  " +rs.getString(4)+"   "+ rs.getString(5)+"  "+rs.getString(6));
                    con.close();
                }catch(Exception e){ System.out.println(e);}
            }


        public static void main(String[] args){
            connectDatabase();
    }

}