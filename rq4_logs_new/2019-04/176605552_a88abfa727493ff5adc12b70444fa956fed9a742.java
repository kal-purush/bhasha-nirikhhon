import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author syntel
 */
public class manageUsers {
    
    private Connection connection;
    private PreparedStatement stmt;
    private Scanner input;
    private String userId;
    
    private String deleteUserYesOrNo;
    private String updateOrDeleteUser;
    private String updateUserYesOrNo;

    private String userName;
    private String password;
    
    private String userIdToUpdate;
    
    
    public void init()
    {
        System.out.println("Would you like to update or delete a user (UPDATE or DELETE)?: ");
        input = new Scanner(System.in);
        updateOrDeleteUser = input.nextLine().toUpperCase().trim();
        
        if (updateOrDeleteUser.equals("UPDATE"))
        {
            updateUser();
        }
        
        else if (updateOrDeleteUser.equals("DELETE"))
        {
            deleteUser();
        }
    }
    
    public void deleteUser()
    {
        // change print statement to appriopriate form of input
        input = new Scanner(System.in);
        
        while(true)
        {
            System.out.println("Do you want to delete a User (YES or NO)?: ");
            deleteUserYesOrNo = input.nextLine().trim().toUpperCase();

            if (deleteUserYesOrNo.equals("YES"))
            {
                System.out.println("Please enter a User ID to delete: ");
                String userIdToDelete = input.nextLine().trim();

                
                try {
                    Class.forName("oracle.jdbc.driver.OracleDriver");
                    connection = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521:XE","Student_Performance","Student_Performance");
                    stmt = connection.prepareStatement("delete from Users where user_id = ?");  
                    stmt.setString(1, userIdToDelete);
                    stmt.executeUpdate();
                    
                    connection.close();
                    stmt.close();
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
                
                break;
              
            }
            
            else if (deleteUserYesOrNo.equals("NO"))
            {
                System.out.println("Thank you. Have a nice day.");
            }
            
            else
            {
                System.out.println("Please enter YES or NO");
            }
        }

        
        
        /*
        try {
            // User input goes here
            stmt = con.prepareStatement("select * from (tableName) where id = ?");
            // do they want to update or delete.
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        */
        
    }
    
    public void updateUser()
        {
            // change print statement to appriopriate form of input
            input = new Scanner(System.in);

                System.out.println("Do you want to update a User (YES or NO)?: ");
                updateUserYesOrNo = input.nextLine().trim().toUpperCase();
            while(!((updateUserYesOrNo.equals("YES")) || updateUserYesOrNo.equals("NO")))
            {
                System.out.println("Please enter only 'YES' or 'NO'");
                updateUserYesOrNo = input.nextLine().trim().toUpperCase();
            }
            
                if (updateUserYesOrNo.equals("YES"))
                {
                    System.out.println("Please enter a user ID to update: ");
                    String userIdToUpdate = input.nextLine();
                    System.out.println("What is the new password?: ");
                    String newPassword = input.nextLine();
                    
                    System.out.println("Is User Admin? Please enter Y or N.");
                    String isAdmin = input.nextLine().toUpperCase().trim();

                    while(!(isAdmin.equals("Y") || isAdmin.equals("N"))){
                        System.out.println("Please enter only the character 'Y' or 'N'.");
                        isAdmin = input.nextLine().toUpperCase().trim();
                    }
               
                    try {
                        Class.forName("oracle.jdbc.driver.OracleDriver");
                        System.out.println("try");
                        connection = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521:XE","Student_Performance","Student_Performance");
                        stmt=connection.prepareStatement("update Users set password = ?, isAdmin = ? where user_id = ?");  
                        stmt.setString(1, newPassword);
                        stmt.setString(2, isAdmin);
                        stmt.setString(3, userIdToUpdate);

                        stmt.executeUpdate();
                        System.out.println("execute");
                        connection.close();
                        stmt.close();
                                    
                        System.out.println("Update Complete. Have a nice day.");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

                else if (updateUserYesOrNo.equals("NO"))
                {            
                    System.out.println("Thank you. Have a nice day.");
                }
        }
}