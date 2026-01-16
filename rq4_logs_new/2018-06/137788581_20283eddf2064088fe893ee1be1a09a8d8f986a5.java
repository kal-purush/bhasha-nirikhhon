
package Beans;

import Connections.Connector;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.faces.bean.ManagedBean;
import javax.faces.bean.SessionScoped;

@ManagedBean(name="manager")
@SessionScoped
public class Manager {
    
    Connector cn = new Connector();
    public static Connection conn=null;
    public static PreparedStatement pstmt=null;
    public static ResultSet rs=null;
    private String str="";
  
    private String tg;
    private String catg;
    private String plc;

   
    
    private String username;
    private String password;

    public Manager(String tg, String catg, String plc) {
        this.tg = tg;
        this.catg = catg;
        this.plc = plc;
    }
   
    public Manager() { 
    }
    
     public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getPlc() {
        return plc;
    }

    public void setPlc(String plc) {
        this.plc = plc;
    }
    

    /**
     * @return the tg
     */
    public String getTg() {
        return tg;
    }

    /**
     * @param tg the tg to set
     */
    public void setTg(String tg) {
        this.tg = tg;
    }

    /**
     * @return the catg
     */
    public String getCatg() {
        return catg;
    }

    /**
     * @param catg the catg to set
     */
    public void setCatg(String catg) {
        this.catg = catg;
    }
    
 
    
    
    
    public String desc() throws ClassNotFoundException{
        
        str="select de from descri";
        String des=null;
        
        try {
            Class.forName("com.mysql.jdbc.Driver");
             conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/AlbaniaTravel","root","");
             Statement stm = conn.createStatement();
             ResultSet rs = stm.executeQuery(str);
             while(rs.next()){
                 des=rs.getString("de");
                 System.out.println("this is des "+des);
             }
        } catch (SQLException ex) {
            System.err.println("Problme with dbc "+ex);
        }
        finally{
            Connector.closeAll(conn, pstmt, rs);
        }
        System.out.println("this is des "+des);
        return des;
    }
    
    public List<Manager> tags() throws ClassNotFoundException{
        
        List<Manager> arr = new ArrayList<Manager>();
        str = "select name from tgs";
        System.out.println(str);
          
        try {
             Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/AlbaniaTravel","root","");
            Statement stm = conn.createStatement();
            ResultSet rs = stm.executeQuery(str);
            
            while(rs.next()){
                Manager mmb = new Manager();
                mmb.setTg(rs.getString("name"));
                arr.add(mmb);
                
            }  
        } catch (SQLException ex) {
            ex.printStackTrace();
            System.out.println("Exception Occured in the process :" + ex);
        }
        finally{
            cn.closeAll(conn, pstmt, rs);
            System.out.println("Finally block closed the connection OK from getAllMembers");
        }
        return arr;
    }
    
    
    public List<Manager> city() throws ClassNotFoundException{
        
        List<Manager> arr = new ArrayList<>();
      
        str="SELECT * FROM places WHERE category = 'city'";
        
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/AlbaniaTravel","root","");
            Statement stm = conn.createStatement();
            ResultSet rs = stm.executeQuery(str);
            
            while(rs.next()){
                Manager mg = new Manager();
                mg.setPlc(rs.getString("name"));
                arr.add(mg);
            }
        } catch (SQLException e) {
            System.err.println("Something went wrong here"+e);
        }
        finally{
            cn.closeAll(conn, pstmt, rs);
        }
        return arr;
    } 
    
     public List<Manager> village() throws ClassNotFoundException{
        
        List<Manager> arr = new ArrayList<>();
        
        str="select name from places where category like 'village'";
        
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/AlbaniaTravel","root","");
            Statement stm = conn.createStatement();
            ResultSet rs = stm.executeQuery(str);
            
            while(rs.next()){
                Manager mg = new Manager();
                mg.setPlc(rs.getString("name"));
                arr.add(mg);
            }
        } catch (SQLException e) {
            System.err.println("Something went wrong here"+e);
        }
        finally{
            cn.closeAll(conn, pstmt, rs);
        }
        return arr;
    } 
     
     
    public List<Manager> touristicpoint() throws ClassNotFoundException{
        
        List<Manager> arr = new ArrayList<>();
        
        str="SELECT * FROM places WHERE category = 'Tourist Point'";
        
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/AlbaniaTravel","root","");
            Statement stm = conn.createStatement();
            ResultSet rs = stm.executeQuery(str);
            
            while(rs.next()){
                Manager mg = new Manager();
                mg.setPlc(rs.getString("name"));
                arr.add(mg);
            }
        } catch (SQLException e) {
            System.err.println("Something went wrong here"+e);
        }
        finally{
            cn.closeAll(conn, pstmt, rs);
        }
        return arr;
    } 
         
    
        public void addTgs() throws ClassNotFoundException{
             
        cn.getConnection();
        String sql ="Insert into tgs(name) values(?)";
        try {
            pstmt = conn.prepareStatement(sql);
            pstmt.setString(1,this.getTg());
            
            int executeUpdate = pstmt.executeUpdate();
            if(executeUpdate >0){
                System.out.println("Add method Successfull");
                
            }
        } catch (SQLException ex) {
            Logger.getLogger(Manager.class.getName()).log(Level.SEVERE, null, ex);
        }finally{
            cn.closeAll(conn, pstmt, rs);
            System.out.println("Finally block closed the connection OK from ADD()");
        }
        
    }
        
        public List<String> search(String name) throws ClassNotFoundException{
            List<String> arr = new ArrayList<String>();
            for(Manager m : tags())
                if(m.getTg().toLowerCase().startsWith(name.toLowerCase()))
                    arr.add(m.getTg());
                return arr;
                
        }
        
        public List<String> checkcat() throws ClassNotFoundException{
            
            List<String> ar = new ArrayList<String>();
            str="select name from categ";
            
            try {
                Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/AlbaniaTravel","root","");
            Statement stm = conn.createStatement();
            ResultSet rs = stm.executeQuery(str);
            
            while(rs.next()){
                
                setCatg(rs.getString("name"));
                ar.add(catg);
            }
            } catch (SQLException e) {
                System.err.println("Smth went wrong"+e);
            }
            return ar;
        }
     
}