
package Class;

import mycode.DbConnection;

/**
 *
 * @author Janith Chathuranga
 */
public class Baker {
    String id;
    String name;
    String age;
    String address;
    String tel;

    public Baker(String id, String name, String age, String add, String tel) {
        this.id=id;
        this.name=name;
        this.age=age;
        this.address=add;
        this.tel=tel;
    }

    public Baker(String id) {
           this.id=id;
    }
    
     public boolean addcashier()
     {
        boolean status=false;
         try {
             DbConnection con=new DbConnection();
             String q="INSERT INTO baker (id,name,age,address,tel) values ('"+ id +"', '"+ name +"', '"+ age +"', '"+ address +"', '"+ tel +"')";
             status=con.insert(q);
         } catch (Exception e) {
         }
         return status;
     }
    
    
   
     
     public boolean updatecashier()
    {
     boolean status=false;
        try {
            DbConnection con=new DbConnection();
            String q="update baker set name = '"+ name +"' ,age = '"+ age +"' ,address = '"+ address +"' ,tel = '"+ tel +"' where id = '"+ id +"'";
            status=con.insert(q);
  
        } catch (Exception e) {
        }   return status;
    }
    
    
    
    
    public boolean deletecashier()
    {
      boolean status=false;
        try {
            DbConnection con=new DbConnection();
            String q="DELETE  FROM baker Where id = '"+ id +"'";
            status=con.insert(q);
        } catch (Exception e) {
        }
 
         
      return status;  
    }
    
    
    
    
}