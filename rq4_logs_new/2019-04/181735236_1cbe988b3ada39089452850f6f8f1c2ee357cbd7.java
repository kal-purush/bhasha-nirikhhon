package com.hellToheaven;

import java.sql.Connection;
import java.sql.PreparedStatement;

public class AddBooksDao_1 {

   public static int save(String callno,String name,String author,String publisher,int quantity,int issued){
        int status = 0;
       try{
           Connection con = DB_1.getConnection();
           PreparedStatement ps = con.prepareStatement("insert into books(callno,name,author,publisher,quantity,issued)" +
                   " values(?,?,?,?,?,?)");
           ps.setString(1,callno);
           ps.setString(2,name);
           ps.setString(3,author);
           ps.setString(4,publisher);
           ps.setInt(5,quantity);
           ps.setInt(6,issued);
           status = ps.executeUpdate();
       }
       catch (Exception e){
           e.printStackTrace();
       }
       return status;
    }
}