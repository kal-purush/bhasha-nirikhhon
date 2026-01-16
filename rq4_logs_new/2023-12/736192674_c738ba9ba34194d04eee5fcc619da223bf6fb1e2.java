package jdbc;

import java.io.*;
import java.io.IOException;
import java.sql.*;

public class storeimage {

		public static void main(String[] args) {
			String url="jdbc:mysql://localhost:3307/product";
			String user="root";
			String password="12345";
			
			String filepath="D:\\jaym\\verna.jpg";
			
			try {
				Connection conn=DriverManager.getConnection(url,user,password);
				
				String sql="INSERT INTO person(name,image)values(?,?)";
				PreparedStatement statement = conn.prepareStatement(sql);
				statement.setString(1,"verna");
				
				InputStream inputStream=new FileInputStream(new File(filepath));
				statement.setBlob(2,inputStream);
				
				int row=statement.executeUpdate();
				if(row>0) {
					System.out.println("A contact was inserted with photo image.");
				}
				conn.close();
			}catch(SQLException ex) {
				ex.printStackTrace();
			}catch(IOException ex) {
				ex.printStackTrace();
		}
	}
}




