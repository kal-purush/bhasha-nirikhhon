package in.kiruba.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import in.kiruba.model.Loansdb;
import in.kiruba.util.Connections;

public class AddLoansDao {
	private AddLoansDao() {
		
	}
	
	public static void addloans1(Loansdb type) throws SQLException, ClassNotFoundException  {
		// 1: Get the connection
		try {
			Connection connection = Connections.getConnection();
			// 2: Query

			String sql = "insert into loan (loan_name) values (?)";// (performance faster)
			System.out.println(sql);
			// 3: Execute
			PreparedStatement pst = connection.prepareStatement(sql);
			pst.setString(1, type.getLoanNames());

			int rows = pst.executeUpdate();
			System.out.println("No of rows inserted :" + rows);
			// 4: Release the connection
			Connections.close(connection,pst);
		} catch (ClassNotFoundException | SQLException e) {
			
			e.printStackTrace();
		}
		
	}

	
		
		
	}

