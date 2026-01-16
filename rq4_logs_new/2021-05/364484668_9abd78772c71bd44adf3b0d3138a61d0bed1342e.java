package in.casekartin.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashSet;
import java.util.Set;

import in.casekartin.model.CaseManager;
import in.casekartin.util.ConnectionUtil;

public class CaseManagerDAO1 {
	/**
	 * add the case name and price to database
	 * @param caseName
	 * @param price
	 * @return
	 * @throws Exception
	 */
	public static boolean addCase(String caseName, Float price) throws Exception {
		Connection connection = null;
		PreparedStatement pst = null;
		try {
			// Get Connection
			connection = ConnectionUtil.getConnection();
			// prepare data
			String sql = "insert into caseTypes(casename,price) values (?,?)";
			pst = connection.prepareStatement(sql);
			pst.setString(1, caseName);
			pst.setFloat(2, price);
			System.out.println(connection);

			// Execute Query
			int rows = pst.executeUpdate();
			boolean inserted = rows == 1 ? true : false;
			return inserted;
		} catch (Exception e) {
			throw new RuntimeException("Unable to add case");
		} finally {
			ConnectionUtil.close(connection, pst);
		}
	}
	/**
	 * Retrieve all data from database table 
	 * @return
	 * @throws Exception
	 */
	public static Set<CaseManager> listAllCases() throws Exception {
		Set<CaseManager> caseTypes = new HashSet<>();
		Connection connection = null;
		PreparedStatement pst = null;
		connection = ConnectionUtil.getConnection();
		// Retrieve data from table
		String sql = "select casename,price from caseTypes";
		pst = connection.prepareStatement(sql);
		ResultSet rs = pst.executeQuery();
		while (rs.next()) {
			String caseName = rs.getString("casename");
			Float price = rs.getFloat("price");

			// Store the data in model
			CaseManager product = new CaseManager(caseName, price);
			// Store all products in list
			caseTypes.add(product);
		}
		ConnectionUtil.close(connection, pst, rs);
		return caseTypes;
	}
	/**
	 * Delete specific data in database
	 * @param caseName
	 * @return
	 * @throws Exception
	 */
	public static boolean deleteCase(String caseName) throws Exception {
		Connection connection = null;
		PreparedStatement pst = null;
		try {
			// Get Connection
			connection = ConnectionUtil.getConnection();
			// prepare data
			String sql = "delete from caseTypes where casename=?";
			
			// Execute Query
			pst = connection.prepareStatement(sql);
			pst.setString(1, caseName);
			int rows = pst.executeUpdate();
			boolean deleted = rows == 1 ? true : false;
			System.out.println("isDeleted"+rows);
			return deleted;
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("Unable to delete case");
		}
		finally {
			ConnectionUtil.close(connection, pst);
		}
		
	}
}