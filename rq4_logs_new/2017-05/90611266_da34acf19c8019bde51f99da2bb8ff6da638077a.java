package model;

import java.sql.*;
import javafx.collections.*;
import util.DBUtil;

public class CustomerDAO {
	/************************************************
	 * SELECT a particular customer from the database
	 * 
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 ************************************************/
	public static Customer searchCustomer(String customerID) throws SQLException, ClassNotFoundException {
		String query = "SELECT * FROM customers WHERE customerID = " + customerID;
		try {
			// run query on the database
			ResultSet rs = DBUtil.executeQuery(query);

			// send result set to getCustomerFromRS()
			Customer c = getCustomerFromRs(rs);
			return c;
		} catch (SQLException e) {
			System.out.println("Error on executing " + query + " on database: " + e);
			e.printStackTrace();
			throw e;
		}

	}

	/*************************************************************************
	 * SELECT Multiple customers from the database and create multiple objects
	 *************************************************************************/
	public static ObservableList<Customer> searchCustomers(){
		String query = "SELECT * FROM customers";
		try{
			//run query on the database
			ResultSet rs = DBUtil.executeQuery(query);
			ObservableList<Customer> customerList = getCustomerListFromRs(rs);
		}catch(SQLException e){
			System.out.println("Error on executing " + query + " on database " + e);
			e.printStackTrace();
			throw e;
		}
		return null;
	}

	private static ObservableList<Customer> getCustomerListFromRs(ResultSet rs) throws SQLException {
		ObservableList<Customer> cList = FXCollections.observableArrayList();
		int customerIDIndex = 1;
		int customerEmailIndex = 2;
		int customerPasswordIndex = 3;
		int customerPhoneIndex = 4;
		
		while(rs.next()){
			Customer c = new Customer();
			c.setCustomerID(rs.getInt(customerIDIndex));
			c.setCustomerEmail(rs.getString(customerEmailIndex));
			c.setCustomerPassword(rs.getString(customerPasswordIndex));
			c.setCustomerPhone(rs.getString(customerPhoneIndex));
			cList.add(c);
		}
		return cList;
	}

	/*
	 * Use result set from Database query to set a customer objects attributes
	 */
	private static Customer getCustomerFromRs(ResultSet rs) throws SQLException {
		Customer c = null;
		if (rs.next()) {
			int customerIDIndex = 1;
			int customerEmailIndex = 2;
			int customerPasswordIndex = 3;
			int customerPhoneIndex = 4;

			c = new Customer();
			c.setCustomerID(rs.getInt(customerIDIndex));
			c.setCustomerEmail(rs.getString(customerEmailIndex));
			c.setCustomerPassword(rs.getString(customerPasswordIndex));
			c.setCustomerPhone(rs.getString(customerPhoneIndex));
		}
		return c;
	}
}