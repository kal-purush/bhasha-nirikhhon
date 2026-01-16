package com.kupybaton.web.jdbc.create;

import com.kupybaton.web.jdbc.DatabaseConnect;

import java.sql.SQLException;

public class CategoryInserter extends DatabaseConnect {

	private static CategoryInserter categoryInserter;

	private CategoryInserter() {
	}

	public static CategoryInserter getListInserter() {
		if (categoryInserter == null) {
			categoryInserter = new CategoryInserter();
		}

		return categoryInserter;
	}

	public boolean insertNewCategory(String name) {
		String sql = "INSERT INTO category (name) VALUES (?)";

		try {
			insertIntoCategory(sql, name);
            return true;
		} catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeAllDBConnections();
        }
        return false;
	}

	private void insertIntoCategory(String sql, String name) throws SQLException {
        getPreparedStatementForCustomInsert(sql);
		preparedStatement.setString(1, name);
		preparedStatement.executeUpdate();
	}
	
}