package com.kupybaton.web.jdbc.delete;

import com.kupybaton.web.jdbc.DatabaseConnect;

import java.sql.SQLException;

public class ListDeleter extends DatabaseConnect {

	private static ListDeleter listDeleter;

	private ListDeleter() {
	}

	public static ListDeleter getListDeleter() {
		if (listDeleter == null) {
			listDeleter = new ListDeleter();
		}

		return listDeleter;
	}

	public boolean deleteList(Integer id) {
		String sql = "DELETE FROM list WHERE id=?";

		try {
			deleteFromList(sql, id);
            return true;
		} catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeAllDBConnections();
        }
        return false;
	}

	private void deleteFromList(String sql, Integer id) throws SQLException {
        getPreparedStatementForCustomCRUD(sql);
		preparedStatement.setInt(1, id);
		preparedStatement.executeUpdate();
	}
	
}