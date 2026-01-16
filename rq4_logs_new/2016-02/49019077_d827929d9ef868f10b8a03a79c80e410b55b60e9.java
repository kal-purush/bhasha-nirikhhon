package com.kupybaton.web.jdbc.update;

import com.kupybaton.web.jdbc.DatabaseConnect;

import java.sql.Date;
import java.sql.SQLException;

public class ListUpdater extends DatabaseConnect {

	private static ListUpdater listUpdater;

	private ListUpdater() {
	}

	public static ListUpdater getListUpdater() {
		if (listUpdater == null) {
			listUpdater = new ListUpdater();
		}

		return listUpdater;
	}

	public boolean updateList(String name, Integer id, Date dateCreated) {
		String sql = "UPDATE list SET name=? WHERE id=? AND date_cr=?";

		try {
			updateListName(sql, name, id, dateCreated);
            return true;
		} catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeAllDBConnections();
        }
        return false;
	}

	private void updateListName (String sql, String name, Integer id, Date dateCreated) throws SQLException {
        getPreparedStatementForCustomCRUD(sql);
		preparedStatement.setString(1, name);
		preparedStatement.setInt(2, id);
		preparedStatement.setDate(3, dateCreated);
		preparedStatement.executeUpdate();
	}
	
}