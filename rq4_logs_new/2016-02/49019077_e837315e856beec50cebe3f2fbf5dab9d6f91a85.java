package com.kupybaton.web.jdbc.create;

import com.kupybaton.web.jdbc.DatabaseConnect;

import java.sql.Date;
import java.sql.SQLException;
import java.util.Calendar;

public class ListInserter extends DatabaseConnect {

	private static ListInserter listInserter;

	private ListInserter() {
	}

	public static ListInserter getListInserter() {
		if (listInserter == null) {
			listInserter = new ListInserter();
		}

		return listInserter;
	}

	public boolean insertNewList(String name) {
		String sql = "INSERT INTO list (name, date_cr) VALUES (?, ?)";

		try {
			insertIntoDataBase(sql, name);
            return true;
		} catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeAllDBConnections();
        }
        return false;
	}

	private void insertIntoDataBase(String sql, String name) throws SQLException {
        getPreparedStatementForCustomInsert(sql);
		preparedStatement.setString(1, name);
		preparedStatement.setDate(2, new Date(Calendar.getInstance().getTimeInMillis()));
		preparedStatement.executeUpdate();
	}
}