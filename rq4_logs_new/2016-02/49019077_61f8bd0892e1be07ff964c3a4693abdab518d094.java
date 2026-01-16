package com.kupybaton.web.jdbc.insert;

import java.sql.SQLException;

import com.kupybaton.model.KupyBaton;
import com.kupybaton.model.ProductList;
import com.kupybaton.web.jdbc.DatabaseConnect;

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

	public void insertNewList(ProductList list) {
		
		String sql = "INSERT INTO list (id, name, date_cr) VALUES (?, ?, ?)";
		
		try {
			insertIntoDataBase(sql, list);
		} catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
	}
	
	@Override
	protected void insertIntoDataBase(String sql, KupyBaton list) throws SQLException {
		preparedStatement = getPreparedStatementForCustomInsert(sql);
		preparedStatement.setInt(1, list.getId());
		preparedStatement.setString(2, list.getName());
		preparedStatement.setDate(3, ((ProductList) list).getDateCreated());
		preparedStatement.executeUpdate();
	}
}