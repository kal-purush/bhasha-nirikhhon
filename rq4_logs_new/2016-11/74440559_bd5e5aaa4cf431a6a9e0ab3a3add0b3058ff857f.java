package common;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;

import db.DataBaseProperty;

abstract public class DataBaseManager {
	static final public String driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver";

	static final public String dburl=DataBaseProperty.DB_URL+";DataBaseName="+DataBaseProperty.DB_NAME;
	static final public String dbuser=DataBaseProperty.DB_USERNAME;
	static final public String dbpassword=DataBaseProperty.DB_PASSWORD;

	public int getRID(String tableName) {
		Connection con = null;
		int rid=0;
		try {
			Class.forName(driver);
			con = DriverManager.getConnection(dburl, dbuser, dbpassword);
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT IDENT_CURRENT('"+tableName+"') AS ID");
			ResultSetMetaData meta = rs.getMetaData();
            if (rs.next()) {
                rid = rs.getInt(meta.getColumnCount());
            }
			stmt.close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				con.close();
			} catch (SQLException es) {
				es.printStackTrace();
			}
		}
		return rid;
	}

	public void resetRID(String tableName){
		Connection con = null;
		try {
			Class.forName(driver);
			con = DriverManager.getConnection(dburl, dbuser, dbpassword);
			Statement stmt = con.createStatement();
			stmt.executeUpdate("DBCC CHECKIDENT ("+tableName+",RESEED, 0);");
			stmt.close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				con.close();
			} catch (SQLException es) {
				es.printStackTrace();
			}
		}
	}

	protected void updateRecord(String query) {
		Connection con = null;
		try {
			Class.forName(driver);
			con = DriverManager.getConnection(dburl, dbuser, dbpassword);
			Statement stmt = con.createStatement();
			stmt.executeUpdate(query);
			stmt.close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				con.close();
			} catch (SQLException es) {
				es.printStackTrace();
			}
		}
	}

	public Object getRecord(String sql) {
		Connection con = null;
		Object object = null;
		try {

			Class.forName(driver);
			con = DriverManager.getConnection(dburl, dbuser, dbpassword);
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery(sql);

			if (rs.next()) {
				object = copyRecord(rs);
			}

			rs.close();
			stmt.close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				con.close();
			} catch (SQLException es) {
				es.printStackTrace();
			}
		}
		return object;
	}

	public LinkedList getRecords(String sql) {
		Connection con = null;
		LinkedList list = new LinkedList();
		try {

			Class.forName(driver);
			con = DriverManager.getConnection(dburl, dbuser, dbpassword);
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery(sql);

			while (rs.next()) {
				Object object = copyRecord(rs);
				list.addLast(object);
			}

			rs.close();
			stmt.close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				con.close();
			} catch (SQLException es) {
				es.printStackTrace();
			}
		}
		return list;
	}

	abstract protected Object copyRecord(ResultSet rs) throws Exception;

}