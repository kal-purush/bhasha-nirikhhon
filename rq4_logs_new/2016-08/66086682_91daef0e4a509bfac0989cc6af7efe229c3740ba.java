package br.com.PersistSpring.conexao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Conexao {
	
	private static final String URL = "jdbc:mysql://localhost:3306/bd_jpa?useUnicode=true&amp;characterEncoding=utf8";
	private static final String DRIVER = "com.mysql.jdbc.Driver";
	private static final String USER = "root";
	private static final String PASSWORD = "root";
	

	private Connection conn;
	
		
	public Connection abreConexao() throws SQLException {
		try {
			Class.forName(DRIVER);
			setConn(DriverManager.getConnection(URL, USER, PASSWORD));
			return conn;
		} catch (Exception e) {
			throw new SQLException(e.getMessage());
		}
	}
	
	public void closeConnection(PreparedStatement stmt, ResultSet rs)  {
		if (rs != null) {
			try {
				rs.close();
				rs = null;
			} catch (SQLException e) {
			}
		}
		if (stmt != null) {
			try {
				stmt.close();
				stmt = null;
			} catch (SQLException e) {
			}
		}
		fechaConexao();
		
	}
	
	public void fechaConexao()  {
		if (conn != null) try {
	        conn.close();
        }
        catch (SQLException e) {
	        // TODO Auto-generated catch block
	        e.printStackTrace();
        }
		
	}
	
	protected void finalize() throws Throwable {
		try {
			conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		super.finalize();
	}
	
	public Connection getConn() {
		return conn;
	}
	
	public void setConn(Connection conn) {
		this.conn = conn;
	}
	
}