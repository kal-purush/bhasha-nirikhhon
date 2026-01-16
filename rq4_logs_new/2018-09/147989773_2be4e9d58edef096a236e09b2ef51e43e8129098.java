package hal_shop.util;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.sql.Connection;

/**
 * closeを行う必要のある、SQL接続クラス。
 * @author arakitakaki
 *
 */
public class DBManager {
	
	private String dbname = "jv_test";
	private String server = "localhost";
	private String port = "3306";
	private Connection con;
	private StringBuilder query;
	private boolean where;
	
	public DBManager() {}
	
	public DBManager(String dbname) {
		this.dbname = dbname;
		connect();
	}
	public DBManager(String dbname, String server){
		this.dbname = dbname;
		this.server = server;
		connect();
	}
	public DBManager(String dbname, String server, String port){
		this.dbname = dbname;
		this.server = server;
		this.port = port;
		connect();
	}

	
	public void connect() {
		try {
			StringBuilder url = new StringBuilder();
			Class.forName("org.hsqldb.jdbcDriver");
			url.append("jdbc:hsqldb:hsql://");
			url.append(this.server).append(":");
			url.append(this.port).append("/");
			url.append(this.dbname);
			url.append("?useUnicode=true&characterEncoding=utf8");	//設定
			con = DriverManager.getConnection(url.toString(), "root", "");
		} catch (ClassNotFoundException e) {
			System.out.println(e);
		} catch (SQLException e) {
			System.out.println(e);
		} finally {
			
		}
	}
	
	public void db(String table) {
		this.query = new StringBuilder();
		this.query.append("SELECT * FROM ").append(table);
		this.where = false;
	}

	public void where(String col ,String conditions) {
		String opperand = this.where ? " AND " : " WHERE ";
		this.query.append(opperand).append(col).append(" = ").append(conditions);
		this.where = true;
	}
	
	/**
	 * Stringの二次元配列として値を返却する。
	 * @return table
	 */
	public ArrayList<ArrayList<String>> toArray(){
		try {
			ArrayList<ArrayList<String>> tbl = new ArrayList<ArrayList<String>>();
			Statement st = this.con.createStatement();
			ResultSet rs = st.executeQuery(this.query.toString());
			ResultSetMetaData rsmd = rs.getMetaData();
			while(rs.next()) {
				ArrayList<String> rec = new ArrayList<String>();
				for(int i = 1; i < rsmd.getColumnCount(); i++ ) {
					rec.add(rs.getString(i));
				}
				tbl.add(rec);
			}
			return tbl;
		}catch (SQLException e) {
			System.out.println(e);
		}
		return null;
	}
	
	/**
	 * DTOモデルの通りに値を返却する。
	 * @param mapping
	 * @return
	 */
	public <T> List<T> get(ResultSetMapping<T> mapping) {
		try {
			// (2) SQLの実行
			List<T> list = new ArrayList<T>();
			Statement st = this.con.createStatement();
			ResultSet rs = st.executeQuery(this.query.toString());
			while(rs.next()) {
				T dto = mapping.setMapping(rs);
				list.add(dto);
			}
			st.close();
			return list;
		} catch (SQLException e){
			System.out.println(e);
		}
		return null;
	}
	
	public <T> T find(String key, ResultSetMapping<T> mapping) {
		try {
			Statement st = this.con.createStatement();
			ResultSet rs = st.executeQuery(this.query.toString());
			if(!rs.next()) return null;
			T dto = mapping.setMapping(rs);
			return dto;	
		}catch(SQLException e) {
			System.out.println(e);
		}
		return null;
	}
	
	public void close() {
		try{
			this.con.close();
		}catch(SQLException e){}
	}
}