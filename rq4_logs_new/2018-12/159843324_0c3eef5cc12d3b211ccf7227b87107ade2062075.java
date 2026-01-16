package de.Ninju.tsBot.Main;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.mysql.jdbc.Connection;

public class MySQL {
	
    public static String host = "18.185.25.212";
	public static String port = "3306";
	public static String database = "ts3bot";
	public static String username = "ts3bot";
	public static String password = "";
	public static Connection con;

	
	public static void addGamegroup(String uid, String game, String name) {
		connect();
		update("UPDATE userData2 SET " + game + " = '" + name + "' WHERE uid = '" + uid + "'");
		disconnect();
	}
	
	public static void register(String uid) {
		connect();
		if(!isRegisterd(uid)) {
			update("INSERT INTO `userData2` (`uid`) VALUES ('" + uid + "');");
		}
		disconnect();
	}
	
	private static boolean isRegisterd(String uid) {
		ResultSet rs = getResult("SELECT * FROM `userData2` WHERE `uid` = '" + uid + "'");
		try {
			while(rs.next()) {
				return rs.getString("uid") != null;
			}
		} catch (SQLException e) {}
		return false;
	}
	
	
	public static void connect() {
		if (!isConnected()) {
			try
			{
				con = (Connection) DriverManager.getConnection("jdbc:mysql://" + host + ":" + port + "/" + database, username, password);
				System.out.println("[MySQL] [Sync] Verbindung aufgebaut!");
			}
			catch (SQLException e)
			{
				System.out.println("[MySQL] Connection failed");
				e.printStackTrace();
			}
		}
	}
	public static void disconnect()
	{
		if (isConnected()) {
			try
			{
				con.close();
				System.out.println("[MySQL] [Sync] Verbindung geschlossen");
			}
			catch (SQLException e)
			{
				e.printStackTrace();
			}
		}
	}

	public static boolean isConnected()
	{
		return con != null;
	}

	public static Connection getConnection()
	{
		return con;
	}
	public static void update(String qry) {
		if (isConnected()) {
			try {
				con.createStatement().executeUpdate(qry);
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	public static ResultSet getResult(String qry) {
		if (isConnected()) {
			try {
				return con.createStatement().executeQuery(qry);
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return null;
	}
	public static Object get(String whereresult, String where, String select, String database) {

		ResultSet rs = getResult("SELECT " + select + " FROM " + database + " WHERE " + where + "='" + whereresult + "'");
		try {
			if(rs.next()) {
				Object v = rs.getObject(select);
				return v;
			}
		} catch (SQLException e) {
			return "ERROR";
		}

		return "ERROR";
	}

}