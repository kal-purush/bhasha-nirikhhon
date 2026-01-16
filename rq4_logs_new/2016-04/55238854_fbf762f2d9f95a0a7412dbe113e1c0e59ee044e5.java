package Server;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class UserDBConnection {

    // define MySQL database driver
    public static final String DBDRIVER = "org.gjt.mm.mysql.Driver";

    // define MySQL database connection URL
    public static final String DBURL = "jdbc:mysql://localhost:3306/user";

    // username of MySQL database
    public static final String DBUSER = "root";

    // password of MySQL database
    public static final String DBPASS = "mysqladmin";

    public void insertInfo(String name, String password, String sex, int age, String birthday, String email) throws Exception {
        Connection conn = null;
        Statement stmt = null;
        Class.forName(DBDRIVER);
        String sqlInsert = "INSERT INTO userinfo(name,password,sex,age,birthday,email) VALUES ('" + name + "','" + password + "','" + sex + "'," + age + ",'" + birthday + "','" + email + "');";
        conn = DriverManager.getConnection(DBURL, DBUSER, DBPASS);
        stmt = conn.createStatement();
        stmt.executeUpdate(sqlInsert);
        stmt.close();
        conn.close();
    }

    public boolean searchInfo(String name) throws Exception {
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        Class.forName(DBDRIVER);
        String sqlSelect = "SELECT name FROM userinfo WHERE name='" + name + "';";
        conn = DriverManager.getConnection(DBURL, DBUSER, DBPASS);
        stmt = conn.createStatement();
        rs = stmt.executeQuery(sqlSelect);
        boolean result = rs.next();
        rs.close();
        stmt.close();
        conn.close();
        return result;
    }

    public boolean searchInfo(String name, String password) throws Exception {
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        Class.forName(DBDRIVER);
        String sqlSelect = "SELECT name FROM userinfo WHERE name='" + name + "' AND password = '" + password + "';";
        conn = DriverManager.getConnection(DBURL, DBUSER, DBPASS);
        stmt = conn.createStatement();
        rs = stmt.executeQuery(sqlSelect);
        boolean result = rs.next();
        rs.close();
        stmt.close();
        conn.close();
        return result;
    }

    public void insertTemp(String ip, String name, String password) throws Exception {
        Connection conn = null;
        Statement stmt = null;
        Class.forName(DBDRIVER);
        String sqlInsert = "INSERT INTO usertemp(ip,name,password) VALUES ('" + ip + "','" + name + "','" + password + "');";
        conn = DriverManager.getConnection(DBURL, DBUSER, DBPASS);
        stmt = conn.createStatement();
        stmt.executeUpdate(sqlInsert);
        stmt.close();
        conn.close();
    }

    public void deleteTemp(String ip) throws Exception {
        Connection conn = null;
        Statement stmt = null;
        Class.forName(DBDRIVER);
        String sqlInsert = "DELETE FROM usertemp WHERE ip='" + ip + "';";
        conn = DriverManager.getConnection(DBURL, DBUSER, DBPASS);
        stmt = conn.createStatement();
        stmt.executeUpdate(sqlInsert);
        stmt.close();
        conn.close();
    }

    public boolean searchTemp(String ip) throws Exception {
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        Class.forName(DBDRIVER);
        String sqlSelect = "SELECT name FROM usertemp WHERE ip='" + ip + "';";
        conn = DriverManager.getConnection(DBURL, DBUSER, DBPASS);
        stmt = conn.createStatement();
        rs = stmt.executeQuery(sqlSelect);
        boolean result = rs.next();
        rs.close();
        stmt.close();
        conn.close();
        return result;
    }

    public String getName(String ip) throws Exception {
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        Class.forName(DBDRIVER);
        String sqlSelect = "SELECT name FROM usertemp WHERE ip='" + ip + "';";
        conn = DriverManager.getConnection(DBURL, DBUSER, DBPASS);
        stmt = conn.createStatement();
        rs = stmt.executeQuery(sqlSelect);
        String name = null;
        if (rs.next()) {
            name = rs.getString(1);
        }
        rs.close();
        stmt.close();
        conn.close();
        return name;
    }

    public String getPass(String ip) throws Exception {
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        Class.forName(DBDRIVER);
        String sqlSelect = "SELECT password FROM usertemp WHERE ip='" + ip + "';";
        conn = DriverManager.getConnection(DBURL, DBUSER, DBPASS);
        stmt = conn.createStatement();
        rs = stmt.executeQuery(sqlSelect);
        String pass = null;
        if (rs.next()) {
            pass = rs.getString(1);
        }
        rs.close();
        stmt.close();
        conn.close();
        return pass;
    }
}