package lab.mvc.dao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Dbmanage {
    public Connection initDB() {
        // 初始化数据库连接方法
        Connection conn = null;
        // 创建一个Connection句柄
        try {
            Class.forName("com.mysql.jdbc.Driver");
            // 加载数据库驱动
            String url = "jdbc:mysql://localhost:3306/userdb?charaterEncoding=utf-8";
            // 定义数据库地址url，并设置编码格式
            conn = DriverManager.getConnection(url, "root", "");
            // 得到数据连接
        } catch (ClassNotFoundException e) {

            e.printStackTrace();
        } catch (SQLException e) {

            e.printStackTrace();
        }
        return conn;
        // 返回数据库连接
    }

    public void closeDB(Statement sta, Connection conn) {
        // 关闭数据库连接（无结果集）
        try {
            sta.close();
            conn.close();
        } catch (SQLException e) {

            e.printStackTrace();
        }

    }

    public void closeDB(ResultSet rs, Statement sta, Connection conn) {
        // 关闭数据库连接（有结果集）
        try {
            rs.close();
            sta.close();
            conn.close();
        } catch (SQLException e) {

            e.printStackTrace();
        }

    }
}