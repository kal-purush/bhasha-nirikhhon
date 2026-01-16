package com.jdbcutils;


import java.sql.*;

/**
 * ClassName： JDBCUtils
 * package：jdbcutils
 * Description：
 *
 * @Author 任少騫
 * @Create 2024/6/29 @{TIME}
 * @Version 1.0
 */
public class JDBCUtils {
//    private static final Logger LOGGER = Logger.getLogger(JDBCUtils.class.getName());

    public static Connection getConnection() throws Exception {
//        LOGGER.info("Attempting to get a database connection...");
        Class.forName("com.mysql.cj.jdbc.Driver");
        String URL = "jdbc:mysql://localhost:3306/TIA102G3?serverTimezone=Asia/Taipei";
        String USER = "root";
        String PASSWORD = "123456";
        Connection conn = DriverManager.getConnection(URL, USER, PASSWORD);
//        LOGGER.info("Database connection established successfully.");
        return conn;
    }
    /**
     * 一般建立連線
     * @return
     * @throws Exception
     */
//    public static Connection getConnection() throws Exception {
       /* InputStream is = ClassLoader.getSystemClassLoader().getResourceAsStream("jdbc.properties");
//        InputStream is = new FileInputStream("D:\\TIA102-WebApp\\idea_WTP_workspace1\\TIA102G3\\src\\main\\java\\jdbc.properties");
//        InputStream is = ClassLoader.getSystemClassLoader().getResourceAsStream("D:\\TIA102-WebApp\\idea_WTP_workspace1\\TIA102G3\\src\\main\\webapp\\WEB-INF\\jdbc.properties");

        Properties properties = new Properties();
        properties.load(is);

        String user = properties.getProperty("user");
        String password = properties.getProperty("password");
        String url = properties.getProperty("url");
        String driverClass = properties.getProperty("driverClass");

        Class.forName(driverClass);
        Connection conn = DriverManager.getConnection(url, user, password);*/

//        Class.forName("com.mysql.cj.jdbc.Driver");
//        String URL = "jdbc:mysql://localhost:3306/TIA102G3?serverTimezone=Asia/Taipei";
//        String USER = "root";
//        String PASSWORD = "123456";
//        Connection conn = DriverManager.getConnection(URL, USER, PASSWORD);
//
//        return conn;
//    }


//    private static ComboPooledDataSource cpds = new ComboPooledDataSource("helloc3p0");
//
//    /**
//     * c3p0連線池
//     * @return
//     * @throws SQLException
//     */
//    public static Connection getC3P0Connection() throws SQLException {
//        Connection conn = cpds.getConnection();
//        return conn;
//    }

    /**
     * 關閉資源
     * @param conn
     */
    public static void closeResource(Connection conn) {
        try {
            if (conn != null)
                conn.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 關閉資源
     * @param conn
     * @param ps
     */
    public static void closeResource(Connection conn, Statement ps) {
        try {
            if (ps != null)
                ps.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        try {
            if (conn != null)
                conn.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 關閉資源
     * @param conn
     * @param ps
     * @param rs
     */
    public static void closeResource(Connection conn, Statement ps, ResultSet rs) {
        try {
            if (rs != null)
                rs.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        try {
            if (ps != null)
                ps.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        try {
            if (conn != null)
                conn.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


//    @Test
//    public void testGetConnection() throws Exception {
//        Connection conn = getConnection();
//        System.out.println(conn);
//    }
//    @Test
//    public void testGetC3P0Connection() throws SQLException {
//        Connection conn = getC3P0Connection();
//        System.out.println(conn);
//    }
}