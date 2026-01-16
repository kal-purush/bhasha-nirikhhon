/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package bot_card;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 *
 * @author N ~ N
 */
public class DBConnection {

    private static final String URL = "jdbc:mysql://localhost:3306/botdb";
    private static final String USER = "root";
    private static final String PASSWORD = "";

    public static Connection connect() {
        try {
            // Kết nối MySQL
            return DriverManager.getConnection(URL, USER, PASSWORD);
        } catch (SQLException e) {
            System.err.println("Không thể kết nối tới MySQL!");
            e.printStackTrace();
            return null;
        }
    }

    public static void main(String[] args) throws SQLException {
        try (Connection conn = connect()) {
            if (conn != null) {
                System.out.println("Kết nối MySQL thành công!");
                System.out.println(conn.getCatalog());
            }
        }
    }

    public static boolean UserInfo(String id, String name, String address, String dob, String licensePlate, String publicKey) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        String sql = "INSERT INTO users (id, name, address, dob, license_plate_number, public_key) VALUES (?, ?, ?, ?, ?, ?)";

        try {
            // Lấy kết nối
            conn = DBConnection.connect();
            if (conn == null) {
                System.err.println("Kết nối cơ sở dữ liệu thất bại!");
                return false;
            }
            System.out.println("Connect Db");

            // Chuẩn bị câu lệnh SQL
            pstmt = conn.prepareStatement(sql);
            System.out.println("Prepare sql");

            // Gán giá trị cho các tham số
            pstmt.setString(1, id);
            pstmt.setString(2, name);
            pstmt.setString(3, address);
            pstmt.setString(4, dob);
            pstmt.setString(5, licensePlate);
            pstmt.setString(6, publicKey); // Thêm giá trị public_key
            System.out.println("prepare done");

            // Thực thi câu lệnh
            int rows = pstmt.executeUpdate();
            System.out.println("execute done");

            return rows > 0; // Trả về true nếu có ít nhất một dòng được thêm
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("Error");
            return false;
        } finally {
            // Đóng kết nối
            try {
                if (pstmt != null) {
                    pstmt.close();
                }
                if (conn != null) {
                    conn.close();
                }
                System.out.println("Close");
            } catch (SQLException ex) {
                ex.printStackTrace();
                System.out.println("Close Failed");
            }
        }
    }

//    public static String getPublicKey(String id) {
//        String publicKey = null; // Biến để lưu khóa công khai
//        String sql = "SELECT public_key FROM users WHERE id = ?";
//
//        // Kết nối cơ sở dữ liệu và thực thi truy vấn
//        try (Connection conn = DBConnection.connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
//
//            // Kiểm tra kết nối
//            if (conn == null || conn.isClosed()) {
//                System.err.println("Không thể kết nối cơ sở dữ liệu!");
//                return null;
//            }
//            System.out.println("kiem tra publickey");
//            // Gán giá trị cho tham số trong câu lệnh SQL
//            ps.setString(1, id);
//            System.out.println("Truy van khoa cong khai voi ID: " + id); // Log ID
//
//            // Thực thi truy vấn
//            try (ResultSet rs = ps.executeQuery()) {
//                if (rs.next()) {
//                    publicKey = rs.getString("public_key");
//                    System.out.println("Khoa cong khai tim thay: " + publicKey);
//                } else {
//                    System.err.println("Khong tim thay khoa cong khai cho ID: " + id);
//                }
//            }
//
//        } catch (SQLException e) {
//            // Xử lý lỗi SQL
//            System.err.println("Lỗi truy vấn cơ sở dữ liệu: " + e.getMessage());
//        }
//
//        // Kiểm tra nếu không tìm thấy khóa công khai
//        if (publicKey == null) {
//            System.err.println("Khoa cong khai tra ve null. Vui long kiem tra ID: " + id);
//        }
//
//        return publicKey;
//    }

    public static boolean updateUserInfo(String id, String name, String address, String dob, String licensePlate) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        String sql = "UPDATE Users SET name = ?, dob = ?, address = ?, license_plate_number = ? WHERE id = ?";

        try {
            conn = DBConnection.connect(); // Hàm lấy kết nối tới DB
            pstmt = conn.prepareStatement(sql);

            // Gán giá trị cho các tham số
            pstmt.setString(1, name);
            pstmt.setString(2, dob);
            pstmt.setString(3, address);
            pstmt.setString(4, licensePlate);
            pstmt.setString(5, id); // Sử dụng id ở cuối để khớp với WHERE

            // Thực thi câu lệnh
            int rowsUpdated = pstmt.executeUpdate();

            System.out.println("Rows updated: " + rowsUpdated);
            return rowsUpdated > 0; // Trả về true nếu có ít nhất một dòng được cập nhật
        } catch (SQLException e) {
            e.printStackTrace();
            return false; // Trả về false nếu có lỗi xảy ra
        } finally {
            // Đóng kết nối
            try {
                if (pstmt != null) {
                    pstmt.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }
    }

}