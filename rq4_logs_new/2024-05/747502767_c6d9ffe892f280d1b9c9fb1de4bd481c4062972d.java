package controllers;

import database.ConnectDB;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;
import models.PhongKhamModel;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

public class PhongKhamCtrl {

    public static List<PhongKhamModel> timTatCaPhongKham() throws ClassNotFoundException {
        List<PhongKhamModel> dsPhongKham = new ArrayList<>();
        Connection connection = null;
        Statement statement = null;

        try {
            connection = ConnectDB.getConnection();
            String sql = "SELECT * FROM PHONGKHAM";
            statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);

            while (resultSet.next()) {
                PhongKhamModel pk = new PhongKhamModel(
                        resultSet.getString("MaPhongKham"),
                        resultSet.getString("TenPhongKham"),
                        resultSet.getString("TrangThai"),
                        resultSet.getInt("SoLuongBenhNhan"));
                dsPhongKham.add(pk);
            }
        } catch (SQLException ex) {
            Logger.getLogger(BenhNhanCtrl.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException ex) {
                    Logger.getLogger(BenhNhanCtrl.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException ex) {
                    Logger.getLogger(BenhNhanCtrl.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }

        return dsPhongKham;
    }

    public static void ThemPhongKham(PhongKhamModel pk) throws ClassNotFoundException {
        Connection connection = null;
        PreparedStatement statement = null;
        try {
            connection = ConnectDB.getConnection();
            String sql = "INSERT INTO PHONGKHAM (MaPhongKham, TenPhongKham, TrangThai, SoLuongBenhNhan) VALUES (?, ?, ?, ?);";
            statement = connection.prepareStatement(sql);

            statement.setString(1, pk.getMaPhongKham());
            statement.setString(2, pk.getTenPhongKham());
            statement.setString(3, pk.getTrangThai());
            statement.setInt(4, pk.getSoLuong());

            statement.executeUpdate();

        } catch (SQLException ex) {
            Logger.getLogger(BenhNhanCtrl.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException ex) {
                    Logger.getLogger(BenhNhanCtrl.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException ex) {
                    Logger.getLogger(BenhNhanCtrl.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
    }

    public static void CapNhatPhongKham(PhongKhamModel pk) throws ClassNotFoundException {
        Connection connection = null;
        PreparedStatement statement = null;
        try {
            connection = ConnectDB.getConnection();
            String sql = "UPDATE PHONGKHAM SET TenPhongKham=?, TrangThai=? WHERE MaPhongKham=?";
            statement = connection.prepareCall(sql);

            statement.setString(1, pk.getTenPhongKham());
            statement.setString(2, pk.getTrangThai());
            statement.setString(3, pk.getMaPhongKham());

            statement.executeUpdate();
        } catch (SQLException ex) {
            Logger.getLogger(BenhNhanCtrl.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException ex) {
                    Logger.getLogger(BenhNhanCtrl.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException ex) {
                    Logger.getLogger(BenhNhanCtrl.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
    }

    public static List<PhongKhamModel> laySoLuong() throws ClassNotFoundException {
        List<PhongKhamModel> dsPhongKham = new ArrayList<>();
        Connection connection = null;
        Statement statement = null;

        try {
            connection = ConnectDB.getConnection();
            String sql = "SELECT MaPhongKham, COUNT(MaPhongKham) SoLuongBenhNhan FROM DANGKY WHERE TrangThai=N'Đợi khám' GROUP BY MaPhongKham ";
            statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);

            while (resultSet.next()) {
                PhongKhamModel pk = new PhongKhamModel(
                        resultSet.getString("MaPhongKham"),
                        resultSet.getInt("SoluongBenhNhan"));
                dsPhongKham.add(pk);
            }
        } catch (SQLException ex) {
            Logger.getLogger(BenhNhanCtrl.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException ex) {
                    Logger.getLogger(BenhNhanCtrl.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException ex) {
                    Logger.getLogger(BenhNhanCtrl.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }

        return dsPhongKham;
    }

    public static void capNhatSoLuong() throws ClassNotFoundException {
        Connection connection = null;
        PreparedStatement statement = null;
        List<PhongKhamModel> dsSoLuong = laySoLuong();

        try {
            connection = ConnectDB.getConnection();

            for (PhongKhamModel sl : dsSoLuong) {
                try {
                    String sql = "UPDATE PHONGKHAM SET SoLuongBenhNhan=? WHERE MaPhongKham=?";
                    try (PreparedStatement updateStatement = connection.prepareStatement(sql)) {
                        updateStatement.setInt(1, sl.getSoLuong());
                        updateStatement.setString(2, sl.getMaPhongKham());
                        updateStatement.executeUpdate();
                    }
                } catch (SQLException ex) {
                    Logger.getLogger(PhongKhamCtrl.class.getName()).log(Level.SEVERE, null, ex);
                }
            }

        } catch (SQLException ex) {
            Logger.getLogger(BenhNhanCtrl.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException ex) {
                    Logger.getLogger(BenhNhanCtrl.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException ex) {
                    Logger.getLogger(BenhNhanCtrl.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
    }

    public static void capNhatLaiSoLuong() throws ClassNotFoundException {
        Connection connection = null;
        PreparedStatement statement = null;
        List<PhongKhamModel> dsPhongKham = timTatCaPhongKham();

        try {
            connection = ConnectDB.getConnection();

            for (PhongKhamModel pk : dsPhongKham) {
                try {
                    String sql = "UPDATE PHONGKHAM SET SoLuongBenhNhan=0 WHERE MaPhongKham=?";
                    statement = connection.prepareStatement(sql);
                    statement.setString(1, pk.getMaPhongKham());
                    statement.executeUpdate();

                } catch (SQLException ex) {
                    Logger.getLogger(PhongKhamCtrl.class.getName()).log(Level.SEVERE, null, ex);
                }
            }

        } catch (SQLException ex) {
            Logger.getLogger(BenhNhanCtrl.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException ex) {
                    Logger.getLogger(BenhNhanCtrl.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException ex) {
                    Logger.getLogger(BenhNhanCtrl.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
    }

    public static void ThemSoLuong(String maPhongKham) throws ClassNotFoundException {
        Connection connection = null;
        PreparedStatement statement = null;
        try {
            connection = ConnectDB.getConnection();

            String sql = "UPDATE PHONGKHAM SET SoLuongBenhNhan = (CASE WHEN SoLuongBenhNhan + 1 = 51 THEN 0 ELSE SoLuongBenhNhan + 1 END) WHERE MaPhongKham=?";
            statement = connection.prepareStatement(sql);
            statement.setString(1, maPhongKham);

            statement.executeUpdate();
        } catch (SQLException ex) {
            Logger.getLogger(BenhNhanCtrl.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException ex) {
                    Logger.getLogger(BenhNhanCtrl.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException ex) {
                    Logger.getLogger(BenhNhanCtrl.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
    }

    public static void BotSoLuong(String maPhongKham) throws ClassNotFoundException {
        Connection connection = null;
        PreparedStatement statement = null;
        try {
            connection = ConnectDB.getConnection();
            String sql = "UPDATE PHONGKHAM SET SoLuongBenhNhan=SoLuongBenhNhan - 1 WHERE MaPhongKham=?";
            statement = connection.prepareStatement(sql);
            statement.setString(1, maPhongKham);

            statement.executeUpdate();
        } catch (SQLException ex) {
            Logger.getLogger(BenhNhanCtrl.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException ex) {
                    Logger.getLogger(BenhNhanCtrl.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException ex) {
                    Logger.getLogger(BenhNhanCtrl.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
    }

    public static void XoaPhongKham(String maPhongKham) throws ClassNotFoundException {
        Connection connection = null;
        PreparedStatement statement = null;
        try {
            connection = ConnectDB.getConnection();
            String sql = "DELETE FROM PHONGKHAM WHERE MaPhongKham=?";
            statement = connection.prepareStatement(sql);

            statement.setString(1, maPhongKham);

            statement.executeUpdate();

        } catch (SQLException ex) {
            Logger.getLogger(BenhNhanCtrl.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException ex) {
                    Logger.getLogger(BenhNhanCtrl.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException ex) {
                    Logger.getLogger(BenhNhanCtrl.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
    }

    public static List<PhongKhamModel> timTatCaDichVuTheoDK(String timKiem) throws ClassNotFoundException {
        List<PhongKhamModel> dsPhongKham = new ArrayList<>();
        Connection connection = null;
        PreparedStatement statement = null;

        try {
            connection = ConnectDB.getConnection();
            String sql = "SELECT * FROM PHONGKHAM WHERE MaPhongKham LIKE ? OR TenPhongKham LIKE ?";

            statement = connection.prepareStatement(sql);
            statement.setString(1, "%" + timKiem + "%");
            statement.setString(2, "%" + timKiem + "%");

            ResultSet resultSet = statement.executeQuery();

            while (resultSet.next()) {
                PhongKhamModel pk = new PhongKhamModel(
                        resultSet.getString("MaPhongKham"),
                        resultSet.getString("TenPhongKham"),
                        resultSet.getString("TrangThai"),
                        resultSet.getInt("SoluongBenhNhan"));
                dsPhongKham.add(pk);
            }
        } catch (SQLException ex) {
            Logger.getLogger(BenhNhanCtrl.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException ex) {
                    Logger.getLogger(BenhNhanCtrl.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException ex) {
                    Logger.getLogger(BenhNhanCtrl.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }

        return dsPhongKham;
    }

    public static void exportToExcel(List<PhongKhamModel> dsDichVu, String filePath) {
        try (Workbook workbook = new XSSFWorkbook()) {
            Sheet sheet = workbook.createSheet("DanhSachDichVuKhamBenh");

            // Tạo header
            Row headerRow = sheet.createRow(0);
            headerRow.createCell(0).setCellValue("MaPhongKham");
            headerRow.createCell(1).setCellValue("TenPhongKham");
            headerRow.createCell(2).setCellValue("TrangThai");

            // Ghi dữ liệu vào sheet
            int rowNum = 1;
            for (PhongKhamModel dv : dsDichVu) {
                Row row = sheet.createRow(rowNum++);
                row.createCell(0).setCellValue(dv.getMaPhongKham());
                row.createCell(1).setCellValue(dv.getTenPhongKham());
                row.createCell(2).setCellValue(dv.getTrangThai());
            }

            // Xuất workbook ra file Excel
            try (FileOutputStream fileOut = new FileOutputStream(filePath)) {
                workbook.write(fileOut);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String generateMaPhongKham() {
        Date now = new Date();

        SimpleDateFormat dateFormat = new SimpleDateFormat("mmss");
        String timeString = dateFormat.format(now);

        Random random = new Random();
        int randomNumber = random.nextInt(10000);

        String randomString = "PK" + timeString + randomNumber;
        return randomString;
    }
}