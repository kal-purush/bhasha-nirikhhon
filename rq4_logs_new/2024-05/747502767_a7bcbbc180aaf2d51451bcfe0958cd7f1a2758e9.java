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
import models.DichVuKhamBenhModel;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

public class DichVuKhamBenhCtrl {

    public static List<DichVuKhamBenhModel> timTatCaDichVu() throws ClassNotFoundException {
        List<DichVuKhamBenhModel> dsDichVu = new ArrayList<>();
        Connection connection = null;
        Statement statement = null;

        try {
            connection = ConnectDB.getConnection();
            String sql = "SELECT DICHVUKB.* , NHOMDICHVUKB.TenNhomDichVuKB FROM DICHVUKB "
                    + "JOIN NHOMDICHVUKB ON DICHVUKB.MaNhomDichVuKB = NHOMDICHVUKB.MaNhomDichVuKB";
            statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);

            while (resultSet.next()) {
                DichVuKhamBenhModel dv = new DichVuKhamBenhModel(
                        resultSet.getString("MaDichVuKB"),
                        resultSet.getString("TenDichVuKB"),
                        resultSet.getString("MaNhomDichVuKB"),
                        resultSet.getString("TenNhomDichVuKB"),
                        resultSet.getInt("GiaTien"),
                        resultSet.getInt("GiaBaoHiem"),
                        resultSet.getString("TrangThai"));
                dsDichVu.add(dv);
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

        return dsDichVu;
    }

    public static List<DichVuKhamBenhModel> timTatCaDichVuTheoMaNhom(String maNhomDichVuKB) throws ClassNotFoundException {
        List<DichVuKhamBenhModel> dsDichVu = new ArrayList<>();
        Connection connection = null;
        PreparedStatement statement = null;

        try {
            connection = ConnectDB.getConnection();
            String sql = "SELECT DICHVUKB.* , NHOMDICHVUKB.TenNhomDichVuKB FROM DICHVUKB "
                    + "JOIN NHOMDICHVUKB ON DICHVUKB.MaNhomDichVuKB = NHOMDICHVUKB.MaNhomDichVuKB WHERE DICHVUKB.MaNhomDichVuKB=?";
            statement = connection.prepareStatement(sql);
            statement.setString(1, maNhomDichVuKB);

            ResultSet resultSet = statement.executeQuery();

            while (resultSet.next()) {
                DichVuKhamBenhModel dv = new DichVuKhamBenhModel(
                        resultSet.getString("MaDichVuKB"),
                        resultSet.getString("TenDichVuKB"),
                        resultSet.getString("MaNhomDichVuKB"),
                        resultSet.getString("TenNhomDichVuKB"),
                        resultSet.getInt("GiaTien"),
                        resultSet.getInt("GiaBaoHiem"),
                        resultSet.getString("TrangThai"));
                dsDichVu.add(dv);
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

        return dsDichVu;
    }

    public static void ThemDichVuKhamBenh(DichVuKhamBenhModel dv) throws ClassNotFoundException {
        Connection connection = null;
        PreparedStatement statement = null;
        try {
            connection = ConnectDB.getConnection();
            String sql = "INSERT INTO DICHVUKB (MaDichVuKB, TenDichVuKB, MaNhomDichVuKB, GiaTien, GiaBaoHiem, TrangThai) VALUES (?, ?, ?, ?, ?, ?);";
            statement = connection.prepareStatement(sql);

            statement.setString(1, dv.getMaDichVuKB());
            statement.setString(2, dv.getTenDichVuKB());
            statement.setString(3, dv.getMaNhomDichVuKB());
            statement.setInt(4, dv.getGiaTien());
            statement.setInt(5, dv.getGiaBaoHiem());
            statement.setString(6, dv.getTrangThai());

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

    public static void CapNhatDichVuKhamBenh(DichVuKhamBenhModel dv) throws ClassNotFoundException {
        Connection connection = null;
        PreparedStatement statement = null;
        try {
            connection = ConnectDB.getConnection();
            String sql = "UPDATE DICHVUKB SET TenDichVuKB=?, MaNhomDichVuKB=?, GiaTien=?, GiaBaoHiem=?, TrangThai=? WHERE MaDichVuKB=?";
            statement = connection.prepareCall(sql);

            statement.setString(1, dv.getTenDichVuKB());
            statement.setString(2, dv.getMaNhomDichVuKB());
            statement.setInt(3, dv.getGiaTien());
            statement.setInt(4, dv.getGiaBaoHiem());
            statement.setString(5, dv.getTrangThai());
            statement.setString(6, dv.getMaDichVuKB());

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

    public static void XoaDichVuKhamBenh(String maDichVuKB) throws ClassNotFoundException {
        Connection connection = null;
        PreparedStatement statement = null;
        try {
            connection = ConnectDB.getConnection();
            String sql = "DELETE FROM DICHVUKB WHERE MaDichVuKB=?";
            statement = connection.prepareStatement(sql);

            statement.setString(1, maDichVuKB);

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

    public static List<DichVuKhamBenhModel> timTatCaDichVuTheoDK(String timKiem, String maNhomDichVuKB) throws ClassNotFoundException {
        List<DichVuKhamBenhModel> dsDichVu = new ArrayList<>();
        Connection connection = null;
        PreparedStatement statement = null;

        try {
            connection = ConnectDB.getConnection();
            if (!timKiem.isEmpty() && maNhomDichVuKB.equals("---Nhóm dịch vụ---")) {
                String sql = "SELECT DICHVUKB.* , NHOMDICHVUKB.TenNhomDichVuKB FROM DICHVUKB "
                        + "JOIN NHOMDICHVUKB ON DICHVUKB.MaNhomDichVuKB = NHOMDICHVUKB.MaNhomDichVuKB"
                        + "WHERE (MaDichVuKB LIKE ? OR TenDichVuKB LIKE ? OR DICHVUKB.MaNhomDichVuKB LIKE ? OR TenNhomDichVuKB LIKE ?)";

                statement = connection.prepareStatement(sql);
                statement.setString(1, "%" + timKiem + "%");
                statement.setString(2, "%" + timKiem + "%");
                statement.setString(3, "%" + timKiem + "%");
                statement.setString(4, "%" + timKiem + "%");

                ResultSet resultSet = statement.executeQuery();

                while (resultSet.next()) {
                    DichVuKhamBenhModel dv = new DichVuKhamBenhModel(
                            resultSet.getString("MaDichVuKB"),
                            resultSet.getString("TenDichVuKB"),
                            resultSet.getString("MaNhomDichVuKB"),
                            resultSet.getString("TenNhomDichVuKB"),
                            resultSet.getInt("GiaTien"),
                            resultSet.getInt("GiaBaoHiem"),
                            resultSet.getString("TrangThai"));
                    dsDichVu.add(dv);
                }
            } else if (timKiem.isEmpty() && !maNhomDichVuKB.equals("---Nhóm dịch vụ---")) {
                String sql = "SELECT DICHVUKB.* , NHOMDICHVUKB.TenNhomDichVuKB FROM DICHVUKB "
                        + "JOIN NHOMDICHVUKB ON DICHVUKB.MaNhomDichVuKB = NHOMDICHVUKB.MaNhomDichVuKB"
                        + "WHERE DICHVUKB.MaNhomDichVuKB = ?";

                statement = connection.prepareStatement(sql);
                statement.setString(1, maNhomDichVuKB);

                ResultSet resultSet = statement.executeQuery();

                while (resultSet.next()) {
                    DichVuKhamBenhModel dv = new DichVuKhamBenhModel(
                            resultSet.getString("MaDichVuKB"),
                            resultSet.getString("TenDichVuKB"),
                            resultSet.getString("MaNhomDichVuKB"),
                            resultSet.getString("TenNhomDichVuKB"),
                            resultSet.getInt("GiaTien"),
                            resultSet.getInt("GiaBaoHiem"),
                            resultSet.getString("TrangThai"));
                    dsDichVu.add(dv);
                }
            } else if (!timKiem.isEmpty() && !maNhomDichVuKB.equals("---Nhóm dịch vụ---")) {
                String sql = "SELECT DICHVUKB.* , NHOMDICHVUKB.TenNhomDichVuKB FROM DICHVUKB "
                        + "JOIN NHOMDICHVUKB ON DICHVUKB.MaNhomDichVuKB = NHOMDICHVUKB.MaNhomDichVuKB"
                        + "WHERE (MaDichVuKB LIKE ? OR TenDichVuKB LIKE ? OR DICHVUKB.MaNhomDichVuKB LIKE ? OR TenNhomDichVuKB LIKE ?) AND DICHVUKB.MaNhomDichVuKB = ?";

                statement = connection.prepareStatement(sql);
                statement.setString(1, "%" + timKiem + "%");
                statement.setString(2, "%" + timKiem + "%");
                statement.setString(3, "%" + timKiem + "%");
                statement.setString(4, "%" + timKiem + "%");
                statement.setString(5, maNhomDichVuKB);

                ResultSet resultSet = statement.executeQuery();

                while (resultSet.next()) {
                    DichVuKhamBenhModel dv = new DichVuKhamBenhModel(
                            resultSet.getString("MaDichVuKB"),
                            resultSet.getString("TenDichVuKB"),
                            resultSet.getString("MaNhomDichVuKB"),
                            resultSet.getString("TenNhomDichVuKB"),
                            resultSet.getInt("GiaTien"),
                            resultSet.getInt("GiaBaoHiem"),
                            resultSet.getString("TrangThai"));
                    dsDichVu.add(dv);
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

        return dsDichVu;
    }

    public static void exportToExcel(List<DichVuKhamBenhModel> dsDichVu, String filePath) {
        try (Workbook workbook = new XSSFWorkbook()) {
            Sheet sheet = workbook.createSheet("DanhSachDichVuKhamBenh");

            // Tạo header
            Row headerRow = sheet.createRow(0);
            headerRow.createCell(0).setCellValue("MaDichVuKB");
            headerRow.createCell(1).setCellValue("TenDichVuKB");
            headerRow.createCell(2).setCellValue("MaNhomDichVuKB");
            headerRow.createCell(3).setCellValue("TenNhomDichVuKB");
            headerRow.createCell(4).setCellValue("GiaTien");
            headerRow.createCell(5).setCellValue("GiaBaoHiem");
            headerRow.createCell(6).setCellValue("TrangThai");

            // Ghi dữ liệu vào sheet
            int rowNum = 1;
            for (DichVuKhamBenhModel dv : dsDichVu) {
                Row row = sheet.createRow(rowNum++);
                row.createCell(0).setCellValue(dv.getMaDichVuKB());
                row.createCell(1).setCellValue(dv.getTenDichVuKB());
                row.createCell(2).setCellValue(dv.getMaNhomDichVuKB());
                row.createCell(3).setCellValue(dv.getTenNhomDichVuKB());
                row.createCell(4).setCellValue(dv.getGiaTien());
                row.createCell(5).setCellValue(dv.getGiaBaoHiem());
                row.createCell(6).setCellValue(dv.getTrangThai());
            }

            // Xuất workbook ra file Excel
            try (FileOutputStream fileOut = new FileOutputStream(filePath)) {
                workbook.write(fileOut);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String generateMaDichVuKhamBenh() {
        Date now = new Date();

        SimpleDateFormat dateFormat = new SimpleDateFormat("mmss");
        String timeString = dateFormat.format(now);

        Random random = new Random();
        int randomNumber = random.nextInt(10000);

        String randomString = "DVKB" + timeString + randomNumber;
        return randomString;
    }
}