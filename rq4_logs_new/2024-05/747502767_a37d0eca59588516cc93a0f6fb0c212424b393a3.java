package controllers;

import database.ConnectDB;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import models.TaiKhoanCaNhanModel;

public class TaiKhoanCaNhanCtrl {

    public static String currentEmail = null;
    public static String maChucVu = null;

    public static String layTenChucVu(String maChucVu) throws ClassNotFoundException {
        String tenChucVu = null;
        String sql = "SELECT * FROM ChucVu WHERE MaChucVu = ?";
        try (Connection connection = ConnectDB.getConnection(); PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, maChucVu);
            ResultSet resultSet = statement.executeQuery();
            if (resultSet.next()) {
                tenChucVu = resultSet.getString("TenChucVu");
            }
        } catch (SQLException ex) {
            Logger.getLogger(TaiKhoanCaNhanCtrl.class.getName()).log(Level.SEVERE, null, ex);
        }
        return tenChucVu;
    }

    public static TaiKhoanCaNhanModel hienThiThongTinCaNhan() throws ClassNotFoundException {
        TaiKhoanCaNhanModel taiKhoan = null;
        if (maChucVu != null) {
            if (maChucVu.equalsIgnoreCase("BS")) {
                taiKhoan = hienThiThongTinCaNhanBacSi();
                return taiKhoan;
            } else if (maChucVu.equalsIgnoreCase("YT")) {
                taiKhoan = hienThiThongTinCaNhanYTa();
                return taiKhoan;
            } else if (maChucVu.equalsIgnoreCase("QL")) {
                taiKhoan = hienThiThongTinCaNhanQuanLy();
                return taiKhoan;
            }
        }
        return null;
    }

    public static TaiKhoanCaNhanModel hienThiThongTinCaNhanBacSi() throws ClassNotFoundException {
        TaiKhoanCaNhanModel taiKhoan = null;
        String sql = "SELECT BS.MaBacSi, BS.HoTen, BS.GioiTinh, BS.NamSinh, BS.DiaChi, "
                + "BS.SoDienThoai, BS.CanCuoc, BS.TrinhDo, BS.Anh, BS.TrangThaiXoa, "
                + "TK.MaChucVu, TK.MatKhau, TK.Email "
                + "FROM TAIKHOAN AS TK "
                + "JOIN BACSI AS BS ON TK.Email = BS.Email "
                + "WHERE BS.Email = ?";
        try (Connection connection = ConnectDB.getConnection(); PreparedStatement statement = connection.prepareStatement(sql)) {

            statement.setString(1, currentEmail);
            ResultSet resultSet = statement.executeQuery();
            String tenChucVu = layTenChucVu(maChucVu);
            while (resultSet.next()) {
                if (!resultSet.getBoolean("TrangThaiXoa")) {
                    TaiKhoanCaNhanModel tk = new TaiKhoanCaNhanModel(
                            resultSet.getString("MaBacSi"),
                            resultSet.getString("MaChucVu"),
                            resultSet.getString("MatKhau"),
                            resultSet.getString("Email"),
                            resultSet.getString("HoTen"),
                            resultSet.getString("GioiTinh"),
                            resultSet.getString("NamSinh"),
                            resultSet.getString("DiaChi"),
                            resultSet.getString("SoDienThoai"),
                            resultSet.getString("CanCuoc"),
                            resultSet.getString("TrinhDo"),
                            resultSet.getString("Anh")
                    );
                    tk.setTenChucVu(tenChucVu);
                    taiKhoan = tk;
                }
                return taiKhoan;
            }
        } catch (SQLException ex) {
            Logger.getLogger(TaiKhoanCaNhanCtrl.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }

    public static TaiKhoanCaNhanModel hienThiThongTinCaNhanYTa() throws ClassNotFoundException {
        TaiKhoanCaNhanModel taiKhoan = null;
        String sql = "SELECT YT.MaYTa, YT.HoTen, YT.GioiTinh, YT.NamSinh, YT.DiaChi, "
                + "YT.SoDienThoai, YT.CanCuoc, YT.Anh, YT.TrangThaiXoa, "
                + "TK.MaChucVu, TK.MatKhau, TK.Email "
                + "FROM TAIKHOAN AS TK "
                + "JOIN YTA AS YT ON TK.Email = YT.Email "
                + "WHERE YT.Email = ?";
        try (Connection connection = ConnectDB.getConnection(); PreparedStatement statement = connection.prepareStatement(sql)) {

            statement.setString(1, currentEmail);
            ResultSet resultSet = statement.executeQuery();
            String tenChucVu = layTenChucVu(maChucVu);
            while (resultSet.next()) {
                if (!resultSet.getBoolean("TrangThaiXoa")) {
                    TaiKhoanCaNhanModel tk = new TaiKhoanCaNhanModel(
                            resultSet.getString("MaYTa"),
                            resultSet.getString("MaChucVu"),
                            resultSet.getString("MatKhau"),
                            resultSet.getString("Email"),
                            resultSet.getString("HoTen"),
                            resultSet.getString("GioiTinh"),
                            resultSet.getString("NamSinh"),
                            resultSet.getString("DiaChi"),
                            resultSet.getString("SoDienThoai"),
                            resultSet.getString("CanCuoc"),
                            resultSet.getString("Anh")
                    );
                    String trinhDo = "Y tá tổng hợp";
                    tk.setTrinhDo(trinhDo);
                    tk.setTenChucVu(tenChucVu);
                    taiKhoan = tk;
                }
            }
        } catch (SQLException ex) {
            Logger.getLogger(TaiKhoanCaNhanCtrl.class.getName()).log(Level.SEVERE, null, ex);
        }
        return taiKhoan;
    }

    public static TaiKhoanCaNhanModel hienThiThongTinCaNhanQuanLy() throws ClassNotFoundException {
        TaiKhoanCaNhanModel taiKhoan = null;
        String sql = "SELECT QL.MaQuanLy, QL.HoTen, QL.GioiTinh, QL.NamSinh, QL.DiaChi, "
                + "QL.SoDienThoai, QL.CanCuoc, QL.Anh, QL.TrangThaiXoa, "
                + "TK.MaChucVu, TK.MatKhau, TK.Email "
                + "FROM TAIKHOAN AS TK "
                + "JOIN QUANLY AS QL ON TK.Email = QL.Email "
                + "WHERE QL.Email = ?";
        try (Connection connection = ConnectDB.getConnection(); PreparedStatement statement = connection.prepareStatement(sql)) {

            statement.setString(1, currentEmail);
            ResultSet resultSet = statement.executeQuery();
            String tenChucVu = layTenChucVu(maChucVu);
            while (resultSet.next()) {
                if (!resultSet.getBoolean("TrangThaiXoa")) {
                    TaiKhoanCaNhanModel tk = new TaiKhoanCaNhanModel(
                            resultSet.getString("MaQuanLy"),
                            resultSet.getString("MaChucVu"),
                            resultSet.getString("MatKhau"),
                            resultSet.getString("Email"),
                            resultSet.getString("HoTen"),
                            resultSet.getString("GioiTinh"),
                            resultSet.getString("NamSinh"),
                            resultSet.getString("DiaChi"),
                            resultSet.getString("SoDienThoai"),
                            resultSet.getString("CanCuoc"),
                            resultSet.getString("Anh")
                    );
                    String trinhDo = "Quản lý phòng khám";
                    tk.setTrinhDo(trinhDo);
                    tk.setTenChucVu(tenChucVu);
                    taiKhoan = tk;
                }
            }
        } catch (SQLException ex) {
            Logger.getLogger(TaiKhoanCaNhanCtrl.class.getName()).log(Level.SEVERE, null, ex);
        }
        return taiKhoan;
    }

}