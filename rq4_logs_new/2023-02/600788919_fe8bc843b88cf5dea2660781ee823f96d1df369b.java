package lab3;
import java .util.Scanner;
public class Nhanvien {
    private String tenNhanVien;
    private double luongCoBan;
    private double heSoLuong;
    private double luongMax;

    public Nhanvien() {
        this.tenNhanVien = null;
        this.luongCoBan = 0;
        this.heSoLuong = 0;
        this.luongMax = 0;
    }

    public Nhanvien(String tenNhanVien, double luongCoBan, double heSoLuong, double luongMax) {
        this.tenNhanVien = tenNhanVien;
        this.luongCoBan = luongCoBan;
        this.heSoLuong = heSoLuong;
        this.luongMax = luongMax;
    }

    public String getTenNhanVien() {
        return this.tenNhanVien;
    }

    public void setTenNhanVien(String tenNhanVien) {
        this.tenNhanVien = tenNhanVien;
    }

    public double getLuongCoBan() {
        return this.luongCoBan;
    }

    public void setLuongCoBan(double luongCoBan) {
        this.luongCoBan = luongCoBan;
    }

    public double getHeSoLuong() {
        return this.heSoLuong;
    }

    public void setHeSoLuong(double heSoLuong) {
        this.heSoLuong = heSoLuong;
    }

    public double getLuongMax() {
        return this.luongMax;
    }

    public void setLuongMax(double luongMax) {
        this.luongMax = luongMax;
    }

    public double tinhLuong() {
        double luong = this.luongCoBan * this.heSoLuong;
        return luong;
    }

    public boolean tangLuong(double a) {
        setHeSoLuong(a);
        if (tinhLuong() > this.luongMax){
            return false;
        } else {
            return true;
        }
    }

    public void inThongTin() {
        System.out.println("ten nhan vien " + tenNhanVien + "luong co ban" + luongCoBan + "he so luong" + heSoLuong
                + "luong toi da" + luongMax);
    }
}