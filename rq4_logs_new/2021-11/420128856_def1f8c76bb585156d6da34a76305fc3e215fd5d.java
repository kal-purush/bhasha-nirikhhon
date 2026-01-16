package com.app.laptrinhdidong.model;

import java.io.Serializable;
import java.util.Date;

public class KhachHang implements Serializable {

    private int id;
    private String hoTenKH;
    private String tenDangNhap;
    private String matkhau;
    private String email;
    private String sdt;
    private Date ngaysinh;
    private String gioitinh;
    private String chitietdiachi;
    private XaPhuong diaChi;

    public KhachHang() {
    }

    public KhachHang(int id, String hoTenKH, String tenDangNhap, String matkhau,
                     String email, String sdt, Date ngaysinh, String gioitinh,
                     String chitietdiachi, XaPhuong diaChi) {
        this.id = id;
        this.hoTenKH = hoTenKH;
        this.tenDangNhap = tenDangNhap;
        this.matkhau = matkhau;
        this.email = email;
        this.sdt = sdt;
        this.ngaysinh = ngaysinh;
        this.gioitinh = gioitinh;
        this.chitietdiachi = chitietdiachi;
        this.diaChi = diaChi;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getHoTenKH() {
        return hoTenKH;
    }

    public void setHoTenKH(String hoTenKH) {
        this.hoTenKH = hoTenKH;
    }

    public String getTenDangNhap() {
        return tenDangNhap;
    }

    public void setTenDangNhap(String tenDangNhap) {
        this.tenDangNhap = tenDangNhap;
    }

    public String getMatkhau() {
        return matkhau;
    }

    public void setMatkhau(String matkhau) {
        this.matkhau = matkhau;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getSdt() {
        return sdt;
    }

    public void setSdt(String sdt) {
        this.sdt = sdt;
    }

    public Date getNgaysinh() {
        return ngaysinh;
    }

    public void setNgaysinh(Date ngaysinh) {
        this.ngaysinh = ngaysinh;
    }

    public String getGioitinh() {
        return gioitinh;
    }

    public void setGioitinh(String gioitinh) {
        this.gioitinh = gioitinh;
    }

    public String getChitietdiachi() {
        return chitietdiachi;
    }

    public void setChitietdiachi(String chitietdiachi) {
        this.chitietdiachi = chitietdiachi;
    }

    public XaPhuong getDiaChi() {
        return diaChi;
    }

    public void setDiaChi(XaPhuong diaChi) {
        this.diaChi = diaChi;
    }
}