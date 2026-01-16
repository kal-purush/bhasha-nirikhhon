package session3.lab1;

import java.util.Scanner;

public class GiaoVien extends Person {
    public String danhSachLop;
    public String mucLuong;
    public  int soMonDay;

    public String getDanhSachLop() {
        return danhSachLop;
    }

    public void setDanhSachLop(String danhSachLop) {
        this.danhSachLop = danhSachLop;
    }

    public String getMucLuong() {
        return mucLuong;
    }

    public void setMucLuong(String mucLuong) {
        this.mucLuong = mucLuong;
    }

    public int getSoMonDay() {
        return soMonDay;
    }

    public void setSoMonDay(int soMonDay) {
        this.soMonDay = soMonDay;
    }

    public void nhapThongTin(){
        Scanner sc = new Scanner(System.in);
        super.nhapThongTin();
        System.out.println("Danh sach lop giang day: ");
        danhSachLop = sc.nextLine();
        System.out.println("Muc luong: ");
        mucLuong = sc.nextLine();
        System.out.println("So mon giang day: ");
        soMonDay = sc.nextInt();
    }

    public void inThongTin() {
        super.inThongTin();
        System.out.println("Danh sach lop: "+this.danhSachLop);
        System.out.println("Muc luong: "+this.mucLuong);
        System.out.println("So mon giang day: "+this.soMonDay);
    }
}