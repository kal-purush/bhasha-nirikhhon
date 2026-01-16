package session5.lab2;

import java.util.Scanner;

public class KhachHang {
    String MaKH,HoTen,NRHD;
    int SoLuong, count = 0;
    Scanner sc = new Scanner(System.in);
    public void Nhap(){
        System.out.println("Nhập Thông Tin Của Khách Hàng");
        System.out.println("\tNhập Mã Khách Hàng: ");
        MaKH = sc.nextLine();
        System.out.println("\tNhập Họ Tên: ");
        HoTen = sc.nextLine();
        System.out.println("\tNhập Ngày Ra Hoá Đơn: ");
        NRHD = sc.nextLine();
        System.out.println("\tNhập Số Lượng Tiêu Thụ: ");
        SoLuong = sc.nextInt();
        sc.nextLine();
    }
    public void IN(){
        System.out.println("Hiện Thông Tin Của Khách Hàng");
        System.out.println("\tMã Khách Hàng: " + MaKH);
        System.out.println("\tHọ Tên: " + HoTen);
        System.out.println("\tNgày Ra Hoá Đơn: " + NRHD);
        System.out.println("\tSố Lượng Tiêu Thụ: " + SoLuong);
    }
    public int DonGia(){
        if (SoLuong < 50){
            return 1000;
        }
        else if (SoLuong < 100){
            return 1200;
        }
        else if (SoLuong < 200){
            return 1500;
        }
        else {
            return 2000;
        }
    }
}