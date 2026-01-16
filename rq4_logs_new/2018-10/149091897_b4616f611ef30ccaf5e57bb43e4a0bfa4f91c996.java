package bai1;

import java.util.Date;

/**
 *
 * @author Nguyễn Quang Hiệp
 * class test
 */
public class TestDriverBai1 {
    public static void main(String[] args) {
        HoaQua hoaQua = new HoaQua();
        QuaCam quaCam = new QuaCam();
        QuaTao quaTao = new QuaTao();
        CamCaoPhong camCaoPhong  = new CamCaoPhong();
        CamSanh camSanh = new CamSanh();
        camSanh.setDateAdded(new Date(2018-1900, 9, 8));
        camSanh.setExpirationDate(new Date(2018-1900, 10, 8));
        camSanh.setAmount(10);
        camSanh.setPrice(100000);
        camSanh.setSource("Viet Nam");
        camSanh.setFlavor("Ngọt");
        System.out.println("camSanh");
        System.out.println("Số lượng " + camSanh.getAmount() + "kg");
        System.out.println("Giá " + camSanh.getPrice() + "vnd/1kg");
        System.out.println("Nguồn gốc " + camSanh.getSource());
        System.out.println("Hương vị " + camSanh.getFlavor());
        System.out.println("Ngày ngập " + camSanh.getDateAdded());
        System.out.println("Ngày hết hạn " + camSanh.getExpirationDate());
    }
}