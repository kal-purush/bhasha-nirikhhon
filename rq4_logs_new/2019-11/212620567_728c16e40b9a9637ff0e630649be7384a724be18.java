/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package javaapplication;

import java.util.Scanner;

/**
 *
 * @author NVT
 */
public class KySu extends Person{
    public String nganh;
    public int namTN;
    
    public void nhap(){
        Scanner scanner = new Scanner(System.in);
        System.out.print("Nhap ho ten: ");
        hoTen = scanner.nextLine();
        System.out.print("Nhap ngay sinh: ");
        ngaySinh = scanner.nextLine();
        System.out.print("Nhap que quan: ");
        queQuan = scanner.nextLine();
        System.out.print("Nhap nganh hoc: ");
        nganh = scanner.nextLine();
        System.out.print("Nhap nam tot nghiep: ");
        namTN = scanner.nextInt();
    }
    
    public void xuat(){
        System.out.println("Ho ten: " + hoTen);
        System.out.println("Ngay sinh: " + ngaySinh);
        System.out.println("Que quan: " + queQuan);
        System.out.println("Nganh hoc: " + nganh);
        System.out.println("Nam tot nghiep: " + namTN);
    }
}