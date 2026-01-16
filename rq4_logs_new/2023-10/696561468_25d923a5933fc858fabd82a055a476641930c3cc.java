package com.fakhri.praktikum.abstraction;

abstract class Hewan {
    public String nama;
    public abstract void habitatHewan();

    // error: abstract methods cannot have a body
//    public abstract void namaHewan(){
//        System.out.println("\nMethod ini berada di abstractclass Hewan");
//        System.out.println("Nama hewan ini adalah " + nama);
//    }

    // correct way
    public abstract void namaHewan();

    // Kesimpulan
    // Abstract class adalah class yang tidak bisa diinstansiasi (tidak bisa dibuat objeknya)
    // Abstract class hanya bisa dijadikan sebagai parent class (super class) untuk class lain
}