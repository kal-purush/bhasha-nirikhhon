/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.mycompany.tp_mod_10_1302213089_muhammadvikhanmuharram;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 *
 * @author hp
 */
public class Orang {
    private String nama;
    private String[] waktuDatang = new String[10];
    private String[] asalJanji = new String[10];
    private int jumlahJanji;
    private Prokastinasi sifatNgaret;
    
    public Orang(String nama, int ngaretJam, int ngaretMenit){
        this.nama = nama;
        sifatNgaret = new Prokastinasi();
        sifatNgaret.ngaretJam = ngaretJam;
        sifatNgaret.ngaretMenit = ngaretMenit;
    }
    
    public class Prokastinasi {
        private int ngaretJam;
        private int ngaretMenit;
        
        public int waktuSiapSiap(String input){
            if (input == "pacar"){
                return 20;
            } else if (input == "temen"){
                return 35;
            } else if (input == "bos"){
                return 10;
            }
            return 45;
        }
        
        public int waktuKarenaLupa(){
            if (jumlahJanji < 3){
                return 0;
            } else if (jumlahJanji >= 3 && jumlahJanji < 5){
                return 15;
            }
            return 45;
        }
    }
    
    public void tambahJanji(LocalDateTime waktuJanji, String asalJanji){
        String waktuDatangBaru = TanggalHelper.tambahMenit(waktuJanji, 
                sifatNgaret.ngaretJam * TanggalHelper.WAKTU_MACET + 
                sifatNgaret.ngaretMenit + sifatNgaret.waktuSiapSiap(asalJanji)
                + sifatNgaret.waktuKarenaLupa());
        this.asalJanji[jumlahJanji] = asalJanji;
        waktuDatang[jumlahJanji] = waktuDatangBaru;
        jumlahJanji++;
    }
    
    public void tampilkanSemuaWaktuDatang() {
        System.out.println("Nama pejanji: "+this.nama);
        System.out.println("ngaretJam: "+this.sifatNgaret.ngaretJam);
        System.out.println("ngaretMenit: "+this.sifatNgaret.ngaretMenit);
        System.out.println("Daftar Waktu Datang: ");
        for (int i = 0; i < jumlahJanji; i++) {
            System.out.println("Janji dengan "+asalJanji[i]+" datang pada " + waktuDatang[i]);
        }
    }
}