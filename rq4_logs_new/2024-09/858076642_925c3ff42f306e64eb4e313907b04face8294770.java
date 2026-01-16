package Tugas5;
import java.util.Scanner;

public class BeratBadanIMT {

    public static void main(String[] args) {
       Scanner bb = new Scanner(System.in);
       
        System.out.print("Berat badan anda dalam bentuk (kg): ");
        double beratbadan = bb.nextDouble();
        
        System.out.print("Tinggi badan dalam bentuk (cm): ");
        double tinggi = bb.nextDouble();
        
        double tinggi1 = tinggi/100.0;
        double IMT = beratbadan/(tinggi1*tinggi1);
        String Kriteria;
        
        if(IMT >= 40){
            Kriteria = "Kriteria : Sangat Gemuk";
        }else if(IMT >= 30){
            Kriteria = "Kriteria : Gemuk";
        }else if(IMT >= 25){
            Kriteria = "Kriteria : Berat badan lebih";
        }else if(IMT >= 18.5){
            Kriteria = "Kriteria : Berat badan ideal";
        }else{
            Kriteria = "Kriteria : Berat badan kurang";
        }
        
        System.out.println("==================================");
        System.out.println("Berat badan anda: " + beratbadan);
        System.out.println("Tinggi badan anda: "+ tinggi);
        System.out.println("Nilai IMT: " +IMT);
        System.out.println("Kriteria: " +Kriteria);
    }
    
}