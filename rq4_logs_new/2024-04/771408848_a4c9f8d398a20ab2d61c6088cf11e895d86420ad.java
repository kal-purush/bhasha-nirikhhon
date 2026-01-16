import java.util.Scanner;

public class Hitung_Luas_Volume_Bangun {
    double sisi;
    double panjang;
    double lebar;
    double tinggi;
    double radius;
    double volume;
    double luas;
    double keliling;

   

    Scanner scanner = new Scanner(System.in);
    public static void main(String[] args) {
        Hitung_Luas_Volume_Bangun main = new Hitung_Luas_Volume_Bangun();
        main.menuUtama();
    }

    public void menuUtama() {
        System.out.println("==== BANGUN RUANG ====");
        System.out.println("1. KUBUS");
        System.out.println("2. BALOK");
        System.out.println("3. BOLA");
        System.out.println("4. TABUNG");
        System.out.println();
        System.out.println("==== BANGUN DATAR ====");
        System.out.println("5. PERSEGI");
        System.out.println("6. PERSEGI PANJANG");
        System.out.println("7. LINGKARAN");
        System.out.println("8. TRAPESIUM");
        System.out.print(">");

        int selectMenu = scanner.nextInt();
        switch (selectMenu) {
            case 1:
                Kubus kubus = new Kubus();
                kubus.hitungKubus();
                break;

            case 2:
                Balok balok = new Balok();
                balok.hitungBalok();
                break;
                
            case 3:
                Bola bola = new Bola();
                bola.hitungBola();
                break;
                
            case 4:
                Tabung tabung = new Tabung();
                tabung.hitungTabung();
                break;
                
            case 5:
                Persegi persegi = new Persegi();
                persegi.hitungPersegi();
                break;
                
            case 6:
                PersegiPanjang persegiPanjangn= new PersegiPanjang();
                persegiPanjangn.hitungPersegiPanjang();
                break;
                
            case 7:
                Lingkaran lingkaran = new Lingkaran();
                lingkaran.hitungLingkaran();
                break;
                
            case 8:
                Trapesium trapesium = new Trapesium();
                trapesium.hitungTrapesium();
                break;
                
            default:
                System.out.println("Pilihan tidak valid");
        }
    }
   
    }



class Kubus extends Hitung_Luas_Volume_Bangun {
    public double hitungKubus() {
        System.out.print("Masukkan sisi kubus: ");
        sisi = scanner.nextDouble();

        luas = 6 * sisi * sisi;
        volume = sisi * sisi * sisi;

        System.out.println("--------------------------------");
        System.out.println("Luas kubus : " + luas);
        System.out.println("Volume kubus : " + volume);
        System.out.println("--------------------------------");

        return luas;
    }
}
 
class Balok extends Hitung_Luas_Volume_Bangun{
    public double hitungBalok() {
        System.out.print("Masukkan panjang balok: ");
        panjang = scanner.nextDouble();

        System.out.print("Masukkan lebar balok: ");
        lebar = scanner.nextDouble();

        System.out.print("Masukkan tinggi balok: ");
        tinggi = scanner.nextDouble();

        volume = panjang * lebar * tinggi;
        luas = 2 * ((panjang * lebar) + (panjang * tinggi) + (lebar * tinggi));
        System.out.println("--------------------------------");
        System.out.println("Luas balok adalah : " + luas);
        System.out.println("Volume balok adalah : " + volume);
        System.out.println("--------------------------------");
        return luas;
    }

}

class Bola extends Hitung_Luas_Volume_Bangun{
    public double hitungBola() {
        System.out.print("Masukkan radius bola: ");
        radius = scanner.nextDouble();

        volume = (4.0 / 3.0) * Math.PI * Math.pow(radius, 3);

        luas = 4 * Math.PI * Math.pow(radius, 2);
        System.out.println("--------------------------------");
        System.out.println("Volume bola : " + volume);
        System.out.println("Luas bola : " + luas);
        System.out.println("--------------------------------");
        return luas;
    }
}

class Tabung extends Hitung_Luas_Volume_Bangun{
    public double hitungTabung() {
        System.out.print("Masukkan radius tabung: ");
        radius = scanner.nextDouble();
        System.out.print("Masukkan tinggi tabung: ");
        tinggi = scanner.nextDouble();
        
        luas = 2 * Math.PI * radius * (radius + tinggi);
        volume = Math.PI * Math.pow(radius, 2) * tinggi;
        System.out.println("-----------------------------------");
        System.out.println("Luas tabung : " + luas);
        System.out.println("Volume tabung : " + volume);
        System.out.println("-----------------------------------");

        return luas;
    }



}
class Persegi extends Hitung_Luas_Volume_Bangun{
    public double hitungPersegi(){
        System.out.print("Masukkan sisi persegi :" );
        sisi = scanner.nextDouble();
        System.out.println("Masukkan panjang persegi :");
        panjang = scanner.nextDouble();
        System.out.println("Masukkan lebar persegi ");
        lebar = scanner.nextDouble();
        System.out.println("Masukkan tinggi persegi");
        tinggi = scanner.nextDouble();
            
        luas = sisi * sisi;
        volume = panjang * lebar * tinggi;
        System.out.println("-----------------------------------");
        System.out.println("Luas persegi : "  + luas);
        System.out.println("Volume persgi : " + volume);
        System.out.println("-----------------------------------");
    
        return luas;
    
    }
}
class PersegiPanjang extends Hitung_Luas_Volume_Bangun{
    public double hitungPersegiPanjang(){
        System.out.print("Masukkan panjang persegi panjang : ");
        panjang = scanner.nextDouble();
        System.out.print("Masukkan lebar : ");
        lebar = scanner.nextDouble();
        
        luas = panjang * lebar;
        volume = panjang * lebar * tinggi;
        System.out.println("-----------------------------------");
        System.out.println("Luas persegi panjang : " + luas);
        System.out.println("Volume persegi panjang : " + volume);
        System.out.println("-----------------------------------");

        return luas;
    }
}
class Lingkaran extends Hitung_Luas_Volume_Bangun{
    public double hitungLingkaran(){
        System.out.print("Masukkan radius : ");
        radius = scanner.nextDouble();
        
        keliling = 2 * Math.PI*radius;
        


        luas = Math.PI * radius * radius;
        System.out.println("-----------------------------------");
        System.out.println("Luas lingkaran : " + luas);
        System.out.println("Keliling lingkaran : " + keliling);
        System.out.println("-----------------------------------");

        return luas;
        
    }

}

class Trapesium extends Hitung_Luas_Volume_Bangun{
    public double hitungTrapesium(){
        System.out.print("Masukkan sisi 1 trapesium : ");
        sisi = scanner.nextDouble();
        System.out.print("Masukkan sisi 2 trapesium : ");
        double sisi2 = scanner.nextDouble();
        System.out.print("Masukkan tinggi trapesium : ");
        tinggi = scanner.nextDouble();
        
        luas = ((sisi + sisi2) * tinggi) / 2;
        volume = luas * tinggi;
        System.out.println("-----------------------------------");
        System.out.println("Luas trapesium : " + luas);
        System.out.println("Volume trapesium : " + volume);
        System.out.println("-----------------------------------");
        return luas;

    }   
}



    



