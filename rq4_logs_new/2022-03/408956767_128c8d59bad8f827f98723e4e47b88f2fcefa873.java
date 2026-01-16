import java.util.Scanner;
import java.lang.Math;

public class Vke {

    public static void main(String[] args) {

        
        Scanner scan = new Scanner(System.in);

        System.out.print("Boyunuzu giriniz: ");
        int boy = scan.nextInt();

        System.out.print("Kilonuzu giriniz: ");
        int kilo = scan.nextInt();

        scan.close();

        if (boy > 5) boy /= 100;

    float sonuc = kilo / Math.pow(boy, 2)//VKE hesaplama
    System.out.println("Vücut kitle endeksiniz: ", sonuc); 
    if (sonuc < 18.49) {//Yanıtlar
      System.out.println("Zayıfsınız.");
    } else if (sonuc > 18.49 && sonuc < 25) {
      System.out.println("Normalsiniz.");
    } else if (sonuc > 24.99 && sonuc < 30) {
      System.out.println("Fazla kilolusunuz, sağlığınız için kilo verin.");
    } else if (sonuc > 29.99 && sonuc < 40) {
      System.out.println("Obezitesiniz, sağlığınız için kilo verin.");
    } else if (sonuc > 40) {
      System.out.println("İleri obezitesiniz..");
    }
        
        
    }
}