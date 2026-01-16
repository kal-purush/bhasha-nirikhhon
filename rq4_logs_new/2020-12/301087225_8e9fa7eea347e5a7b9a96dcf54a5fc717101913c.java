import java.util.Scanner;

public class Pratikum6No1 {
    public static void main(String[] args) {
        Scanner read = new Scanner(System.in);
        String kal1 = read.nextLine();
        int jumlah = kal1.length();
        int hasilkali = kal1.length()*kal1.length();
        String hex = String.format("%x", hasilkali);
        kal1 = kal1.replaceAll(" ", "");
        String kal2 = kal1.substring(0, (kal1.length()/2));
        String pal = "";
        for (int i = kal2.length()-2 ; i >= 0; i--) {
            char dibalik = kal2.charAt(i);
            kal2 += dibalik;
            pal = kal2.toUpperCase();    
        }
        String octal = String.format("%o", jumlah);
        char hex1 = hex.charAt(0);
        String simbol = "?";
        if (hex1 == '0' || hex1 == '1' || hex1 == '2' || hex1 == '3' || hex1 == '4' || hex1 == '5' ||  hex1 == '6' || hex1 == '7' || hex1 == '8' || hex1 == '9') {
            simbol = "!";
        } 
        System.out.printf("#%s%s%s%s\n", hex, pal, octal, simbol);
    }
}