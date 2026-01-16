import java.util.Scanner;
class No1{
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        String kalimat = sc.nextLine();

        //hexadecimal
        int hex = kalimat.length() * kalimat.length();
        String jumlahHex = String.format("%x", hex);

        // palindrom
        String pal = kalimat.replace(" ", "");
        String pal2 = pal.substring(0, (kalimat.length()-1)/2);
        String pal3 = pal2.toUpperCase();
        
        char[] manipulasi = pal3.toCharArray();
        String str = "";
        System.out.println(pal3);
        for (int i = 0; i < pal3.length()-1; i++) {
            str = manipulasi[i] + str;
        }
        String palindrom = pal3 + str;

        //Octal
        int oct = kalimat.length();
        String jumlahOct = String.format("%o", oct);

        String s = "!";
        String t = "?";
        int hasil = (int)jumlahHex.charAt(0);
        if (hasil <= 97 && hasil >= 122) {
            System.out.printf("#%s%s%s%s" , jumlahHex, palindrom, jumlahOct, t);
        } else{
            System.out.printf("#%s%s%s%s" , jumlahHex, palindrom, jumlahOct,s);
        }
    }
}