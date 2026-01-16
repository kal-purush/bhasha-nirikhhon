import java.util.Scanner;
public class Nomor1{
    public static void main(String[] args) {
        Scanner read = new Scanner(System.in);
       String kalimat = read.nextLine();
      String kalimat1 = kalimat.replaceAll(" ","").toUpperCase();
       int setengah = kalimat1.length()/2;
      String kalimat2 = kalimat1.substring(0,setengah);
       int panjang = kalimat2.length();
       char [] str = new char[panjang - 1];
       int i = panjang - 2;
       for(int j = 0;j<=panjang-2;j++){
           str [j] = kalimat2.charAt(i);
           i--;
       }
       String kalimat3 = new String(str);
       String kalimat4 = kalimat2 + kalimat3;
       int panjangAwal = kalimat.length()*kalimat.length();
       String hexa = Integer.toHexString(panjangAwal);
       String octa = Integer.toOctalString(kalimat.length());
    try{
       char hexa1 = hexa.charAt(0);
       String hexa2 = String.format("%c",hexa1);
        double d = Double.parseDouble(hexa2);
        String simbol = "!";
        System.out.println("#"+hexa+kalimat4+octa+simbol);
    }catch(Exception e){
        String simbol = "?";
        System.out.println("#"+hexa+kalimat4+octa+simbol);
}

       


    
    }
}