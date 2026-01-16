import java.util.Scanner;
import java.util.Random;
public class Tp5No2 {
    public static void main(String[] args) {
        Scanner scan = new Scanner(System.in);
        int n = scan.nextInt();
        int m = scan.nextInt();
        String serialNumber = generateSerial(n,m);
        System.out.println(serialNumber);
    }
    public static String generateSerial(int n,int m){
        Random angka = new Random();
       int kali = n*m + n-1;
       int hasil;
       char hasil1[] = new char[kali];
       int k = m+1;
       for(int i = 0;i<kali;i++){
       hasil = angka.nextInt(10);
       hasil1[i] = (char) (hasil+'0') ;
    } for(int i = 1;i<n;i++){
        hasil1[m] ='-';
        m = m+k;
    }
     String str = new String(hasil1);
    return str ;
    }
}