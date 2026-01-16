
import java.util.Scanner;

public class TP1_1_H071231085 {
    
    public static void main(String[] args) {
    
        Scanner input = new Scanner(System.in);
        int BilanganBulat = 0;
        int BilanganDecimal = 0;

        int n = Integer.parseInt(input.nextLine());
        for (int i = 0; i < n ; i++){ //ada 3 bagian, yg pertama inisialisasi, kedua kondisi, ketiga apa yg dibuat ketika selesai dalam fornya
            try { 
              input.nextInt();
              BilanganBulat ++ ;
        

            } catch(Exception e){
              input.nextDouble();
              BilanganDecimal ++ ; 
            }
        }
        System.out.println(BilanganBulat + " Bilangan Bulat" );
        System.out.println(BilanganDecimal + " Bilangan Decimal");
        input.close();
    }  
}