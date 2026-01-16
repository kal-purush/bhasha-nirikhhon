import java.util.Scanner;
public class Nomor1  {
    
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        String inputan = sc.nextLine(); // kalimat inputan
        String kataBaru1 = inputan.replaceAll(" ", ""); 
        String kataBaru2 = kataBaru1.toUpperCase(); 
        int panjang = kataBaru2.length(); 
        int akhir = panjang/2;   
        String kata3 = kataBaru2.substring(0, akhir); 
        int kata4 = inputan.length();  
     
        for(int i = kata3.length()-2; i >= 0 ; i--) { // dibuat palindrom
            kata3 += kata3.charAt(i);
        }
        String hex = String.format("%x", kata4*kata4);
        String oct = String.format("%o", kata4);
        String startCheck = hex.charAt(0) >= 'A' && hex.charAt(0) <= 'z' ? "?" : "!";
        System.out.println("#" + hex + kata3 + oct + startCheck);

        
    }

} 