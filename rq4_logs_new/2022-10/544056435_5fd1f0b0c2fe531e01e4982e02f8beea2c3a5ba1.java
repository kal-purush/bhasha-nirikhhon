import java.util.Scanner;

public class App {
    public static void main(String args []) {
        // Requisito 1 Peça uma medida em metros
        Scanner sc = new Scanner(System.in);
        // pedir em metros
        System.out.print("Digite uma médida em metros: ");
        double  metros = sc.nextDouble();
        sc.close();
        //Converter centimetros e imprimir
        double centimetros = metros * 100;
        System.out.println(metros + "metros corresponte á " + centimetros + " centimetros.");
    
    }

}
