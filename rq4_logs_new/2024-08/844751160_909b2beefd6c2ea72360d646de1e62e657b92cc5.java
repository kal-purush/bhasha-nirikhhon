import java.util.Scanner;

public class temperatura {
    public static void main(String[] args) {
        Scanner ler = new Scanner(System.in);
        int temp;

        System.out.print("Digite a temperatura (sem as casas decimais): ");
        temp = ler.nextInt();

        if (temp > 30){
            System.out.print("O tempo está quente");
        } else {
            System.out.print("O tempo está frio");
        }
    }
}