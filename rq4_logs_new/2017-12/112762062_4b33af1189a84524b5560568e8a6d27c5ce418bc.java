package Sweets;

import java.util.Scanner;

/**
 * Класс-компоновщик подарка
 */
public class Present {
    public void start(){
        Scanner scanner = new Scanner(System.in);

        while (true){
            System.out.println("Вы хотите добавить сладость к подарку?");
            String sWish = scanner.next();
            if (sWish.equals("да")){
                System.out.println("Какую сладость вы хотите добавить(конфета - 1, печенька - 2, кексик - 3)?");
                int iSweetType = scanner.nextInt();
                if (iSweetType == 1){
                    Candy candy = new Candy();
                    
                }
            }
        }
    }
}