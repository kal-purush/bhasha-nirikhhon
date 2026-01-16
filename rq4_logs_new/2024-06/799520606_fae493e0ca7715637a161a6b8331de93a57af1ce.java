package Module4_KorotaevaSA.Middle;

import java.util.Scanner;

public class Task4 {
    public static void main(String[] args) {
        /*
        Нужно написать программу — помощника по кухне. Помощник должен определять, какое блюдо можно приготовить
        из имеющихся продуктов. В программу сначала вводятся продукты. А далее нужно определить, возможно ли из
        имеющихся продуктов приготовить яичницу, омлет или кофе. Для омлета требуется молоко и яйца, для яичницы
        только яйца, для кофе — кофе и молоко.
         */
        final int RANGE_0 = 0;
        boolean eggs = false;
        boolean coffee = false;
        boolean milk = false;

        Scanner scan = new Scanner(System.in);
        System.out.println("Укажите число, сколько продуктов у вас имеется из списка (яйца, молоко, кофе):");
        int x = scan.nextInt();
        //заполним диапазон продуктов
        System.out.println("Укажите наименования продуктов:");
        String[] product = new String[x];
        for (int i = RANGE_0; i < x; i++) {
            product[i] = scan.next();
        }

        for (int i = RANGE_0; i < product.length; i++) {
            switch (product[i]) {
                case ("яйца"): {
                    eggs = true;
                    break;
                }
                case ("кофе"): {
                    coffee = true;
                    break;
                }
                case ("молоко"): {
                    milk = true;
                    break;
                }
                default: {
                }
            }
        }

        if (coffee & milk & eggs) {
            System.out.println("Можно приготовить омлет");
            System.out.println("Можно приготовить кофе");
            System.out.println("Можно приготовить яичницу");
        } else if (coffee & milk) {
            System.out.println("Можно приготовить кофе");
        } else if (eggs & milk) {
            System.out.println("Можно приготовить омлет или яичницу");
        } else if (eggs) {
            System.out.println("Можно приготовить яичницу");
        } else {
            System.out.println("Не достаточно продуктов");
        }
        /* результат:
        Укажите число, сколько продуктов у вас имеется из списка (яйца, молоко, кофе):
        3
        Укажите наименования продуктов:
        кофе
        молоко
        яйца
        Можно приготовить омлет
        Можно приготовить кофе
        Можно приготовить яичницу
         */
    }
}