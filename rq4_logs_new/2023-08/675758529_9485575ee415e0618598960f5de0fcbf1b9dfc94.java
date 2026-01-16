package lesson5.hw2;

/**
 * Задача 2:
 * Создать программу для раскраски шахматной доски с помощью цикла. Создать
 * двумерный массив String 8х8. С помощью циклов задать элементам массива значения
 * B(Black) или W(White). При выводе результат работы программы должен быть
 */
public class Hw2 {
    public static void main(String[] args) {

        String[][] mass = new String[8][8];

        for (int i = 0; i < mass.length; i++) {
            for (int j = 0; j < mass[i].length; j++) {
                if ((i + j) % 2 == 0) {
                    mass[i][j] = "W";
                } else {
                    mass[i][j] = "B";
                }
                System.out.print(mass[i][j]);
            }
            System.out.println();
        }
    }
}