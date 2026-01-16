//написать класс который выводит в консоль массив чисел увеличенных в два раза, для решения использовать библиотеку Math
public class TaskEight {
    public static void main(String[] args) {
        int mas[] = {2, 4, 6, 1, 3, 4};
        int square = 2;
        for (int i = 0; i < mas.length; ++i) {
            mas[i] = Math.multiplyExact(mas[i], square);
            System.out.println(mas[i]);
        }
    }
}