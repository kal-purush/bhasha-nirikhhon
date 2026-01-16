import java.io.FileNotFoundException;
import java.util.Scanner;

public class Task3 {

    public static void main(String[] args) throws Exception {
        try {
            int a = 90;
            int b = 3;
            System.out.println(a / b);
            printSum(23, 234);
            int[] abc = { 1, 2 };
            abc[3] = 9;
        } catch (NullPointerException ex) {
            System.out.println("Указатель не может указывать на null!");
        } catch (IndexOutOfBoundsException ex) {
            System.out.println("Массив выходит за пределы своего размера!");
        } catch (Throwable ex) {
            System.out.println("Что-то пошло не так...");
        }
    }
    public static void printSum(Integer a, Integer b) throws FileNotFoundException {
        System.out.println(a + b);
    }

    public void inp(String str){
        Scanner scanner = new Scanner(System.in);
        String numAsSyr = scanner.nextLine();
        try {
            double d = Double.parseDouble(str);
        }catch (IllegalArgumentException e) {
            System.out.println("empty string");
        }
    }
}