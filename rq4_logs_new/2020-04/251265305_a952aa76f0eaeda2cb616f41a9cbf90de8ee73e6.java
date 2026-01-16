package class_01;

/**
 * This class describes difference between i++ and ++i operations.
 */
public class Task1_1 {
    public static void main(String[] args) {
        postIncrement (1);
        preIncrement (1);
    }
    /**
     * This method increases argument before assignment
     */
    private static void preIncrement(int number) {
        int res = ++number;
        System.out.println(res);
    }
    /**
     * This method increases argument after assignment
     */
    private static void postIncrement(int number) {
        int res = number++;
        System.out.println(res);
    }
}