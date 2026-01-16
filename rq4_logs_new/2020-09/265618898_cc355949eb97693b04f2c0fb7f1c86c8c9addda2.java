/**
 * @author 烛影鸾书
 * @date 2020/9/21 11:06
 * @copyright© 2020
 */
public class Test {

    private static long num = 0;

    public static void set1() {
        num = 0;
    }

    public static void set2() {
        num = -1;
    }

    public static void check() {
        System.out.println(num);
        if (0 != num && -1 != num) {
            System.out.println("error");
        }
    }

    public static void main(String[] args) {
        new Thread(() -> {
            while (true) {
                set1();
            }
        }).start();
        new Thread(() -> {
            while (true) {
                set2();
            }
        }).start();
        new Thread(() -> {
            while (true) {
                check();
            }
        }).start();
    }

}