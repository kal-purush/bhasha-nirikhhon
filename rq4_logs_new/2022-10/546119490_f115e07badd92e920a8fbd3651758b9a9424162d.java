package fibonacci;

/**
 *  フィボナッチ数 - シンプル
 *  0, 1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233
 */
public class main01 {
        public static void main(String[] args) throws Exception {
        int number = 50;
        for(int i = 0;i <= number; i++) {
            System.out.println(i + ":" + fibonacci(i));
        }
    }

    // 指定番のフィボナッチ数を求める
    public static long fibonacci(int num) {
        if(num == 0) {
            return 0;
        }
        if(num == 1) {
            return 1;
        }

        long fibo = 0;
        long fibo_0 = 0;
        long fibo_1 = 1;

        for(int i = 1;i < num; i++) {
            fibo = fibo_0 + fibo_1;
            fibo_0 = fibo_1;
            fibo_1 = fibo;
        }
        return fibo;
    }
}