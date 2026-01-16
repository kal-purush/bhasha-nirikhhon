package chapter6.exercise04;

import java.util.Random;
import java.util.concurrent.atomic.LongAccumulator;

public class Main {
    private static final int RANDOM_VALUES_LIMIT = 100_000;
    private static final int RANDOM_VALUES_NUM   = 100;

    public static void main(String[] args) {
        long[] randomValues = createLongRandomValues(RANDOM_VALUES_LIMIT, RANDOM_VALUES_NUM);

        // 乱数列の中から最大値を取得
        LongAccumulator maximumValueCalculator = new LongAccumulator(Math::max, 0);
        for (int i = 0; i < randomValues.length; i++) {
            maximumValueCalculator.accumulate( randomValues[i] );
        }
        System.out.println("Maximum value: " + maximumValueCalculator.get());

        // 乱数列の中から最大値を取得
        LongAccumulator minimumValueCalculator = new LongAccumulator(Math::min, RANDOM_VALUES_LIMIT);
        for (int i = 0; i < randomValues.length; i++) {
            minimumValueCalculator.accumulate( randomValues[i] );
        }
        System.out.println("Minimum value: " + minimumValueCalculator.get());
    }

    /*
     * [0, limit) の範囲で乱数を生成
     */
    public static long[] createLongRandomValues(int limit, int size) {
        long[] values = new long[size];
        Random random = new Random();

        for (int i = 0; i < values.length; i++) {
            values[i] = random.nextInt(limit);
        }

        return values;
    }
}