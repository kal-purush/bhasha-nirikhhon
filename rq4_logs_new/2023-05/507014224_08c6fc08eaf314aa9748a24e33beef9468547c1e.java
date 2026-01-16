package com.discphy.code.programmers.woowa;

import org.junit.jupiter.api.Test;

import java.util.List;

import static java.util.Arrays.stream;
import static java.util.Comparator.reverseOrder;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

public class TechCamp2023Real_1_Test {

    @Test
    void solution1TestCase1() {
        int[] boxes = new int[]{1000, 800, 900};
        int m = 1000000;
        int k = 3;

        int result = solution(boxes, m, k);
        System.out.println("result = " + result);

        assertThat(result).isEqualTo(1294200);
    }

    @Test
    void solution1TestCase2() {
        int[] boxes = new int[]{9000, 10000, 8520, 9500};
        int m = 110000;
        int k = 4;

        int result = solution(boxes, m, k);
        System.out.println("result = " + result);

        assertThat(result).isEqualTo(209000);
    }

    private int solution(int[] boxes, int m, int k) {
        List<Integer> list = stream(boxes)
                .boxed()
                .sorted(reverseOrder())
                .collect(toList());

        for (int i = 0; i < k; i++) {
            final int temp = m;
            Integer money = list.stream().map(a -> getMoney(a, temp))
                    .filter(a -> a <= 100000)
                    .findFirst()
                    .orElse(0);

            m += money;
        }

        return m;
    }

    private int getMoney(Integer i, int m) {
        return (m / 10000) * i;
    }

}