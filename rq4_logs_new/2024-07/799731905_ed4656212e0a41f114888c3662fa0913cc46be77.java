package math.p1010;

import math.MathPath;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigInteger;

public class Main {

    public static void main(String[] args) throws IOException {

        System.setIn(new FileInputStream(MathPath.PATH + "/p1010/data/1.txt"));
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

        StringBuilder answer = new StringBuilder();

        Bridge bridge = new Bridge(br.readLine());
        for (int i = 0; i < bridge.getTestCount(); ++i) {
            answer.append(bridge.test(br.readLine()));
            answer.append("\n");
        }

        System.out.print(answer);
    }
}

class Bridge {

    private final int testCount;

    public Bridge(String size) {

        this.testCount = Integer.parseInt(size);
    }

    public int getTestCount() {

        return testCount;
    }

    // 다형성 오버로딩 사용 데이터 변환
    public String test(String line) {

        String[] data = line.split(" ");
        return test(Integer.parseInt(data[0]), Integer.parseInt(data[1]));
    }

    // 본 로직
    public String test(int west, int east) {

        BigInteger n, m, k;
        m = factorial(west);
        n = factorial(east);
        k = factorial(east - west);

        // n! / m! * (n - m)!
        return String.valueOf(n.divide(m.multiply(k)));
    }

    private BigInteger factorial(int n) {

        BigInteger num = BigInteger.valueOf(1);
        for (int i = 1; i <= n; ++i) {
            num = num.multiply(BigInteger.valueOf(i));
        }
        return num;
    }
}