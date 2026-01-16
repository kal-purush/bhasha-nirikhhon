package ru.otus.algo;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;

import static java.lang.Math.pow;
import static java.lang.Math.sqrt;

public class Fibo {

    private int n;

    public Fibo(int n) {
        this.n = n;
    }

    public BigInteger iter() {
        if (n == 0) {
            return BigInteger.ZERO;
        }
        BigInteger prev = BigInteger.ZERO;
        BigInteger lastCurr;
        BigInteger curr = BigInteger.ONE;
        for (int i = 1; i < n; i++) {
            lastCurr = curr;
            curr = prev.add(curr);
            prev = lastCurr;
        }
        return curr;
    }

    public BigInteger recursive() {
        return recursive(n);
    }

    private BigInteger recursive(int depth) {
        if (depth == 0) {
            return BigInteger.ZERO;
        }
        if (depth == 1) {
            return BigInteger.ONE;
        }
        return recursive(depth - 1).add(recursive(depth - 2));
    }

    public BigInteger golden() {
        if (n == 0) {
            return BigInteger.ZERO;
        }
        if (n == 1) {
            return BigInteger.ONE;
        }
        if (n == 2) {
            return BigInteger.ONE;
        }
        if (n == 3) {
            return BigInteger.TWO;
        }
        BigDecimal phi = BigDecimal.ONE.add(BigDecimal.valueOf(sqrt(5))).divide(BigDecimal.valueOf(2), RoundingMode.CEILING);
        BigDecimal res = phi.pow(n).divide(BigDecimal.valueOf(sqrt(5)), RoundingMode.CEILING);
        res = res.setScale(0, RoundingMode.HALF_UP);
        return res.toBigInteger();
    }

}