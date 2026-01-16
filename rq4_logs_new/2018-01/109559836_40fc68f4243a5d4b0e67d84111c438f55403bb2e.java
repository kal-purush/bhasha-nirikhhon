package com.aspirine.global.abstracts;

import com.aspirine.global.interfaces.Calculator;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.Objects;

/**
 * All math works are done here.
 *
 * @author yathish
 */
public abstract class MathWorks implements Calculator {

    private static final MathContext mathContext = new MathContext(4);

    public BigDecimal add(BigDecimal... bigDecimals) {

        Objects.requireNonNull(bigDecimals);

        BigDecimal r = BigDecimal.ZERO;
        for (BigDecimal b : bigDecimals) {
            r = b.add(b);
        }
        return r;
    }

    public BigDecimal subtract(BigDecimal... bigDecimals) {

        Objects.requireNonNull(bigDecimals);

        BigDecimal r = BigDecimal.ZERO;
        for (BigDecimal b : bigDecimals) {
            r = b.subtract(b);
        }
        return r;
    }

    public BigDecimal multiply(BigDecimal... bigDecimals) {

        Objects.requireNonNull(bigDecimals);

        BigDecimal r = BigDecimal.ZERO;
        for (BigDecimal b : bigDecimals) {
            r = b.multiply(b);
        }
        return r;
    }

    public BigDecimal divide(BigDecimal... bigDecimals) {

        Objects.requireNonNull(bigDecimals);

        BigDecimal r = BigDecimal.ZERO;
        for (BigDecimal b : bigDecimals) {
            r = b.divide(b, mathContext);
        }
        return r;
    }

}