package com.lvxiao.algorithm.floattest;

import static java.lang.ThreadLocal.withInitial;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.DecimalFormat;

/**
 * @author lvxiao <lvxiao@kuaishou.com>
 * Created on 2021-06-27
 */
public class BigdecimalUtil {
    private static final ThreadLocal<DecimalFormat> FORMAT = withInitial(() -> new DecimalFormat("#.##"));

    private static final BigDecimal ONE_YUAN = new BigDecimal(10000);

    /**
     * 积分转换为元
     */
    public static String convertToYuan(long amount) {
        return FORMAT.get().format(new BigDecimal(amount)
                .divide(ONE_YUAN, 2, RoundingMode.HALF_UP)
                .stripTrailingZeros());
    }

    public static void main(String[] args) {
        System.out.println(convertToYuan(7554));
    }
}