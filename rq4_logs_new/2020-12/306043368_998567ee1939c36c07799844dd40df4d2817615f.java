package org.example.common.util;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Objects;

import org.springframework.util.StringUtils;

/**
 * @version V1.0
 * @since 2020-12-07 11:32
 */
public class DateUtil {

    /**
     * 默认格式化: yyyy-MM-dd HH:mm:ss
     */
    public static final String DEF_FORMAT = "yyyy-MM-dd HH:mm:ss";

    /**
     * 格式化(带毫秒):
     */
    public static final String DATE_FORMAT_WITH_MILL = "yyyyMMddHHmmssSSS";

    /**
     * 格式化当前日期数据
     *
     * @param format 格式化
     * @return String
     */
    public static String formatNow(String format) {
        return format(new Date(), format);
    }

    /**
     * 格式化日期数据
     *
     * @param date   日期数据
     * @param format 格式化
     * @return String
     */
    public static String format(Date date, String format) {
        if (Objects.isNull(date)) {
            return null;
        }
        if (StringUtils.isEmpty(format)) {
            format = DEF_FORMAT;
        }

        SimpleDateFormat sdf = new SimpleDateFormat(format);
        return sdf.format(date);
    }
}