package org.example.common.util;

import java.util.Objects;

/**
 * 数组工具类
 */
public class ArrayUtil {

    private ArrayUtil() {}

    /**
     * 判断数组是否为空
     *
     * @param array 数组
     * @return boolean
     */
    public static boolean isEmpty(Object[] array) {
        return null == array || array.length == 0;
    }

    /**
     * 判断数据是否不为空
     *
     * @param array 数组
     * @return boolean
     */
    public static boolean isNotEmpty(Object[] array) {
        return !isEmpty(array);
    }

    /**
     * 判断数组是否含有指定元素
     *
     * @param array   数组
     * @param element 元素
     * @return boolean
     */
    public static <T> boolean hasElement(T[] array, T element) {
        if (isEmpty(array)) {
            return false;
        }
        for (T t : array) {
            if (Objects.equals(t, element)) {
                return true;
            }
        }
        return false;
    }

}