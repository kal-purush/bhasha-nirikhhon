package org.sc.common.utils;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.sc.common.exception.ServiceRunTimeException;

import java.util.List;

/**
 * 空判断
 *
 * @author lv
 * @create 2018-10-12
 **/
public class EmptyValidateUtil {


    public static void strIsEmpty(String data, String errorMsg) {
        if (StringUtils.isEmpty(data)) {
            throw new ServiceRunTimeException(errorMsg + " 不能为空");
        }
    }

    public static void objIsNull(Object obj, String errorMsg) {
        if (obj == null) {
            throw new ServiceRunTimeException(errorMsg + " 不能为 null");
        }
    }

    public static void strIsEmpty(String data, String errorMsg, boolean flag) {
        if (StringUtils.isEmpty(data)) {
            String msg = flag ? "" : " 不能为 null";
            throw new ServiceRunTimeException(errorMsg + msg);
        }
    }

    public static void objIsNull(Object obj, String errorMsg, boolean flag) {
        if (obj == null) {
            String msg = flag ? "" : " 不能为 null";
            throw new ServiceRunTimeException(errorMsg + msg);
        }
    }

    public static void listIsEmpty(List list, String errorMsg) {
        if (CollectionUtils.isEmpty(list)) {
            throw new ServiceRunTimeException(errorMsg);
        }
    }
}