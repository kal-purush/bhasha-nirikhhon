package com.gz.design.panda.biz.utils;

import org.apache.commons.lang.StringUtils;

/**
 * @auther yandajun
 * @date 2018/9/25 15:36
 */
public class AddressUtil {

    public static final String PROVINCE = "省";
    public static final String CITY = "市";
    public static final String AREA = "(区/县)";

    /**
     * 地址拼接，返回格式 XX省XX市XX(区/县)XXXXXX
     * @param province
     * @param city
     * @param area
     * @param address
     * @return
     */
    public static String joinAddress(String province,String city,String area,String address){
        if(StringUtils.isEmpty(city) || StringUtils.isEmpty(area) || StringUtils.isEmpty(address)){
            return "";
        }
        StringBuilder strBuilder = new StringBuilder();
        if(!StringUtils.isEmpty(province) && !city.equals(province)){
            strBuilder.append(province).append(PROVINCE);
        }
        strBuilder.append(city).append(CITY);
        strBuilder.append(area).append(AREA);
        strBuilder.append(address);
        return strBuilder.toString();
    }

}