package com.fisco.app.utils;

import com.fisco.app.enums.CookieName;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;

public class CookieUtil {
    private CookieUtil() {}

    /**
     * 从cookies中提取Token
     * @param cookies 客户端所有的cookie
     * @return token字符串
     */
    public static String getToken(Cookie[] cookies) {
        for (Cookie cookie : cookies) {
            if (CookieName.ACCESS_TOKEN.equals(cookie.getName()))
                return cookie.getValue();
        }
        return null;
    }

    /**
     * 从cookies中提取Token
     * @param cookies 客户端所有的cookie
     * @return 用户名
     */
    public static String getUserName(Cookie[] cookies) {
        for (Cookie cookie : cookies) {
            if (CookieName.USERNAME.equals(cookie.getName()))
                return cookie.getValue();
        }
        return null;
    }

    /**
     * 从请求中提取cookies内的token
     * @param request
     * @return token字符串
     */
    public static String getToken(HttpServletRequest request) {
        return getToken(request.getCookies());
    }
}