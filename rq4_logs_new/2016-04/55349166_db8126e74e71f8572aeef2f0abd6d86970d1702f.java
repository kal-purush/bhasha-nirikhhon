package com.focuszz.login.cookie;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;

import com.focuszz.login.codec.Base64Codec;
import com.focuszz.login.codec.Codec;

public class CookieProcessor {

    public static final String DEFAULT_COOKIE_PATH = "/";

    private String             cookieDomain;

    private String             cookieName;

    private String             cookiePath          = DEFAULT_COOKIE_PATH;

    private Integer            cookieMaxAge;

    private Codec              codec               = new Base64Codec();

    public CookieProcessor(String cookieName) {
        this(cookieName, null);
    }

    public CookieProcessor(String cookieName, String cookieDomain) {
        this.cookieName = cookieName;
        this.cookieDomain = cookieDomain;
    }

    public String getCookieValue(HttpServletRequest request) {
        Cookie[] cookies = request.getCookies();
        if (cookies == null || cookies.length == 0) {
            return null;
        }
        for (Cookie cookie : cookies) {
            if (cookie.getName().equals(cookieName)) {
                String value = cookie.getValue();
                return codec == null ? value : codec.decode(value);
            }
        }
        return null;
    }

    public void addCookie(HttpServletResponse response, String value) {
        Cookie cookie = new Cookie(cookieName, codec == null ? value : codec.encode(value));
        cookie.setPath(cookiePath);
        if (StringUtils.isNotBlank(cookieDomain)) {
            cookie.setDomain(cookieDomain);
        }
        if (null != cookieMaxAge) {
            cookie.setMaxAge(cookieMaxAge);
        }
        response.addCookie(cookie);
    }

    public void removeCookie(HttpServletResponse response) {
        Cookie cookie = new Cookie(getCookieName(), "");
        cookie.setMaxAge(0);
        response.addCookie(cookie);
    }

    public String getCookieDomain() {
        return cookieDomain;
    }

    public void setCookieDomain(String cookieDomain) {
        this.cookieDomain = cookieDomain;
    }

    public String getCookieName() {
        return cookieName;
    }

    public void setCookieName(String cookieName) {
        this.cookieName = cookieName;
    }

    public String getCookiePath() {
        return cookiePath;
    }

    public void setCookiePath(String cookiePath) {
        this.cookiePath = cookiePath;
    }

    public Integer getCookieMaxAge() {
        return cookieMaxAge;
    }

    public void setCookieMaxAge(Integer cookieMaxAge) {
        this.cookieMaxAge = cookieMaxAge;
    }

    public Codec getCodec() {
        return codec;
    }

    public void setCodec(Codec codec) {
        this.codec = codec;
    }

}