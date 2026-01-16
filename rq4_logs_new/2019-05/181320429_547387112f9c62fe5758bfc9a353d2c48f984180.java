package org.plan.managementservice.general;

import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

public class GatewayInfo {

    public static int getUserId () {
        int userId = 0;
        try {
            String user_id = URLDecoder.decode(((ServletRequestAttributes) RequestContextHolder.getRequestAttributes()).getRequest().getHeader("user-id"), "UTF-8");
            userId = Integer.parseInt(user_id);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return userId;
    }

    public static String getUserName () {
        String userName = "";
        try {
            userName = URLDecoder.decode(((ServletRequestAttributes) RequestContextHolder.getRequestAttributes()).getRequest().getHeader("user-name"), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return userName;
    }

    public static String getDeptName () {
        String deptName = "";
        try {
            deptName = URLDecoder.decode(((ServletRequestAttributes) RequestContextHolder.getRequestAttributes()).getRequest().getHeader("user-dept"), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return deptName;
    }
}