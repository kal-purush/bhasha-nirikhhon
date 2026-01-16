package gp.lcw.sd.by.g3p.extension.aLiYunIpCheckService;


import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;

@RestController
@RequestMapping("/getIp")
public class GetUserIp {

    @RequestMapping("/IP")
    public String getIpAddr(HttpServletRequest request) {
       //String ip = request.getHeader("x-forwarded-for");
        String ip = request.getHeader("x-forwarded-for");
        if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getHeader("Proxy-Client-IP");
            System.out.println("用户真实IP地址"+ip);
        }
        if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getHeader("WL-Proxy-Client-IP");
            System.out.println("用户真实IP地址"+ip);
        }
        if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getRemoteAddr();
            System.out.println("用户真实IP地址"+ip);
        }
        System.out.println("用户真实IP地址"+ip);
        return ip;
    }
}