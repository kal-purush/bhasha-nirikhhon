package com.luke.es.global;

import com.luke.es.login.service.ILoginService;
import com.luke.es.tool.vo.VOutUser;
import org.springframework.stereotype.Component;
import org.springframework.ui.Model;

import javax.annotation.Resource;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import java.util.Date;

@Component
public class BController {

    @Resource
    ILoginService loginService ;

    protected VOutUser getCurrentUser(HttpServletRequest request) throws Exception{
        String token = this.getPageToken(request) ;
        return this.loginService.getCurrentUserByToken(token) ;
    }

    protected String getLocalhostUrl(HttpServletRequest request){
        return request.getScheme()+"://"+request.getServerName()+":"+request.getServerPort()+request.getContextPath()+"/";
    }

    protected Model cnfModel(HttpServletRequest request, Model model){
        model.addAttribute("systime",new Date()) ;
        String localhost = getLocalhostUrl(request) ;
        model.addAttribute("localhost",localhost) ;
        return model ;
    }
    protected String getPageToken(HttpServletRequest request){
        Cookie[] cokies = request.getCookies() ;
        if(cokies!=null){
            for(Cookie c :cokies){
                if(c.getName().equals("luke-shop-token")){
                    return c.getValue() ;
                }
            }
            return "" ;
        }else{
            return "" ;
        }
    }
}