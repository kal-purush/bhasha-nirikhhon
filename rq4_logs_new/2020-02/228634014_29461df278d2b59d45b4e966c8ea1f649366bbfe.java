package net.tch.sso.controller;

import net.tch.sso.controller.support.ResponseData;
import net.tch.sso.intercept.TokenIntercepter;
import net.tch.user.dto.UserLoginRequest;
import net.tch.user.dto.UserLoginResponse;
import net.tch.user.service.IUserCoreService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * @description:用户模块控制器
 * @auth tongchenghao
 * @date 2020/2/25
 */
@RestController
public class UserController extends BaseController{


    @Autowired
    @Qualifier("userCoreService")
    IUserCoreService userCoreService;

    public IUserCoreService getUserCoreService() {
        return userCoreService;
    }

    public void setUserCoreService(IUserCoreService userCoreService) {
        this.userCoreService = userCoreService;
    }

    /**
     * 用户登录
     * @param username
     * @param password
     * @return
     */
    @Anoymous
    @PostMapping("/login")
    @ResponseBody
    public UserLoginResponse doLogin(String username, String password, HttpServletResponse httpServletResponse){
        UserLoginRequest request = new UserLoginRequest();
        request.setUsername(username);
        request.setPassword(password);
        UserLoginResponse response = userCoreService.login(request);
        // 登录后将返回的token设置到cookie中
        httpServletResponse.addHeader("Set-Cookie",
                TokenIntercepter.ACCESS_TOKEN+"="+response.getToken()+";Ptch=/;HttpOnly");
        System.out.println(request);
        return response;
    }

    @ResponseBody
    @GetMapping("/test")
    public String test(){
        return "test";
    }

/*    public static void main(String[] args) {
        ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext("classpath*:spring/applicationDubbo.xml");
        IUserCoreService userCoreService = (IUserCoreService) context.getBean("userCoreService");
        userCoreService.login(null);
        System.out.println(userCoreService);
    }*/
}