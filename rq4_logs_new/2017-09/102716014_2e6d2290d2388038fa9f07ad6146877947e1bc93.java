package com.xianyo.loginmodule.controller;

import com.xianyo.loginmodule.dao.pojo.AdminData;
import com.xianyo.loginmodule.dao.pojo.LoginData;
import com.xianyo.loginmodule.dao.pojo.UserData;
import com.xianyo.loginmodule.service.LoginService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.ModelAndView;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

/**
 *  处理登陆
 */
@Controller
public class LoginController {

    @Autowired
    LoginService loginService;

    /**
     * 跳转至用户登录界面
     *
     * @param request 有客户端发来的请求
     * @return 返回跳转的目标地址
     */
    @RequestMapping("/toUser.login")
    public ModelAndView JumpToUserLogin(HttpServletRequest request) {
        Cookie[] cookies = request.getCookies();
        ModelAndView mav = new ModelAndView();
        if (cookies != null) {
            LoginData tmpLoginData = new LoginData();
            //比那里所有的cookie,查找到其中关于登录信息的两个cookie
            for (Cookie cookie : cookies) {
                if (cookie.getName().equals("username"))
                    tmpLoginData.setUsername(cookie.getValue());
                else if (cookie.getName().equals("password"))
                    tmpLoginData.setPassword(cookie.getValue());
            }
            //判断查找的信息是否完整
            // 如果完整就进行登陆操作
            if (tmpLoginData.getUsername() != null && tmpLoginData.getPassword() != null) {
                UserData tmpUser = loginService.Login(tmpLoginData);
                System.out.println(tmpUser==null);

                if (tmpUser != null) {
                    //跳转至登陆成功界面
                    ShowSuccessPage(mav, tmpUser);
                    return mav;
                }
            }
        }
        //跳转至登录界面
        mav.setViewName("loginjsp/userlogin");
        return mav;
    }

    /**
     * 处理用户登录
     *
     * @param user 传入的的用户信息
     * @return 返回模型和视图
     */
    @RequestMapping("/user.login")
    public ModelAndView UserLogin(LoginData user,
                                  HttpSession session,
                                  HttpServletResponse response) throws Exception {
        ModelAndView mav = new ModelAndView();
        user.setAdmin(false);
        UserData tmpUser = loginService.Login(user);
        if (tmpUser != null) {
            //跳转至登陆成功界面
            ShowSuccessPage(mav, tmpUser);
            //将用户名存入会话中表示已经登陆
            session.setAttribute("username", user.getUsername());
            //创建cookie并写入response
            //将用户名和密码保存在cookie中传出
            //使用cookie的便利也放置手机和邮箱的泄露
            Cookie usernameCookie = new Cookie("username", tmpUser.getUsername());
            usernameCookie.setMaxAge(7 * 24 * 3600);
            usernameCookie.setPath("/");//设置作用域
            response.addCookie(usernameCookie);
            Cookie passwordCookie = new Cookie("password", tmpUser.getPassword());
            passwordCookie.setMaxAge(7 * 24 * 3600);
            passwordCookie.setPath("/");//设置作用域
            response.addCookie(passwordCookie);
        } else {
            mav.setViewName("loginjsp/loginerror");
        }
        return mav;
    }

    /**
     * 跳转至用户登录界面
     *
     * @return 返回跳转的目标地址
     */
    @RequestMapping("/toAdmin.login")
    public ModelAndView JumpToAdminLogin() {
        ModelAndView mav = new ModelAndView("loginjsp/adminlogin");
        return mav;
    }

    /**
     * 处理管理员登陆
     *
     * @param admin 传入的管理员信息
     * @return 返回模型和视图
     */
    @RequestMapping("/admin.login")
    public ModelAndView AdminLogin(AdminData admin) {
        return null;
    }

    /**
     * 显示登陆成功界面
     *
     * @param mav     ModelAndView
     * @param tmpUser 用户信息
     */
    private void ShowSuccessPage(ModelAndView mav, UserData tmpUser) {
        mav.setViewName("loginjsp/loginsuccess");
        mav.addObject("usertype", "用户");
        mav.addObject("user", tmpUser);
    }
}