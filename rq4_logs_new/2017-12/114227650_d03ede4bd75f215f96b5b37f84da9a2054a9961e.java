package com.springboot;

import com.springboot.shiro.MyUserNamePasswordToken;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authc.credential.SimpleCredentialsMatcher;
import org.apache.shiro.subject.Subject;
import org.mybatis.spring.annotation.MapperScan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;


@SpringBootApplication
@Controller
@MapperScan(basePackages = "com.springboot.mapper")
public class ShiroApplication {

    private static final Logger log = LoggerFactory.getLogger(SimpleCredentialsMatcher.class);

    public static void main(String[] args){
        SpringApplication.run(ShiroApplication.class, args);
    }

    @RequestMapping("/hi")
    public String hi(){
        return "main";
    }

    @Value("${name}")
    private String username;

    @Value("${password}")
    private String password;

    @RequestMapping("/login")
    public String login(){
        //创建Token
        MyUserNamePasswordToken token = new MyUserNamePasswordToken(username, password, "My");

        // 获取用户subject
        Subject subject = SecurityUtils.getSubject();

      try{
          //身份认证
          subject.login(token);

          //需要将用户信息存入到session中
          log.info("用户身份认证通过" + subject.getPrincipals().asList().size());

      } catch (AuthenticationException e) {
          //身份认证失败
          log.error("用户身份认证失败");
      }
        return  "forward:/main";
    }

    @RequestMapping("/admLogin")
    public String admLogin(){
        //创建Token
        MyUserNamePasswordToken token = new MyUserNamePasswordToken(username, password, "Admin");

        // 获取用户subject
        Subject subject = SecurityUtils.getSubject();

        try{
            //身份认证
            subject.login(token);

            //需要将用户信息存入到session中
            log.info("用户身份认证通过" + subject.getPrincipals().asList().size());

        } catch (AuthenticationException e) {
            //身份认证失败
            log.error("用户身份认证失败");
        }
        return  "forward:/main";
    }




    @RequestMapping("/loginout")
    public String loginout(){
        Subject subject = SecurityUtils.getSubject();
        subject.logout();
        return "true";
    }

    @RequestMapping("/main")
    public String main(){
        log.info("进行处理，调转到主页面");
        return "main";
    }
}