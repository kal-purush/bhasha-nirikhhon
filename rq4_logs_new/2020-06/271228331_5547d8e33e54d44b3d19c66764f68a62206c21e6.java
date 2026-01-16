package LogTreatmentLog4J.cemvc.Controller;

import LogTreatmentLog4J.cemvc.Serivce.UserSerivce;
import LogTreatmentLog4J.cemvc.Serivce.UserSerivceimpl;
import LogTreatmentLog4J.utill.User;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.authc.IncorrectCredentialsException;
import org.apache.shiro.authc.UnknownAccountException;
import org.apache.shiro.authc.UsernamePasswordToken;
import org.apache.shiro.subject.Subject;
import org.hibernate.validator.constraints.EAN;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@Controller
@RequestMapping("/user")
public class UserController {
    @Autowired
    private UserSerivce userSerivce;


    @RequestMapping("user")
    @ResponseBody
    public List<User> user5(){
        return userSerivce.seleectUseridD();
    }

    @RequestMapping("/thymeleaf")
    public String thyme(){
        return "index";

    }
    @RequestMapping("/test")
    public String thyme1(){
        return "test";

    }
    @RequestMapping("/toLogin")
    public String toLogin(){
        return "user/login";

    }

    @RequestMapping("/add")
    public String add(){
        //<a href="add">用户添加</a>
        //只写路径和名字就行
        return "user/add";
    }
    @RequestMapping("/updata")
    public String updata(){
        return "user/updata";
    }
    @RequestMapping("/noAuth")
    public String noAuth(){
        return "user/noAuth";
    }



    @RequestMapping("/login")
    public String login(String name,String password,Model model){
        System.out.println(name);
        /**
         * 使用Shiro编写认证操作
         */
        //1.获取Subject
        Subject subject = SecurityUtils.getSubject();
        //2.封装用户数据
        UsernamePasswordToken token = new UsernamePasswordToken(name,password);
        //3.执行登录方法
        try {
            //token交给Realm
            subject.login(token);

            //登录成功
            //跳转到test.html
            return "redirect:/user/test";
        } catch (UnknownAccountException e) {
            //e.printStackTrace();
            //登录失败:用户名不存在
          //  model.addAttribute("msg", "用户名不存在");
            System.out.println("用户名不存在");
            return "login";
        }catch (IncorrectCredentialsException e) {
            //e.printStackTrace();
            //登录失败:密码错误
           // model.addAttribute("msg", "密码错误");
            System.out.println("密码错误");
            return "login";
        }
    }



    @RequestMapping("testThymeleaf")
    public String testThymeleaf(Model model){
        //把数据存入model
        model.addAttribute("name", "黑马程序员");
        //返回test.html
        return "test";
    }

}