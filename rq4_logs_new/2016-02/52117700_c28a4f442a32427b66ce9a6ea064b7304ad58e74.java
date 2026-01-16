package com.dream.controller;

import com.dream.model.UserEntity;
import com.dream.repository.UserRepository;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import javax.validation.Valid;
import java.util.List;

/**
 * Created by pc on 1/19/16.
 */
@Controller
public class MainController {

    private static final Logger logger = Logger.getLogger(MainController.class);

    //自动装配
    @Autowired
    private UserRepository userRepository;

    //首页
    @RequestMapping(value = "/", method = RequestMethod.GET)
    public String index() {

        //logs debug message
        if(logger.isDebugEnabled()){
            logger.debug("getWelcome is executed!");
        }

        //logs exception
        logger.error("This is Error message", new Exception("Testing"));

        return "index";
    }

    //用户管理
    @RequestMapping(value = "/users", method = RequestMethod.GET)
    public String users(ModelMap modelMap) {

        //找到user表里的所有记录
        List<UserEntity> userList = userRepository.findAll();

        //将所有记录传递给返回的jsp页面
        modelMap.addAttribute("userList", userList);

        //返回jsp目录下的userManage.jsp页面
        return "userManage";
    }
    /*
讲解：

        自动装配：相当于数据库操作的极简化，只要定义了就可以直接进行数据库操作，不用再去管开启连接、关闭连接等问题

        找到所有记录：使用Repository的默认方法findAll()。

        modelMap：用于将controller方法里面的参数传递给所需的jsp页面，以进行相关显示。*/

    //添加用户 页面
    @RequestMapping(value = "/addUser", method = RequestMethod.GET)
    public String addUser() {
        return "addUser";
    }

    //添加用户 处理
    @RequestMapping(value = "/addUserPost", method = RequestMethod.POST)
    public String addUserPost(@Valid @ModelAttribute("user") UserEntity userEntity,
                              BindingResult result,ModelMap modelMap) {
        if(result.hasErrors()){

            modelMap.addAttribute("user",userEntity);
            return "/addUser";
        }
        // 数据库中添加一个用户
        //userRepository.save(userEntity);

        // 数据库中添加一个用户，并立即刷新
        userRepository.saveAndFlush(userEntity);

        // 返回重定向 到 /users 请求
        return "redirect:/users";
    }
    /*    讲解：

    addUser请求：get请求转到添加用户页面

    addUserPost请求：post请求收集数据并存库

    @ModelAttribute注解：收集post过来的数据（在此，相当于post过来了一整个userEntity，不用一个一个地取）

    save()和saveAndFlush()：save()方法处理完毕后，数据依然在缓冲区未写入数据库，使用saveAndFlush()可以立即刷新缓冲区，写入数据库

    redirect:/users：这里使用重定向，可以让该方法重定向访问一个请求，ruturn之后，将访问 :/users 所访问的页面。*/

    //查看用户详情
    // @PathVariable可以收集url中的变量，需匹配的变量用{}括起来
    // 例如：访问 localhost:8080/showUser/1 ，将匹配 userId = 1
    @RequestMapping(value = "/showUser/{userId}", method = RequestMethod.GET)
    public String showUser(@PathVariable("userId") Integer userId,ModelMap modelMap) {

        //找到所有user id 所表示的用户
        UserEntity userEntity = userRepository.findOne(userId);

        //传递给请求页面
        modelMap.addAttribute("user",userEntity);

        return "userDetail";
    }

    //更新用户信息 页面
    @RequestMapping(value = "/updateUser/{userId}", method = RequestMethod.GET)
    public String updateUser(@PathVariable("userId") Integer userId, ModelMap modelMap) {

        //找到user id 所表示的用户
        UserEntity userEntity = userRepository.findOne(userId);

        //传递给请求页面
        modelMap.addAttribute("user",userEntity);

        return "updateUser";
    }

    //更新用户信息 操作
    @RequestMapping(value = "/updateUserPost", method = RequestMethod.POST)
    public String updateUserPost(@Valid @ModelAttribute("user") UserEntity userEntity,
                                 BindingResult result,ModelMap modelMap) {

        if (result.hasErrors()) {

            modelMap.addAttribute("user", userEntity);
            return "updateUser";
        }
        //跟新用户信息
        userRepository.updateUser(userEntity.getFirstName(), userEntity.getLastName(),
                userEntity.getPassword(), userEntity.getId());

        return "redirect:/users";
    }

    //删除用户
    @RequestMapping(value = "/deleteUser/{userId}", method = RequestMethod.GET)
    public String deleteUser(@PathVariable("userId") Integer userId) {

        //删除id为user id 的用户
        userRepository.delete(userId);
        //立即刷新
        userRepository.flush();

        return "redirect:/users";
    }
}
