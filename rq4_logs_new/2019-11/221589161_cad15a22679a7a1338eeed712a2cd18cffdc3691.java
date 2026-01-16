package com.example.controller;

import com.example.domain.MStudent;
import com.example.domain.Student;
import com.example.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

@Controller
@ResponseBody
@RequestMapping("/user")
public class LoginController
{
    @Autowired
    private UserService userService;

    /**
     * 处理学生登录
     */

    @CrossOrigin
    @RequestMapping(value = "/login" ,method = RequestMethod.POST,produces = "application/json;charset=UTF-8")
    public boolean login1(@RequestBody Student student){
        String sid=student.getSid();
        String pwd=student.getPwd();
        Student stu=userService.getStudent(sid,pwd);
        if(stu !=null)
            return true;
        else
            return false;
    }

    @CrossOrigin
    @RequestMapping(value = "/wlogin" ,method = RequestMethod.POST,produces = "application/json;charset=UTF-8")
    public boolean login2(@RequestBody MStudent mstudent){
        String sid=mstudent.getId();
        String pwd=mstudent.getPwd();
        MStudent stu=userService.getStudent1(sid,pwd);
        if(stu !=null)
            return true;
        else
            return false;
    }


    @RequestMapping("/list")
    public Student s() {
        System.out.println(1234455);
        return userService.getStudent("555","555");
    }
        /*
    /跳转登录界面
     */

    /**
     * 返回list的json
     */
}