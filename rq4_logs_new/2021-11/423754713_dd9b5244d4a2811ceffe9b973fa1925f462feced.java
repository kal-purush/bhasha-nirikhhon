package com.njust.roftwaremanage.LabManagement.controller;

import com.njust.roftwaremanage.LabManagement.entity.Manager;
import com.njust.roftwaremanage.LabManagement.entity.Student;
import com.njust.roftwaremanage.LabManagement.entity.Teacher;
import com.njust.roftwaremanage.LabManagement.service.ManagerService;
import com.njust.roftwaremanage.LabManagement.service.StudentService;
import com.njust.roftwaremanage.LabManagement.service.TeacherService;
import com.njust.roftwaremanage.LabManagement.tools.Auth;
import com.njust.roftwaremanage.LabManagement.tools.Message;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;


/**
 * 处理用户登录的servlet
 * 响应路径:/login
 * */
public class LoginController extends HttpServlet {

    public void doGet(HttpServletRequest request, HttpServletResponse response) {
    }

    public void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
        /** 处理用户登录 */
        //获取表单数据
        String id = request.getParameter("id");
        String password = request.getParameter("password");
        String role = request.getParameter("role"); //角色（学生、管理员、教师）
        Message message = new Message(1, "ok");
        //处理
        Map<String, Object> claims = new HashMap<String, Object>();
        if (role.equals("student")) {//学生账号
            Student student = StudentService.login(id, password);
            if (student == null) {
                Message error = new Message(-1, "用户名或密码错误");
                request.setAttribute("message", error);
                //TODO:前后端连接时请更改此处
                request.getRequestDispatcher("输入错误页面").forward(request, response);
                return;
            }
            claims.put("id", id);
            claims.put("name", student.getName());
            claims.put("role", "student");
            String s = Jwts.builder().setClaims(claims).signWith(SignatureAlgorithm.HS512, Auth.key).setExpiration(new Date(System.currentTimeMillis() + Auth.expire)).compact();
            claims.put("token", s);
            claims.remove("exp");
            message.setData(claims);
            //TODO:跳转到学生页面
        } else if (role.equals("teacher")) {//教师账号
            Teacher teacher = TeacherService.login(id, password);
            if (teacher == null) {
                Message error = new Message(-1, "用户名或密码错误");
                request.setAttribute("message", error);
                //TODO:前后端连接时请更改此处
                request.getRequestDispatcher("输入错误页面").forward(request, response);
            }
            claims.put("id", id);
            claims.put("name", teacher.getName());
            claims.put("role", "teacher");
            String s = Jwts.builder().setClaims(claims).signWith(SignatureAlgorithm.HS512, Auth.key).setExpiration(new Date(System.currentTimeMillis() + Auth.expire)).compact();
            claims.put("token", s);
            claims.remove("exp");
            message.setData(claims);
            //TODO:跳转到教师页面
        } else if (role.equals("manager")) {//管理员账号
            Manager manager = ManagerService.login(id, password);
            if (manager == null) {
                Message error = new Message(-1, "用户名或密码错误");
                request.setAttribute("message", error);
                //TODO:前后端连接时请更改此处
                request.getRequestDispatcher("输入错误页面").forward(request, response);
            }
            claims.put("id", id);
            claims.put("name", manager.getName());
            claims.put("role", "manager");
            String s = Jwts.builder().setClaims(claims).signWith(SignatureAlgorithm.HS512, Auth.key).setExpiration(new Date(System.currentTimeMillis() + Auth.expire)).compact();
            claims.put("token", s);
            claims.remove("exp");
            message.setData(claims);
            //TODO:跳转到管理员页面
        }
    }
}