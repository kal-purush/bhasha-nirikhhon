package com.njust.roftwaremanage.LabManagement.controller;


import com.njust.roftwaremanage.LabManagement.service.StudentService;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.HashMap;

/**
 * 学生选课的servlet
 * 响应路径：/StudentHome
 * */
public class ViewExperiment extends HttpServlet {

    /**
     * 获取实验列表
     * 传入参数：学生id
     * 传出参数：HashMap<String,Boolean>对象
     *         String为实验的名字
     *         Boolean对该实验学生是否已经选择，true为已经选择
     * */
    public void doGet(HttpServletRequest request, HttpServletResponse response){
        //获取表单数据
        String id = request.getParameter("id");
        HashMap<String,Boolean> experiment = new HashMap<>();
        StudentService service = new StudentService();
        experiment = service.getArranges(id);
        //传递数据到前端
        request.setAttribute("experiments",experiment);
    }
}