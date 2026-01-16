package com.spx0315.utils;

import com.spx0315.dao.UserDaoImple;
import com.spx0315.model.User;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
public class LoginServlet extends HttpServlet{
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        this.doPost(req, resp);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

        String userName=req.getParameter("userName");
        String password=req.getParameter("password");
        JDBCUtils jdbcUtils= new JDBCUtils();
        User user=new User(userName,password);
        UserDaoImple dao=new UserDaoImple();
        try {
            if(dao.login(user)!=null) {
                resp.sendRedirect("chatroom.jsp");
            }else{
                resp.sendRedirect("register.jsp");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}