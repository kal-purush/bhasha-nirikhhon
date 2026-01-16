package org.example.controller;

import org.example.model.User;
import org.example.service.userService.IUserService;
import org.example.service.userService.UserService;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;

@WebServlet(name="ProfileServlet", urlPatterns = "/profile")
public class ProfileServlet extends HttpServlet {
    private static final long serialVersionUID = 1L;
    private IUserService iUserService;

    public void init() {
        iUserService = new UserService();
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        String action = req.getParameter("action");
        if (action == null) {
            action = "";
        }

        try {
            switch (action) {
                case "view":
                    viewProfile(req, resp);
                    break;
                case "viewChangePassword":
                    viewChangePassword(req, resp);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        String action = request.getParameter("action");
        if (action == null) {
            action = "";
        }
        try {
            switch (action) {
                case "edit":
                    updateProfile(request, response);
                    break;
                case "changePassword":
                    changePassword(request, response);
                    break;
            }
        } catch (SQLException ex) {
            throw new ServletException(ex);
        }
    }

    private void updateProfile(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException, SQLException{
        HttpSession session = request.getSession();
        User user = (User) session.getAttribute("user");

        String name = request.getParameter("name");
        String email = request.getParameter("email");
        String birthDayStr = request.getParameter("birthDay");

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        java.util.Date birthday = null;
        try {
            birthday = dateFormat.parse(birthDayStr);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        user.setName(name);
        user.setEmail(email);
        if (birthday != null) {
            user.setBirthday(new java.sql.Date(birthday.getTime()));
        }
        iUserService.updateUser(user);
        request.getSession().setAttribute("message", "Cập nhật thông tin người dùng thành công!");
        request.getSession().setAttribute("status", "success");
        response.sendRedirect("/profile?action=view");
    }

    private void viewProfile(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException, SQLException {
        HttpSession session = request.getSession();
        User user = (User) session.getAttribute("user");
        if (user == null) {
            response.sendRedirect("login.jsp");
            return;
        }
        request.setAttribute("userBirthDay", user.getBirthday().toLocalDate().format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
        request.setAttribute("user", user);
        request.getRequestDispatcher("profile.jsp").forward(request,response);
    }

    private void viewChangePassword(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException, SQLException {
        request.getRequestDispatcher("changePassword.jsp").forward(request,response);
    }

    private void changePassword(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException, SQLException {
        HttpSession session = request.getSession();
        User user = (User) session.getAttribute("user");

        String oldPassword = request.getParameter("oldPassword");
        String newPassword = request.getParameter("newPassword");
        String confirmPassword = request.getParameter("confirmPassword");
        if (!newPassword.equals(confirmPassword)) {
            request.getSession().setAttribute("message", "Mật khẩu mới không trùng khớp!");
            request.getSession().setAttribute("status", "error");
            response.sendRedirect("/profile?action=viewChangePassword");
        } else if (!oldPassword.equals(user.getPassword())) {
            request.getSession().setAttribute("message", "Mật khẩu cũ không đúng!");
            request.getSession().setAttribute("status", "error");
            response.sendRedirect("/profile?action=viewChangePassword");
        } else {
            user.setPassword(newPassword);
            iUserService.updateUser(user);
            request.getSession().setAttribute("message", "Thay đổi mật khẩu thành công!");
            request.getSession().setAttribute("status", "success");
            response.sendRedirect("/profile?action=view");
        }
    }
}