package org.example.petshop.controlle;

import org.example.petshop.model.User;
import org.example.petshop.service.UserService;
import org.example.petshop.service.UserServiceIpl;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import java.io.IOException;

@WebServlet("/login")
public class LoginServlet extends HttpServlet {
    private final UserService userService = new UserServiceIpl();

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        req.setCharacterEncoding("UTF-8");
        resp.setCharacterEncoding("UTF-8");
        resp.setContentType("text/html; charset=UTF-8");
        req.setCharacterEncoding("UTF-8");
        resp.setCharacterEncoding("UTF-8");
        resp.setContentType("text/html; charset=UTF-8");
        String action = req.getParameter("action");
        if (action == null) {
            switch (action) {
                case "logout":
                    HttpSession session = req.getSession();
                    session.invalidate();
                    resp.sendRedirect("HTML/login.jsp");
                    break;
            }
        }
    }
    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        req.setCharacterEncoding("UTF-8");
        resp.setCharacterEncoding("UTF-8");
        resp.setContentType("text/html; charset=UTF-8");
        req.setCharacterEncoding("UTF-8");
        resp.setCharacterEncoding("UTF-8");
        resp.setContentType("text/html; charset=UTF-8");
        String action = req.getParameter("action");
        if (action == null) action = "default";
        switch (action) {
            case "login":
                loginAction(req, resp);
                break;
            case "register":
                registerAction(req, resp);
                break;
            default:
                resp.sendRedirect("HTML/Login.jsp");
                break;
        }
    }
    public final static UserService userService1 = new UserServiceIpl();
    private void loginAction(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        String userName = req.getParameter("userName");
        String password = req.getParameter("password");
        User user = userService.login(req, userName, password);
        System.out.println(user);
        HttpSession session = req.getSession();
        if (user == null) {
            session.setAttribute("errorMessage", "Sai mật khẩu hoặc tài khoản không tồn tại.");
            resp.sendRedirect("HTML/Login.jsp");
        } else {
            System.out.println(user);
            session.setAttribute("user", user);
            System.out.println(user.getRole());
            switch (user.getRole()) {
                case "Admin":
                    req.getRequestDispatcher("HTML/HomeAdmin.jsp").forward(req, resp);
                    break;
                case "User":
                    req.getRequestDispatcher("View/user/homeUser.jsp").forward(req, resp);
                    break;
            }
        }
    }
    private void registerAction(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        String email = req.getParameter("email");
        String password = req.getParameter("password");
        String username = req.getParameter("username");
        String phoneNumber = req.getParameter("phoneNumber");
        String state = req.getParameter("state");
        String address = req.getParameter("address");
        String role = req.getParameter("role");
        User user = new User(username,password,state,email,phoneNumber,address , role);
        userService.register(user);
        resp.sendRedirect("/login.jsp");
    }


}
