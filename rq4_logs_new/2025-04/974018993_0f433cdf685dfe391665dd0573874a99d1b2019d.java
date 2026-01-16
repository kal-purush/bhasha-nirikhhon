package com.attendance;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import jakarta.servlet.ServletException;
import jakarta.servlet.annotation.WebServlet;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

@WebServlet("/login")
public class Login extends HttpServlet {

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        request.getRequestDispatcher("/WEB-INF/view/login.jsp").forward(request, response);
    }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String username = request.getParameter("username");
        String password = request.getParameter("password");

        String dbURL = "jdbc:mysql://127.0.0.1:3306/demoDB";
        String dbUser = "root";
        String dbPassword = "root";

        try (Connection conn = DriverManager.getConnection(dbURL, dbUser, dbPassword)) {
            String sql = "SELECT * FROM employees WHERE email = ? AND password = ?";
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setString(1, username);
                stmt.setString(2, password);

                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        int roleId = rs.getInt("role_id");
                        String roleQuery = "SELECT role_key FROM roles WHERE role_id = ?";
                        try (PreparedStatement roleStmt = conn.prepareStatement(roleQuery)) {
                            roleStmt.setInt(1, roleId);
                            try (ResultSet roleRs = roleStmt.executeQuery()) {
                                if (roleRs.next()) {
                                    String roleKey = roleRs.getString("role_key");
                                    if ("admin".equals(roleKey) && "0000".equals(password)) {
                                        response.sendRedirect(request.getContextPath() + "/adminMenu");
                                    } else {
                                        response.sendRedirect(request.getContextPath() + "/clockIn");
                                    }
                                } else {
                                    request.setAttribute("errorMessage", "ユーザーの役職情報が見つかりません");
                                    request.getRequestDispatcher("/WEB-INF/view/login.jsp").forward(request, response);
                                }
                            }
                        }
                    } else {
                        request.setAttribute("errorMessage", "IDまたはパスワードが間違っています");
                        request.getRequestDispatcher("/WEB-INF/view/login.jsp").forward(request, response);
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
            request.setAttribute("errorMessage", "データベース接続に失敗しました");
            request.getRequestDispatcher("/WEB-INF/view/login.jsp").forward(request, response);
        }
    }
}