package com.plataformadeportiva.servlets;

import com.plataformadeportiva.utils.DatabaseConnection;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

@WebServlet("/login")
public class LoginServlet extends HttpServlet {
    private static final long serialVersionUID = 1L;

    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String username = request.getParameter("username");
        String password = request.getParameter("password");

        // Validar la autenticación
        boolean isAuthenticated = authenticateUser(username, password);
        
        if (isAuthenticated) {
        	
            // Almacenar el nombre de usuario en la sesión
            HttpSession session = request.getSession();
            session.setAttribute("username", username);
            
        	System.out.println("Redirigiendo a home.jsp");
        	response.sendRedirect(request.getContextPath() + "/home.jsp");
 // Redirigir a la página de inicio
        } else {
            request.setAttribute("errorMessage", "Usuario o contraseña incorrectos.");
            request.getRequestDispatcher("login.jsp").forward(request, response); // Redirigir manteniendo el request
        }
    }

    private boolean authenticateUser(String username, String password) {
        boolean isAuthenticated = false;

        // Consulta SQL para verificar usuario y contraseña
        String query = "SELECT * FROM users WHERE username = ? AND password = ?";

        // Usando try-with-resources para asegurar que todos los recursos se cierren automáticamente
        try (Connection connection = DatabaseConnection.getConnection();
             PreparedStatement stmt = connection.prepareStatement(query)) {

            stmt.setString(1, username);
            stmt.setString(2, password);

            // Ejecutar la consulta
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    isAuthenticated = true;
                    // Usuario autenticado
                    System.out.println("✅ Usuario autenticado correctamente.");
                } else {
                    // No se encontró usuario
                    System.out.println("❌ Usuario o contraseña incorrectos.");
                }
            }

        } catch (SQLException e) {
            e.printStackTrace();  // Podrías loguear el error en vez de solo imprimirlo
        }

        return isAuthenticated;
    }
}