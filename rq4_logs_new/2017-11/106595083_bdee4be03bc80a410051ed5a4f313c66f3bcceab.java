package Escola.servlets;

import java.io.IOException;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletRequest;
import org.json.JSONObject;

import Escola.Cookie.CookieManager;
import Escola.dados.AlunoDAO;
import org.json.JSONException;

@WebServlet("/deslogin")
public class DesLoginUsuario extends HttpServlet {
	@Override
	public void service(ServletRequest request, ServletResponse response) throws ServletException, IOException {
		HttpServletResponse resposta = (HttpServletResponse) response;
		try {
			CookieManager cookieManager = new CookieManager();
			HttpServletRequest req = (HttpServletRequest) request;
			Cookie[] cookies = req.getCookies();
			Cookie cookie = cookieManager.getUsuarioAutenticado(cookies);
			if(cookie != null) {
				cookie.setMaxAge(0);
				resposta.addCookie(cookie);
			}
			resposta.sendRedirect("index.jsp");
		}catch(Exception ex) {
			resposta.sendRedirect("Falha.jsp?msg=" + ex.getMessage());
		}
	}

}