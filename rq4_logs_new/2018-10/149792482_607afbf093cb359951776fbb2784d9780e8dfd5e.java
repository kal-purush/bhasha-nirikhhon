package ctrl;

import java.io.IOException;
import java.io.Writer;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import model.Brain;

@WebServlet("/OAuth.do")
public class OAuth extends HttpServlet {
	private static final long serialVersionUID = 1L;

	protected void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		
		if (request.getParameter("calc") != null) {
			response.sendRedirect("https://www.eecs.yorku.ca/~roumani/servers/auth/oauth.cgi?back="
					+ "http://localhost:4413/ProjB/OAuth.do");
		} else if (request.getParameter("user") != null) {
			try {
				Writer out = response.getWriter();
				String html = "<html><body>";
				html += "<p><a href='Dash.do'>Back to Dashboard</a></p>";
				 html += "<b> Authentication Result:</b><br/><code>" + request.getQueryString() + "</code>";
				html += "</body></html>";
				out.write(html);
			} catch (Exception e) {
				response.setContentType("text/html");
				Writer out = response.getWriter();
				String html = "<html><body>";
				html += "<p><a href=' Dash.do'>Back to Dashboard</a></p>";
				html += "<p>Error " + e.getMessage() + "</p>";
				out.write(html);
			}
		} else {
			this.getServletContext().getRequestDispatcher("/OAuth.html").forward(request, response);
		}
	}

	protected void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		doGet(request, response);
	}

}