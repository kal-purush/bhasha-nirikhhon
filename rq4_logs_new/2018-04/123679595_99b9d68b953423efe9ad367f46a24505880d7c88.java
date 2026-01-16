package servlet;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.HttpUrlConnectorProvider;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import data.UserLogin;

@WebServlet("/LoginServlet")
public class LoginServlet extends HttpServlet {
	
	private static final long serialVersionUID = 1L;

	public LoginServlet() {
		super();
	}
	private static URI getBaseURI() {
		return UriBuilder.fromUri("http://localhost:1234/TuxyDriveSSW/").build();
	}


	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		response.getWriter().append("Served at: ").append(request.getContextPath());
	}

	
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

		String user = request.getParameter("username");
		String pswd = request.getParameter("pswd");
		
		if (!user.equals(null) && !user.equals("")){
			if(!pswd.equals(null) && !pswd.equals("") ) {
				System.out.println("Date ok");
				

				ClientConfig config = new ClientConfig();
				Client client = ClientBuilder.newClient(config);
				client.property(HttpUrlConnectorProvider.SET_METHOD_WORKAROUND, true);
				WebTarget service = client.target(getBaseURI());
				Response resp;
				
				resp = service.path("rest").path("login").path(user).request().accept(MediaType.APPLICATION_JSON).get(Response.class);
				String data = resp.readEntity(String.class);
				int status = resp.getStatus();
				System.out.println(data);

				if (status==200){
					// JSON Parser
					
					response.sendRedirect("home.jsp");
				}else{
					System.out.println("Incorrect password!");
					
					HttpSession session = request.getSession();
					session.setAttribute("error", "Incorrect user or password!");
					request.getRequestDispatcher("login.jsp").forward(request, response);
					
				}
			}
			else{
				request.setAttribute("error", "Fields are mandatory!");
				request.getRequestDispatcher("login.jsp").forward(request, response);
				
			}
		}
		else {
			request.setAttribute("error", "Pasword is mandatory!");
			request.getRequestDispatcher("login.jsp").forward(request, response);
			
		}

	}

}