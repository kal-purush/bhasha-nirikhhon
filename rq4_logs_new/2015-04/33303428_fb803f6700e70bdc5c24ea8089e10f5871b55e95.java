package lai.macgyver;

import javax.servlet.http.*;
import javax.servlet.*;
import java.io.*;

public class SayHi extends HttpServlet {

	public void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		
		response.setContentType("text/html;charset=Big5");
		
		PrintWriter out =response.getWriter();
		request.setCharacterEncoding("Big5");
	}

}