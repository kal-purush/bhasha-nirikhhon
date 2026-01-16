package in.casekartin.servlet;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.gson.Gson;

import in.casekartin.model.CaseManager;
import in.casekartin.service.CaseManagerService;

/**
 * Servlet implementation class DisplayServlet
 */
@WebServlet("/ListCasesServlet")
public class ListCasesServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse
	 *      response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		Set<CaseManager> caseTypes = CaseManagerService.getActiveCaseTypes();
		Gson gson = new Gson();
		String json = gson.toJson(caseTypes);
		PrintWriter out = response.getWriter();
		out.print(json);
		out.flush();
	}
}