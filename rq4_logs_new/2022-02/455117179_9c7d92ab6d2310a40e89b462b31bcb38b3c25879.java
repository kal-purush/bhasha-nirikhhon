package newapi;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.fasterxml.jackson.databind.ObjectMapper;

import dao.StudentDAO;
import entity.Student;

/**
 * Servlet implementation class StudentAPI
 */
@WebServlet("/StudentAPI")
public class StudentAPI extends HttpServlet {
	private static final long serialVersionUID = 1L;
	StudentDAO studentDAO = new StudentDAO();

	/**
	 * @see HttpServlet#HttpServlet()
	 */
	public StudentAPI() {
		super();
		// TODO Auto-generated constructor stub
	}

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse
	 *      response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {

		String command = request.getParameter("command");
		String studentEmail = request.getParameter("studentEmail");

		if (command.equals("GET_ALL_STUDENTS")) {
			try {

				List<Student> students = studentDAO.getStudentList();

				PrintWriter out = response.getWriter();

				response.setContentType("application/json");

				//out.print(students);

				ObjectMapper mapper = new ObjectMapper();
				String json = mapper.writeValueAsString(students);

				out.print(json);
				out.flush();

			} catch (Exception e) {
				// TODO: handle exception
			}
		} else if (command.equals("GET_STUDENT_BY_EMAIL")) {

			try {
				Student student = studentDAO.getStudentByEmail(studentEmail);

				PrintWriter out = response.getWriter();

				response.setContentType("application/json");

				//out.print(student);

				ObjectMapper mapper = new ObjectMapper();
				String json = mapper.writeValueAsString(student);

				out.print(json);
				out.flush();
			} catch (Exception e) {
				// TODO: handle exception
			}
		}
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse
	 *      response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		// TODO Auto-generated method stub
		doGet(request, response);
	}

}