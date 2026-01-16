package hal_shop.servlet.api;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import org.json.JSONObject;

import hal_shop.beans.AuthBeans;
import hal_shop.util.dao.customer.CustomerDAO;
import hal_shop.util.dao.customer.CustomerDTO;
import hal_shop.util.message.Message;

@WebServlet("/API/Auth")
public class Auth extends HttpServlet {
	private static final long serialVersionUID = 1L;

	public Auth() {
		super();
	}

	protected void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
	}

	protected void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		PrintWriter out = response.getWriter();
		String action = request.getParameter("action");

		if (action == null) {
			out.println(new Message().exception404().toJSON());
			return;
		}
		switch (action) {
		case "login":
			out.println(login(request));
			break;
		case "logout":
			out.println(logout(request));
			break;
		case "edit":
			// TODO 会員情報の編集
			break;
		}
		return;
	}

	private String logout(HttpServletRequest request) {
		HttpSession session = request.getSession(false);
		session.invalidate();
		return new Message().createMessage("auth", "ログアウトしました").toJSON();
	}

	private String login(HttpServletRequest request) {
		String id = request.getParameter("id");
		String password = request.getParameter("password");

		CustomerDTO cdo = CustomerDAO.Login(id, password);
		if (cdo == null) {
			return new Message().exception404().toJSON();
		}
		JSONObject arr = new JSONObject(cdo);
		AuthBeans ab = new AuthBeans();
		ab.setName(cdo.getName());
		ab.setNo(cdo.getNo());
		HttpSession session = request.getSession(true);
		session.setAttribute("AUTH", ab);
		return arr.toString();
	}

}