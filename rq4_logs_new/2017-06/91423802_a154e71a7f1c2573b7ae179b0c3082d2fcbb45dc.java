package com.session;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import com.session.helper.AnyWhereInfo;
import com.session.helper.AnyWhereService;
import com.session.helper.AnyWhereUser;
import com.session.helper.GoogleInfo;
import com.session.helper.GoogleService;

public class AnyWhereResp extends HttpServlet {

	public void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException, ServletException {
		System.out.println("Anywhere Resp");

		try {
			String authcode = req.getParameter("code");
			String error = req.getParameter("error");
			AnyWhereUser post = null;
//			System.out.println(authcode);
//			System.out.println(error);
			if ("access_denied".equals(error)) {
				resp.sendRedirect("login?access_denied");

			}

			else {
				StringBuffer reqUrl = req.getRequestURL();
				String accessToken = AnyWhereService.getAccessToken(authcode, reqUrl);
				System.out.println(accessToken);
				if (accessToken == null) {
					/// login?error=token_fetch_error
					resp.sendRedirect("login?error=token_fetch_error");

				}

				else {
					post = AnyWhereService.getUserInfo(accessToken);
					//System.out.println(post);
				}
			}
		
	
			if (post != null) {
				String email = post.getData().getUser().getLogin();
				//System.out.println(email);
				if (email != null) {
				boolean isPresent = SessionHelper.isPresent(email);

					if (isPresent) {
						HttpSession s1 = req.getSession();
						s1.setAttribute("email", email);
						resp.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");
						resp.setDateHeader("Expires", 0);

						resp.sendRedirect("dashboard");

					} else {
						String name = post.getData().getUser().getFirstName();
						String password = null;
						// System.out.println(name);
						SignUp.addinfo(email, name, password);
						HttpSession s1 = req.getSession(false);
						String session = SessionHelper.currentUser(req);
						if (session != null) {
							s1.invalidate();
						}
						s1 = req.getSession();
						s1.setAttribute("email", email);
						resp.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");
						resp.setDateHeader("Expires", 0);

						resp.sendRedirect("dashboard");
					}
				}
			} else {
				resp.sendRedirect("login?information_fetch_error");
			}
		}

		catch (Exception e) {

			resp.sendRedirect("login?exception_occured");
		}
	}
}