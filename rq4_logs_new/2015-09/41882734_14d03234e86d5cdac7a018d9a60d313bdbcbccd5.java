import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.TypedQuery;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import customTools.DBUtil;
import model.GoshCart;
import model.GoshOrder;
import model.GoshProduct;
import model.GoshUser;

/**
 * Servlet implementation class AddComment
 */
@WebServlet("/ReturnItem")
public class ReturnItem extends HttpServlet {
	private static final long serialVersionUID = 1L;

	/**
	 * @see HttpServlet#HttpServlet()
	 */
	public ReturnItem() {
		super();
		// TODO Auto-generated constructor stub
	}

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse
	 *      response)
	 */
	protected void doGet(HttpServletRequest request,
			HttpServletResponse response) throws ServletException, IOException {
		long productid = Long.parseLong(request.getParameter("productid"));
		long orderid = Long.parseLong(request.getParameter("orderid"));
		HttpSession session = request.getSession(true);
		GoshUser user = (GoshUser) session.getAttribute("User");
		System.out.println("Return " + productid + " " + orderid);
//update quantity in GoshProduct
		GoshProduct product = DBUtil.createQuery("Select g from GoshProduct g where g.productId = ?1", GoshProduct.class).setParameter(1,productid).getSingleResult();
		TypedQuery<GoshOrder> query1 = DBUtil.createQuery("SELECT g FROM GoshOrder g WHERE g.goshUser = ?1 and g.goshProduct = ?2 and g.orderId = ?3", GoshOrder.class);
		query1.setParameter(1, user);
		query1.setParameter(2, product);
		query1.setParameter(3, orderid);
		GoshOrder thisOrder = query1.getSingleResult();
		
		TypedQuery<GoshProduct> query2 = DBUtil.createQuery("SELECT g FROM GoshProduct g WHERE g.productId = ?1", GoshProduct.class);
		query2.setParameter(1, productid);
		GoshProduct thisProduct = query2.getSingleResult();
		thisProduct.setProductQty((int)(thisProduct.getProductQty()+thisOrder.getOrderAmt()));
		GoshProductDB.update(thisProduct);
		GoshOrderDB.delete(thisOrder);
		
		response.setContentType("text/html");


		getServletContext().getRequestDispatcher("/GetMyOrder")
				.forward(request, response);
		
		
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse
	 *      response)
	 */
	protected void doPost(HttpServletRequest request,
			HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
	}


}