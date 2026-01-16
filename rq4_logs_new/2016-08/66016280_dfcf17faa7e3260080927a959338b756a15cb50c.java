//Database Final Project balances servlet - Maj Paul Haagenson
//This code is modified from that provided by Prof. Das
/*After modifying local addresses, code should be compiled
	with the resulting .class file placed in the apache servlets
	folder. */

import java.io.*;
import java.util.*;
import javax.servlet.*;
import javax.servlet.http.*;

import java.sql.*;

public class transact extends HttpServlet {

    public void doGet(HttpServletRequest request, HttpServletResponse response)
    throws IOException, ServletException
    {
        PrintWriter out = response.getWriter();

	//This portion grabs the variables from the HTTP GET request
	String account= request.getParameter("account");
	String amount= request.getParameter("amount");
	String desc= request.getParameter("desc");
	String type= request.getParameter("type");
	
	//Differentiates between deposits and expenses
	float newNumber;
	if (type=="Expense") {
			newNumber = -(Float.parseFloat(amount));
		} else {
			newNumber = (Float.parseFloat(amount));
		}
	
	try {
		
		Class.forName("oracle.jdbc.OracleDriver");
		
		//MODIFY THIS PORTION FOR YOUR CONFIGURATION
		String url="jdbc:oracle:thin:@localhost:1521:xe";
		String user = "paul";
		String pwd = "enterin";

		Connection DB_mobile_conn = DriverManager.getConnection(url,user,pwd);

		Statement query_stmt=DB_mobile_conn.createStatement();
				
		//Send the transaction to the database
		String query="insert into expenses values("+
			"expense_seq.nextVal,CURRENT_TIMESTAMP," + 
			newNumber + "," + desc + "," + account + ")";
		
		query_stmt.executeUpdate(query);
		
		//Return success message to the sending .html page
		out.println("Transaction sent.");

		query_stmt.close();
		DB_mobile_conn.close();

    } catch (Exception exp) {
	out.println("Exception = " +exp);
	System.out.println("Exception = " +exp);

    }
	}
    public void doPost(HttpServletRequest request, HttpServletResponse response)
    throws IOException, ServletException
    {
        doGet(request, response);
    }
}
/*-- Maj Haagenson - SDG--*/