package etu1755.framework.servlet;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class FrontServlet extends HttpServlet{

    HashMap<String,Mapping> mappingsUrl;
	
	public HashMap<String, Mapping> getMappingsUrl() {
		return mappingsUrl;
	}

	public void setMappingsUrl(HashMap<String, Mapping> mappingsUrl) {
		this.mappingsUrl = mappingsUrl;
	}

	protected void processRequest(HttpServletRequest request, HttpServletResponse response)throws ServletException, IOException {
        response.setContentType("essaie/html;charset=UTF-8");
        try (PrintWriter out = response.getWriter()) {      
        	String url = request.getRequestURL().toString()+"?"+request.getQueryString();
        	out.println(url);
        }
    }
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)throws ServletException, IOException {
        processRequest(request, response);
    }

    
    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        processRequest(request, response);
    }

    
    @Override
    public String getServletInfo() {
        return "Short description";
    }
}