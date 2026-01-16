package org.apache.jsp;

import javax.servlet.*;
import javax.servlet.http.*;
import javax.servlet.jsp.*;
import org.json.JSONArray;
import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.sql.DriverManager;
import org.json.JSONObject;
import commons.Database;
import java.util.ArrayList;
import java.sql.Connection;
import commons.Initializer;
import constants.Constants;

public final class getAllHelpers_jsp extends org.apache.jasper.runtime.HttpJspBase
    implements org.apache.jasper.runtime.JspSourceDependent {

  private static final JspFactory _jspxFactory = JspFactory.getDefaultFactory();

  private static java.util.List<String> _jspx_dependants;

  private org.glassfish.jsp.api.ResourceInjector _jspx_resourceInjector;

  public java.util.List<String> getDependants() {
    return _jspx_dependants;
  }

  public void _jspService(HttpServletRequest request, HttpServletResponse response)
        throws java.io.IOException, ServletException {

    PageContext pageContext = null;
    HttpSession session = null;
    ServletContext application = null;
    ServletConfig config = null;
    JspWriter out = null;
    Object page = this;
    JspWriter _jspx_out = null;
    PageContext _jspx_page_context = null;

    try {
      response.setContentType("text/html");
      pageContext = _jspxFactory.getPageContext(this, request, response,
      			null, true, 8192, true);
      _jspx_page_context = pageContext;
      application = pageContext.getServletContext();
      config = pageContext.getServletConfig();
      session = pageContext.getSession();
      out = pageContext.getOut();
      _jspx_out = out;
      _jspx_resourceInjector = (org.glassfish.jsp.api.ResourceInjector) application.getAttribute("com.sun.appserv.jsp.resource.injector");

      out.write("\n");
      out.write("\n");
      out.write("\n");
      out.write("\n");
      out.write("\n");
      out.write("\n");
      out.write("\n");
      out.write("\n");
      out.write("\n");
      out.write("\n");

JSONObject object = new JSONObject();
Class.forName(Constants.className);
Connection connection = DriverManager.getConnection(Constants.url, Constants.user, Constants.password);

PreparedStatement statement = connection.prepareStatement("SELECT helper_id,full_name,email,contact,latitude,longitude,service_id,rating FROM helpers");
	ResultSet result = statement.executeQuery();
	
	JSONArray array=new JSONArray();
	JSONObject main=null;
	 while(result.next()) {
		 
             PreparedStatement statement1 = connection.prepareStatement("SELECT service_id,name FROM services WHERE service_id='"+result.getString("service_id")+"'");
	ResultSet result1 = statement1.executeQuery();

	 while(result1.next()) {
		 
             
				 object.put("status", "success");
				 JSONObject newobject=new JSONObject();
				newobject.put("helper_id", result.getString("helper_id"));
				newobject.put("fullname", result.getString("full_name"));
				newobject.put("email", result.getString("email"));
                                newobject.put("contact", result.getString("contact"));
                                newobject.put("latitude", result.getString("latitude"));
                                newobject.put("longitude", result.getString("longitude"));
                                newobject.put("service_id", result.getString("service_id"));
                                newobject.put("service_name", result1.getString("name"));
                                newobject.put("rating", result.getString("rating"));
                                array.put(newobject);
				 main=new JSONObject();
				 main.put("status","success");
				 main.put("helpers",array);
				 	
         }
		}
 
	  

      out.write('\n');
      out.write('\n');
      out.print(main.toString());
      out.write('\n');
    } catch (Throwable t) {
      if (!(t instanceof SkipPageException)){
        out = _jspx_out;
        if (out != null && out.getBufferSize() != 0)
          out.clearBuffer();
        if (_jspx_page_context != null) _jspx_page_context.handlePageException(t);
        else throw new ServletException(t);
      }
    } finally {
      _jspxFactory.releasePageContext(_jspx_page_context);
    }
  }
}