package org.apache.jsp;

import javax.servlet.*;
import javax.servlet.http.*;
import javax.servlet.jsp.*;

public final class admin_jsp extends org.apache.jasper.runtime.HttpJspBase
    implements org.apache.jasper.runtime.JspSourceDependent {

  private static final JspFactory _jspxFactory = JspFactory.getDefaultFactory();

  private static java.util.List _jspx_dependants;

  private javax.el.ExpressionFactory _el_expressionfactory;
  private org.apache.AnnotationProcessor _jsp_annotationprocessor;

  public Object getDependants() {
    return _jspx_dependants;
  }

  public void _jspInit() {
    _el_expressionfactory = _jspxFactory.getJspApplicationContext(getServletConfig().getServletContext()).getExpressionFactory();
    _jsp_annotationprocessor = (org.apache.AnnotationProcessor) getServletConfig().getServletContext().getAttribute(org.apache.AnnotationProcessor.class.getName());
  }

  public void _jspDestroy() {
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

      out.write("\r\n");
      out.write("\r\n");
      out.write("<head>\r\n");
      out.write("<meta http-equiv=\"Content-Language\" content=\"en-us\">\r\n");
      out.write("<meta http-equiv=\"Content-Type\" content=\"text/html; charset=windows-1252\">\r\n");
      out.write("<title>Bookstore Administration Console</title>\r\n");
      out.write("</head>\r\n");
      out.write("\r\n");
      out.write("<body link=\"#FFFFFF\" vlink=\"#FFFFFF\" alink=\"#FFFFFF\" bgcolor=\"#990000\" text=\"#FFFFFF\">\r\n");
      out.write("\r\n");
      out.write("<center>\r\n");
      out.write("<h3>Bookstore Administration Console</h3>\r\n");
      out.write("<i>With great power comes great responsibility...</i>\r\n");
      out.write("</center>\r\n");
      out.write("<hr/>\r\n");
      out.write("\r\n");
      out.write("<h2>Add a new book</h2>\r\n");
      out.write("<form method=\"POST\" name=\"addBook\" action=\"");
      out.print(request.getContextPath());
      out.write("/services/catalog\">\r\n");
      out.write("    <table>\r\n");
      out.write("        <tr>\r\n");
      out.write("            <td>Title: </td>\r\n");
      out.write("            <td><input type=\"text\" name=\"title\"/></td>\r\n");
      out.write("        </tr>\r\n");
      out.write("        <tr>\r\n");
      out.write("            <td>Author: </td>\r\n");
      out.write("            <td><input type=\"text\" name=\"author\"/></td>\r\n");
      out.write("        </tr>\r\n");
      out.write("        <tr>\r\n");
      out.write("            <td>Price: </td>\r\n");
      out.write("            <td><input type=\"text\" name=\"price\"/></td>\r\n");
      out.write("        </tr>\r\n");
      out.write("    </table>\r\n");
      out.write("    <input type=\"submit\" name=\"submit\" value=\"Add book\" />\r\n");
      out.write("</form>\r\n");
      out.write("\r\n");
      out.write("<h2>Get latest statistics from Data Warehouse</h2>\r\n");
      out.write("<form method=\"GET\" name=\"getStats\" action=\"");
      out.print(request.getContextPath());
      out.write("/services/stats\">\r\n");
      out.write("    <input type=\"submit\" name=\"submit\" value=\"Get stats\" />\r\n");
      out.write("</form>\r\n");
      out.write("\r\n");
      out.write("<hr/>\r\n");
      out.write("<center><i>\r\n");
      out.write("For more information about the Bookstore example go <a target=\"_blank\" href=\"http://www.mulesoft.org/documentation/display/MULE3EXAMPLES/Bookstore+Example\">here</a>.\r\n");
      out.write("</i></center>\r\n");
      out.write("</body>\r\n");
      out.write("</html>\r\n");
    } catch (Throwable t) {
      if (!(t instanceof SkipPageException)){
        out = _jspx_out;
        if (out != null && out.getBufferSize() != 0)
          try { out.clearBuffer(); } catch (java.io.IOException e) {}
        if (_jspx_page_context != null) _jspx_page_context.handlePageException(t);
      }
    } finally {
      _jspxFactory.releasePageContext(_jspx_page_context);
    }
  }
}