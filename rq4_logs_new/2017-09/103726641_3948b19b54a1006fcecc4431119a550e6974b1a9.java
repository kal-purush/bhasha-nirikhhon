package org.apache.jsp.Admin;

import javax.servlet.*;
import javax.servlet.http.*;
import javax.servlet.jsp.*;

public final class AddCategory_jsp extends org.apache.jasper.runtime.HttpJspBase
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

      out.write("<!DOCTYPE html>\n");
      out.write("<html>\n");
      out.write("  <head>\n");
      out.write("    <meta charset=\"UTF-8\">\n");
      out.write("    <title>Admin | Dashboard</title>\n");
      out.write("    <meta content='width=device-width, initial-scale=1, maximum-scale=1, user-scalable=no' name='viewport'>\n");
      out.write("    <!-- Bootstrap 3.3.2 -->\n");
      out.write("    \n");
      org.apache.jasper.runtime.JspRuntimeLibrary.include(request, response, "bootstrap.jsp", out, false);
      out.write("\n");
      out.write("    \n");
      out.write("  </head>\n");
      out.write("  <body class=\"skin-blue\">\n");
      out.write("    <div class=\"wrapper\">\n");
      out.write("      \n");
      out.write("          ");
      org.apache.jasper.runtime.JspRuntimeLibrary.include(request, response, "Index/header.jsp", out, false);
      out.write("\n");
      out.write("      <!-- Left side column. contains the logo and sidebar -->\n");
      out.write("      ");
      org.apache.jasper.runtime.JspRuntimeLibrary.include(request, response, "Index/Sidebar.jsp", out, false);
      out.write("\n");
      out.write("      <!-- Content Wrapper. Contains page content -->\n");
      out.write("      <div class=\"content-wrapper\">\n");
      out.write("        <!-- Content Header (Page header) -->\n");
      out.write("        \n");
      out.write("        \n");
      out.write("        \n");
      out.write("        <!-- Main content -->\n");
      out.write("        <section class=\"content\">\n");
      out.write("          <div class=\"row\">\n");
      out.write("            <!-- left column -->\n");
      out.write("            <div class=\"col-md-6\">\n");
      out.write("              <!-- general form elements -->\n");
      out.write("              <div class=\"box box-primary\">\n");
      out.write("                <div class=\"box-header\">\n");
      out.write("                  <h3 class=\"box-title\">ADD CATEGORY</h3>\n");
      out.write("                </div><!-- /.box-header -->\n");
      out.write("                <!-- form start -->\n");
      out.write("                <form role=\"form\" method=\"post\" action=\"../Categorydetail\">\n");
      out.write("                  <div class=\"box-body\">\n");
      out.write("                    <div class=\"form-group\">\n");
      out.write("                      <label>Add Category</label>\n");
      out.write("                      <input type=\"text\" class=\"form-control\" name=\"category\" placeholder=\"Enter category\"/>\n");
      out.write("                    </div>\n");
      out.write("                  </div><!-- /.box-body -->\n");
      out.write("\n");
      out.write("                  <div class=\"box-footer\">\n");
      out.write("                    <button type=\"submit\" class=\"btn btn-primary\">Submit</button>\n");
      out.write("                  </div>\n");
      out.write("                </form>\n");
      out.write("              </div><!-- /.box -->\n");
      out.write("\n");
      out.write("              <!-- Form Element sizes -->\n");
      out.write("             \n");
      out.write("              </div><!-- /.box -->\n");
      out.write("            </div><!--/.col (right) -->\n");
      out.write("   <!-- /.row -->\n");
      out.write("        </section>\n");
      out.write("              </div>  \n");
      out.write("<!--\n");
      out.write("                  </div><!-- /.content-wrapper -->\n");
      out.write("     ");
      org.apache.jasper.runtime.JspRuntimeLibrary.include(request, response, "footer.jsp", out, false);
      out.write("\n");
      out.write("    </div><!-- ./wrapper -->\n");
      out.write(" ");
      org.apache.jasper.runtime.JspRuntimeLibrary.include(request, response, "footerscript.jsp", out, false);
      out.write("\n");
      out.write(" </body>\n");
      out.write("</html>");
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