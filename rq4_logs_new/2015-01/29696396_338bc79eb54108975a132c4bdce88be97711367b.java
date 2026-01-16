package org.apache.jsp;

import javax.servlet.*;
import javax.servlet.http.*;
import javax.servlet.jsp.*;

public final class CadastrA_jsp extends org.apache.jasper.runtime.HttpJspBase
    implements org.apache.jasper.runtime.JspSourceDependent {

  private static final JspFactory _jspxFactory = JspFactory.getDefaultFactory();

  private static java.util.Vector _jspx_dependants;

  private org.glassfish.jsp.api.ResourceInjector _jspx_resourceInjector;

  public Object getDependants() {
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
      response.setContentType("text/html;charset=UTF-8");
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
      out.write("<!DOCTYPE html>\n");
      out.write("<html>\n");
      out.write("    <head>\n");
      out.write("        <meta http-equiv=\"Content-Type\" content=\"text/html; charset=UTF-8\">\n");
      out.write("        <title>JSP Page</title>\n");
      out.write("    </head>\n");
      out.write("   <body bgcolor=\"#C0C0C0\">\n");
      out.write("       <a href=\"PrincipalTela.jsp\">Voltar a pagina principal</a>\n");
      out.write("<h2><o>Os campos abaixo devem ser preenchidos:</o></h2><br />\n");
      out.write("<img src =\"aluno.png\" align=\"leaft\" width=\"60\" height=\"80\">\n");
      out.write("<br>\n");
      out.write("<br>\n");
      out.write("<form action=\"GerenciarUsuarioA\"  method=\"post\">\n");
      out.write("<b>Nome:</b><input type=\"text\" name=\"txtNome\" size=\"40\" ><br />\n");
      out.write("<br>\n");
      out.write("<b>Matricula:</b><input type=\"password\"size=\"20\" name=\"txtMatricula\" ><br />\n");
      out.write("<br>\n");
      out.write("Comfirmar Matricula:<br />\n");
      out.write("<br>\n");
      out.write("<b>ComfirmarMatricula:</b><input type=\"password\"size=\"20\" name=\"txtComfirmarMatricula\" ><br />\n");
      out.write("<br>\n");
      out.write("<b>Telefone:</b><input type=\"text\" name=\"txtTelefone\" size=\"20\" ><br />\n");
      out.write("<br>\n");
      out.write("<b>Email:</b><input type=\"text\" name=\"txtEmail\" size=\"40\" ><br />\n");
      out.write("<br>\n");
      out.write("<input type=\"submit\" value=\"Cadastrar\">\n");
      out.write("\n");
      out.write("\n");
      out.write("\n");
      out.write("\n");
      out.write("</form>\n");
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
      out.write("\n");
      out.write("\n");
      out.write("\n");
      out.write("    </body>\n");
      out.write("</html>\n");
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