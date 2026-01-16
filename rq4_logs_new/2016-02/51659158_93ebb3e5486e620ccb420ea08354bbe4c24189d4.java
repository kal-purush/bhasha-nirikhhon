package org.apache.jsp;

import javax.servlet.*;
import javax.servlet.http.*;
import javax.servlet.jsp.*;
import data.Questao;
import java.util.Date;
import data.Usuario;
import data.Banco;

public final class CadastroQuestao_jsp extends org.apache.jasper.runtime.HttpJspBase
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

      out.write(" \n");
      out.write("\n");
      out.write("\n");
      out.write("\n");
      out.write("\n");
      out.write("<!DOCTYPE html>\n");
      out.write("<html>\n");
      out.write("    <head>\n");
      out.write("        <meta http-equiv=\"Content-Type\" content=\"text/html; charset=UTF-8\">\n");
      out.write("        <title>Banco de Questões</title>\n");
      out.write("        <link rel=\"stylesheet\" href=\"css/style.css\">\n");
      out.write("    </head>\n");
      out.write("    <body>\n");
      out.write("        ");

            try {

                String tema = request.getParameter("conteudo");
                    if (tema != null){

                    String subtema = request.getParameter("subtema");
                    String resposta = request.getParameter("resposta");
                    String questao = request.getParameter("questao");
                     
                    int tipoQuestao = Integer.parseInt(request.getParameter("tipoQuestao"));
                    int dificuldade = Integer.parseInt(request.getParameter("dificuldade"));
                    int id_usuario  = Integer.parseInt(session.getAttribute("id")+"");
                    Date data = new Date(); 
                    String hora = data.getHours() + ":" + data.getMinutes() + ":" + data.getSeconds();
                    String autoria = request.getParameter("autoria");

                    Questao q = new Questao(tema, subtema, resposta, questao, tipoQuestao, dificuldade, id_usuario, data, hora, autoria);
                    Banco b = new Banco();
                    b.cadastrarQuestao(q);

                    response.sendRedirect("?CadastroQuestao=ok");
                }
            } catch (NullPointerException e) {

            }
        
      out.write("\n");
      out.write("        <div class='container'>\n");
      out.write("            <div class='banner'>\n");
      out.write("                <img src=\"\">\n");
      out.write("                <p class='title'> Banco de Questões </p>\n");
      out.write("            </div>\n");
      out.write("            <div class='content'>\n");
      out.write("                <div>\n");
      out.write("                    <form action=\"CadastroQuestao.jsp\" method=\"post\">\n");
      out.write("                        <p class='subtitle'>Cadastro de questões</p>\n");
      out.write("                <center>\n");
      out.write("                            <div id=\"tabeladealteracao\">\n");
      out.write("                                <input type=\"text\" placeholder=\"Conteúdo\" required name=\"conteudo\"><br>  \n");
      out.write("                                <input type=\"text\" placeholder=\"Subtema\" required name=\"subtema\"><br> \n");
      out.write("                                <input type=\"text\" placeholder=\"Autoria\" required name=\"autoria\"><br> \n");
      out.write("                                <select name=\"dificuldade\">\n");
      out.write("                                    <option value=\"0\">Fácil</option>\n");
      out.write("                                    <option value=\"1\">Médio</option>\n");
      out.write("                                    <option value=\"2\">Difícil</option>\n");
      out.write("                                </select><br>\n");
      out.write("                                \n");
      out.write("                                <select name=\"tipoQuestao\">\n");
      out.write("                                    <option value=\"0\">Discursiva</option>\n");
      out.write("                                    <option value=\"1\">Objetiva</option>\n");
      out.write("                                </select><br>\n");
      out.write("                                \n");
      out.write("                                <input type=\"text\" placeholder=\"Questão\" required name=\"questao\"><br> \n");
      out.write("                                \n");
      out.write("                                <input type=\"text\" placeholder=\"Resposta esperada\" required name=\"resposta\"><br>\n");
      out.write("                                <input type='hidden' name='cadastroQuestao' value='ok'>\n");
      out.write("                                <a href=\"perfil.jsp\"> <input type=\"submit\" value=\"Cadastrar\"> </a> <br>\n");
      out.write("                                <a href=\"index.jsp\"><input type=\"button\" value=\"Cancelar\"></a> <br>\n");
      out.write("                                \n");
      out.write("                            </div><br>\n");
      out.write("                    <div><a href=\"perfil.jsp\"><input type=\"button\" value=\"voltar\"> </a></div>\n");
      out.write("                        </center>\n");
      out.write("                    </form>\n");
      out.write("                </div>\n");
      out.write("            </div>\n");
      out.write("        </div>\n");
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