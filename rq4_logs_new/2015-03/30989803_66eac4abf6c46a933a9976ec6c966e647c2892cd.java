package servlet;

import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Array;
import java.text.ParseException;
import java.util.ArrayList;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import escenario2.insumos;
import escenario2.proAndes;

public class ServletRFC2
extends HttpServlet{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private proAndes pro = new proAndes();

	/**
	 * Inicializacin del Servlet
	 */
	public void init( ) throws ServletException
	{

	}

	/**
	 * fin de un servlet
	 */
	public void destroy( )
	{

	}



	/**
	 * Maneja un pedido GET de un cliente
	 * @param request Pedido del cliente
	 * @param response Respuesta
	 */
	protected void doGet( HttpServletRequest request, HttpServletResponse response ) throws ServletException, IOException
	{
		// Maneja el GET y el POST de la misma manera
		try {
			procesarSolicitud( request, response );
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}


	/**
	 * Maneja un pedido POST de un cliente
	 * @param request Pedido del cliente
	 * @param response Respuesta
	 */
	protected void doPost( HttpServletRequest request, HttpServletResponse response ) throws ServletException, IOException
	{
		// Maneja el GET y el POST de la misma manera
		try {
			procesarSolicitud( request, response );
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}



	@SuppressWarnings("deprecation")
	private void procesarSolicitud(HttpServletRequest request,
			HttpServletResponse response) throws IOException, ParseException {
		PrintWriter out = response.getWriter( );
		String accion = request.getParameter("accion");    	


		if (accion.equals("actualizar"))
		{
			
			imprimirOpciones(out);;
		System.out.println("Hey!");
		}  	

		if (accion.equals("registrar"))
		{
			System.out.println("AAA");
			String ida = request.getParameter("parametro");
			String fechi = request.getParameter("seleccion");
			String id = request.getParameter("mat");
			System.out.println(fechi);
			System.out.println(id);
			ArrayList<String> arr = pro.informacionMaterial(fechi, id );
			imprimirRta(out, arr);
		}
	}

	private void imprimirOpciones(PrintWriter out)
	{

		out.println("    <script type=\"text/javascript\">");
		out.println("          $(document).ready(function() {");
		out.println("               $(\"#suc\").click(function(event){");
		out.println("   var seleccion = document.getElementById(\"sela\").value;");
		out.println("   var mat = document.getElementById(\"material\").value;");
		out.println("   $(\"#tablaRFC2\").load('RFC2.htm',{accion: 'registrar',mat: material,seleccion: sela}); ");
		out.println("               });");
		out.println("          });");
		out.println("    </script>");
		out.println("<form name=\"sentMessage\" id=\"contactForm\" novalidate>");
		out.println("<label>Seleccion</label>                                        ");
		out.println("<select class=\"form-control\" id=\"sela\">");
		out.println("<option id=\"Materia Prima\">Materia Prima </option>");
		out.println("<option id=\"Componente\">Componente</option>");
		out.println("<option id=\"Etapa de produccion\">Etapa de produccion</option>");
		out.println("<option id=\"Producto\">Producto</option>");
		out.println("     </select>");
		out.println("                        <div class=\"row control-group\">");
		out.println("                            <div class=\"form-group col-xs-12 floating-label-form-group controls\">");
		out.println("                                <label>Id Material</label>");
		out.println("                 <input type=\"text\" class=\"form-control\" placeholder=\"Ingrese el id del material\" id=\"material\" required data-validation-required-message=\"Complete el campo\">");
		out.println("                                <p class=\"help-block text-danger\"></p>");
		out.println("                            </div>");
		out.println("                        </div>");
		out.println("<button type=\"button\" class=\"btn btn-success\" id=\"suc\">Dar informacion</button>");
		out.println("</form>");

		
	
	}
	
	private void imprimirRta(PrintWriter out, ArrayList<String> arr)
	{

		out.println("<h3> INFORMACION </h3>");
		for (int i=0; i<arr.size(); i++)
		{
			out.println("<h3> Material "+i +  " </h3>");
			out.println("<h5>"+arr.get(i)+"</h5>");
		}
		
		out.println("<label>Seleccion</label>                                        ");
		out.println("                                        <select class=\"form-control\" id=\"sela\">");
		out.println("<option value=\"Materia Prima\">Materia Prima </option>");
		out.println("<option value=\"Componente\">Componente</option>");
		out.println("<option value=\"Etapa de produccion\">Etapa de produccion</option>");
		out.println("<option value=\"Producto\">Producto</option>");
		out.println("     </select>");
		out.println("    <script type=\"text/javascript\">");
		out.println("   var seleccion = document.getElementById(\"sela\").value;");
		out.println("  console.log(seleccion); ");
		out.println("    </script>");
		
		
//		{accion: 'registrar', parametro: '"+cod+"',cantidad: cant,fecha: fech, cliente:idclient }
		

	}
	


}
