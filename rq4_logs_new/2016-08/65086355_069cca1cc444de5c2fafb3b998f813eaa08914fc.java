package br.ufal.controladores;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import br.ufal.fachada.Fachada;
import br.ufal.modelo.Usuario;

/**
 * Servlet implementation class DeletarConta
 */
@WebServlet("/autenticado/deletarConta.jsp")
public class DeletarConta extends HttpServlet {
	private static final long serialVersionUID = 1L;

	/**
	 * @see HttpServlet#HttpServlet()
	 */
	public DeletarConta() {
		super();
		// TODO Auto-generated constructor stub
	}

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse
	 *      response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		Usuario user = (Usuario) request.getSession(true).getAttribute("usuarioLogado");
		Usuario userNoBanco = Fachada.getInstance().getUsuarioById(user.getUsername());
		
		Fachada.getInstance().deletarUsuario(userNoBanco);
		Fachada.getInstance().getAllComunidades();
		response.sendRedirect("deslogar.jsp");
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse
	 *      response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		// TODO Auto-generated method stub
		doGet(request, response);
	}

}