package br.atletismo.controle;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import br.atletismo.dominio.Atleta;

@WebServlet("/AtletaControler")
public class AtletaControler extends HttpServlet {
	private static final long serialVersionUID = 1L;
	
	public AtletaControler(){
		super();
	}
	
	protected void doPost (HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException{
		/*List<Atleta> atleta = (List<Atleta>)getServletContext().getAttribute("Atletas");
		if (atleta == null){
			atleta = new ArrayList<Atleta>();
			getServletContext().setAttribute("Atletas", atleta);
		}
		
		String nome = request.getParameter("nome");
		String sexo = request.getParameter("sexo");
		String cmd = request.getParameter("cmd");
		
		String msg = "oi";
		
		if ("Cadastrar".equals(cmd)){
			Atleta at = new Atleta();
			at.setNome(nome);
			at.setSexo(sexo);
			
			atleta.add(at);
			
			msg = "Seu cadastro foi realizado com sucesso!!!";
		}
		
		request.setAttribute("Mensagem", msg);
		
		RequestDispatcher rd = request.getRequestDispatcher("./AtletaCadastro.jsp");
		rd.forward(request, response);*/	
	}

}