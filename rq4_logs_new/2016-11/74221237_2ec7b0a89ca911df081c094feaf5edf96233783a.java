package controle;

import java.io.IOException;
import java.sql.SQLException;
import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import modelo.Cliente;
import modelo.Endereco;
import modelo.dao.ClienteDAO;
import modelo.dao.DAOFactory;
import modelo.dao.EnderecoDAO;


public class ServletControle extends HttpServlet {

    /**
     * Processes requests for both HTTP <code>GET</code> and <code>POST</code>
     * methods.
     *
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    protected void processRequest(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        String caminho = request.getServletPath();
        if (caminho.equals("/cliente/adicionar")) {

            DAOFactory factory = new DAOFactory();
            try {
                factory.abrirConexao();
                
                EnderecoDAO dao_endereco = factory.criarEnderecoDAO();
                Endereco endereco = new Endereco();
                endereco.setEndereco(request.getParameter("endereco"));
                endereco.setNumero(request.getParameter("numero"));
                endereco.setBairro(request.getParameter("bairro"));
                endereco.setCep(request.getParameter("cep"));
                endereco.setEstado(request.getParameter("estado"));
                endereco.setCidade(request.getParameter("cidade"));
                endereco.setComplemento(request.getParameter("complemento"));
                dao_endereco.gravar(endereco);
                
                ClienteDAO dao_cliente = factory.criarClienteDAO();
                Cliente cliente = new Cliente();
                cliente.setCpf(request.getParameter("cpf"));
                cliente.setNome(request.getParameter("nome"));
                cliente.setData(request.getParameter("data"));
                cliente.setEmail(request.getParameter("email"));
                cliente.setSenha(request.getParameter("senha"));
                cliente.setIdEndereco(endereco.getIdEndereco());
                dao_cliente.gravar(cliente);
                
                RequestDispatcher rd = null;
                rd = request.getRequestDispatcher("/index.html");
                rd.forward(request, response);
            } catch (SQLException ex) {
                System.out.println("Erro no acesso ao banco de dados.");
                DAOFactory.mostrarSQLException(ex);
            } finally {
                try {
                    factory.fecharConexao();
                } catch (SQLException ex) {
                    System.out.println("Erro ao fechar a conexão com o BD.");
                    DAOFactory.mostrarSQLException(ex);
                }
            }
        }
    }

    // <editor-fold defaultstate="collapsed" desc="HttpServlet methods. Click on the + sign on the left to edit the code.">
    /**
     * Handles the HTTP <code>GET</code> method.
     *
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        processRequest(request, response);
    }

    /**
     * Handles the HTTP <code>POST</code> method.
     *
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        processRequest(request, response);
    }

    /**
     * Returns a short description of the servlet.
     *
     * @return a String containing servlet description
     */
    @Override
    public String getServletInfo() {
        return "Short description";
    }// </editor-fold>

}