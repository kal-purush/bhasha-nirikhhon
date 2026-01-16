package model;

import java.awt.Color;
import java.awt.event.ActionEvent;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.swing.table.DefaultTableModel;
import javax.swing.table.TableModel;

import view.Conexao;





public class TemaDAO {

	private Conexao conexao;
	private PreparedStatement prepararSQL;
	private ResultSet resultado;
	
	// Inserindo
	
	public TemaDAO()throws SQLException {
		this.conexao = new Conexao();
	}
	public void inserir(Tema tema)throws SQLException{
		String sql = "insert into tema(nome,status,descricao,genero,preco,dataCompra)"
				+ "values(?,?,?,?,?,?)";
		prepararSQL = this.conexao.getConexao().prepareStatement(sql);
		
		prepararSQL.setString(1,tema.getNome());
		prepararSQL.setString(2,tema.getStatus());
		prepararSQL.setString(3,tema.getDescricao());
		prepararSQL.setString(4,tema.getGenero());
		prepararSQL.setDouble(5,tema.getPreco());
		prepararSQL.setDate(6,java.sql.Date.valueOf(tema.getdataCompra()));
		prepararSQL.execute();
		prepararSQL.close();
			
	}
	

	//Listando
	
	public DefaultTableModel listar()throws SQLException {
		DefaultTableModel tabela = new DefaultTableModel();
		
		String sql = "select id,nome,status,descricao,genero,datacompra,preco,imgtema,idcategoria from tema";
		
		tabela.addColumn("id");
		tabela.addColumn("Nome");
		tabela.addColumn("Status");
		tabela.addColumn("Descricao");
		tabela.addColumn("Genero");
		tabela.addColumn("Data de Compra");
		tabela.addColumn("Preco");
		tabela.addColumn("Imagem");
		tabela.addColumn("Categoria");
		
		prepararSQL = this.conexao.getConexao().prepareStatement(sql);
		
		String titulo[] = {"Id","Nome","Status","Descricao","Genero","Data da Compra","Preco R$","Imagem do Tema","Categoria(ID)"};
		
		tabela.addRow(titulo);
		
				
		resultado = prepararSQL.executeQuery();
		
		while(resultado.next()){
		
			String[] linha = {  resultado.getString("id"),
								resultado.getString("nome"),
								resultado.getString("status"),
								resultado.getString("descricao"),
								resultado.getString("genero"),
								resultado.getString("datacompra"),
								resultado.getString("preco"),
								resultado.getString("imgtema"),
								resultado.getString("idcategoria"),
			};
			
			tabela.addRow(linha);
			
		}
			
		prepararSQL.close();
				
		
		return tabela;	
		}
	
	
		//Excluir
	
	public void excluir(Tema tema) throws SQLException{
		
		String sql = "delete from tema where id = ?";
		
		prepararSQL = this.conexao.getConexao().prepareStatement(sql);
		prepararSQL.setInt(1,tema.getId());
		prepararSQL.execute();
		prepararSQL.close();
	}
	
	
	
	public void alterar(Tema tema)throws SQLException{
		String sql = "UPDATE tema SET " + 
				"nome = ?,status = ?, descricao = ?, genero = ?, preco = ?, dataCompra = ? "+
				"WHERE id = ?";
		prepararSQL = this.conexao.getConexao().prepareStatement(sql);
		
		prepararSQL.setString(1,tema.getNome());
		prepararSQL.setString(2,tema.getStatus());
		prepararSQL.setString(3,tema.getDescricao());
		prepararSQL.setString(4,tema.getGenero());
		prepararSQL.setDouble(5,tema.getPreco());
		prepararSQL.setDate(6,java.sql.Date.valueOf(tema.getdataCompra()));
		prepararSQL.setInt(7, tema.getId());
		
		prepararSQL.execute();
		prepararSQL.close();
			
	}
	
		
		



	
	

		
		


}//Fim da Classe
		
		
