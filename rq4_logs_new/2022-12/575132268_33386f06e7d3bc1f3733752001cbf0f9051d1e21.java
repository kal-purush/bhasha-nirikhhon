/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package Conexao;

import java.sql.DriverManager;

/**
 *
 * @author sn1079379
 */
public class Conexao {
    
    public void conexaoBanco() {
        
        Connection conexao = null;
        
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conexao = DriverManager.getConnection("jdbc:mysql://localhost/banco","root","");
            ResultSet rsCliente = conexao.createStatement().executeQuery("SELECT * FROM CLIENTE");
            while(rsCliente.next()){
                System.out.println("Nome: " + rsCliente.getString("nome"));
            }
        }
        catch (ClassNotFoundException ex){
            System.out.println("DRIVER DO BANCO DE DADOS NÃO LOCALIZADO");
        }   
        catch (SQLException ex){
            System.out.println("OCORREU UM ERRO AO ACESSAR O BANDO DE DADOS");
        }  finally{      
            if (conexao != null){
                conexao.close();
            }
        }
        
    } 
    
}