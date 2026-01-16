/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import lojinha.Carro;

/**
 *
 * @author Fatec
 */
public class CarroDAO {
    private final Connection connection;
    
    public CarroDAO (Connection connection){
        this.connection = connection;
    }
    
    public void insert (Carro usuario) throws SQLException {
        String sql = "insert into carro(placa, potencia, modelo, cor, marca, ano, preco) values ('"+usuario.getPlaca()+"', '"+usuario.getPotencia()+"'"
                + ",'"+usuario.getModelo()+"','"+usuario.getCor()+"','"+usuario.getMarca()+"','"+usuario.getAno()+"','"+usuario.getPreco()+"')";
        
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.execute();
            
        connection.close();
    }
}