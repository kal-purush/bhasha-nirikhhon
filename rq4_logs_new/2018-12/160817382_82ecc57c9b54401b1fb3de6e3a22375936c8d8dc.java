/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package shopping;

import java.io.IOException;
import java.sql.Connection;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.scene.control.Label;
import javafx.scene.layout.HBox;
import shopping.modelos.Funcionario;

public class FuncionarioItemController {

    @FXML
    private HBox root;
    @FXML
    private Label nome;
    @FXML
    private Label funcao;
    @FXML
    private Label telefone;
    @FXML
    private Label salario;

    private Connection conn;

    public FuncionarioItemController() {
        FXMLLoader fxmlLoader = new FXMLLoader(getClass().getResource("fxml/funcionario_item.fxml"));
        fxmlLoader.setController(this);
        try {
            fxmlLoader.load();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public FuncionarioItemController setInfo(Connection c, Funcionario inv) {
        this.conn = c;

        nome.setText(inv.getNome());
        funcao.setText(inv.getFuncao());
        salario.setText("R$" + inv.getSalario());
        telefone.setText(inv.getTelefone());
        return this;
    }

    public HBox getView() {
        return root;
    }

}