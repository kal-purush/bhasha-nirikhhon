package com.mycompany.grupodehomens;
import java.awt.FlowLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import javax.swing.*;

public class GrupoDeHomens extends JFrame implements ActionListener{
    JLabel lNome, lIdade, lEndereco, lFuncao;
    JTextField tfNome, tfIdade, tfEndereco, tfFuncao;  //referenciando as instâncias para construí-las mais abaixo
    JButton bSalvar, bLimpar, bSair;
    
public GrupoDeHomens(){ // método construtor
    setLayout(new FlowLayout());
    
//instanciando as labels e adicionando-as
    lNome = new JLabel("Nome");  //instanciando lNome
    lEndereco = new JLabel("Endereço"); //instanciando lEndereco
    lIdade = new JLabel("Idade");
    lFuncao = new JLabel("Função");
    
//instanciando os campos de texto
    tfNome = new JTextField(20);
    tfIdade = new JTextField(2);
    tfEndereco = new JTextField(40);
    tfFuncao = new JTextField(15);
    

//Adicionando objetos à frame, a sequencia define como eles se apresentarão nela
    add(lNome); 
    add(tfNome);
    add(lIdade);
    add(tfIdade);
    add(lEndereco); 
    add(tfEndereco);
    add(lFuncao);
    add(tfFuncao);
    
//instanciando os botões e adicionando-os    
    bSalvar = new JButton("Salvar");
    add(bSalvar); //adicionando botão à frame
    bSalvar.addActionListener(this); //adicionando ActionListener(leitura da ação) ao objeto
    bLimpar = new JButton("Limpar");
    add(bLimpar);
    bLimpar.addActionListener(this);//this é um link ao método executado no ActionEvent;
    bSair = new JButton("Sair");
    add(bSair);
    bSair.addActionListener(this);
//Configurando tamanho e visibilidade do Frame
    setSize(800, 600);
    setVisible(true);
}

//função principal (onde o código de fato será executado)
    public static void main(String[] args) {
        GrupoDeHomens gh = new GrupoDeHomens(); //instanciando gh a partir da classe GrupoDeHomens.
        gh.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
    }
    
        
    @Override
    public void actionPerformed(ActionEvent ae){
        if(ae.getSource()==bSalvar){
            JOptionPane.showMessageDialog(this, "Salvo com sucesso!");
        }else
        if(ae.getSource()==bLimpar){
            tfNome.setText("");
            tfIdade.setText("");
            tfEndereco.setText("");
            tfFuncao.setText("");
        }else
        if(ae.getSource()==bSair){
            System.exit(0);
        }
    }
}