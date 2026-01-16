/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/GUIForms/JFrame.java to edit this template
 */
package ufes.view;

import java.awt.GraphicsEnvironment;
import javax.swing.JButton;

/**
 *
 * @author Gabriel
 */
public class JanelaErroView extends javax.swing.JFrame {

    public JanelaErroView() {
        initComponents();
        centralizarTela();
        setDefaultCloseOperation(DISPOSE_ON_CLOSE);
        this.setResizable(false);
    }
    
    private void centralizarTela(){
        GraphicsEnvironment ge = GraphicsEnvironment.getLocalGraphicsEnvironment();
        
        //Obtém a posição central da tela
        int screenWidth = ge.getCenterPoint().x;
        int screenHeight = ge.getCenterPoint().y;
        
        //Calcula a posição central da janela
        int janelaX = screenWidth - (this.getWidth()/2);
        int janelaY = screenHeight - (this.getHeight()/2);
        
        //Define a posição na tela
        setLocation(janelaX, janelaY);
    }
    
    public void exibirMensagemErro(String mensagem){
        txtMensagemErro.setText(mensagem);
        this.setVisible(true);
    }

    public JButton getBtnFechar() {
        return btnFechar;
    }
    
    @SuppressWarnings("unchecked")
    // <editor-fold defaultstate="collapsed" desc="Generated Code">//GEN-BEGIN:initComponents
    private void initComponents() {

        jLabelErro = new javax.swing.JLabel();
        txtMensagemErro = new javax.swing.JLabel();
        btnFechar = new javax.swing.JButton();

        setDefaultCloseOperation(javax.swing.WindowConstants.EXIT_ON_CLOSE);
        setTitle("Janela de erro");

        jLabelErro.setFont(new java.awt.Font("Segoe UI", 1, 14)); // NOI18N
        jLabelErro.setText("Erro:");

        txtMensagemErro.setFont(new java.awt.Font("Segoe UI", 0, 14)); // NOI18N

        btnFechar.setFont(new java.awt.Font("Segoe UI", 0, 14)); // NOI18N
        btnFechar.setText("Fechar");

        javax.swing.GroupLayout layout = new javax.swing.GroupLayout(getContentPane());
        getContentPane().setLayout(layout);
        layout.setHorizontalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(javax.swing.GroupLayout.Alignment.TRAILING, layout.createSequentialGroup()
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.TRAILING)
                    .addGroup(layout.createSequentialGroup()
                        .addGap(26, 26, 26)
                        .addComponent(jLabelErro, javax.swing.GroupLayout.PREFERRED_SIZE, 37, javax.swing.GroupLayout.PREFERRED_SIZE)
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                        .addComponent(txtMensagemErro, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
                    .addGroup(layout.createSequentialGroup()
                        .addContainerGap(210, Short.MAX_VALUE)
                        .addComponent(btnFechar)))
                .addGap(26, 26, 26))
        );
        layout.setVerticalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(layout.createSequentialGroup()
                .addGap(27, 27, 27)
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING, false)
                    .addComponent(jLabelErro, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                    .addComponent(txtMensagemErro, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
                .addGap(18, 18, 18)
                .addComponent(btnFechar)
                .addContainerGap(22, Short.MAX_VALUE))
        );

        pack();
    }// </editor-fold>//GEN-END:initComponents


    // Variables declaration - do not modify//GEN-BEGIN:variables
    private javax.swing.JButton btnFechar;
    private javax.swing.JLabel jLabelErro;
    private javax.swing.JLabel txtMensagemErro;
    // End of variables declaration//GEN-END:variables
}