package com.mycompany.intregavellinguagemprogramacao;

import java.awt.Dimension;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;

/**
 *
 * @author Cesar
 */
public class Monitoramento extends javax.swing.JFrame {

    public Monitoramento() {
        initComponents();
    }

    @SuppressWarnings("unchecked")
    // <editor-fold defaultstate="collapsed" desc="Generated Code">//GEN-BEGIN:initComponents
    private void initComponents() {

        jPanel1 = new javax.swing.JPanel();
        lbCPU = new javax.swing.JLabel();
        lbMemoria = new javax.swing.JLabel();
        lbPercentMemoria = new javax.swing.JLabel();
        lbDisco = new javax.swing.JLabel();
        lbPercentDisco = new javax.swing.JLabel();
        lbPercentCPU = new javax.swing.JLabel();
        jPanel4 = new javax.swing.JPanel();
        jPanel3 = new javax.swing.JPanel();
        jpChartDisco = new javax.swing.JPanel();
        jpChartMemoria = new javax.swing.JPanel();
        jpChartCPU = new javax.swing.JPanel();
        jLabel2 = new javax.swing.JLabel();
        lbAtualizar = new javax.swing.JLabel();
        jLabel1 = new javax.swing.JLabel();

        setDefaultCloseOperation(javax.swing.WindowConstants.EXIT_ON_CLOSE);
        setBackground(new java.awt.Color(255, 255, 255));
        setModalExclusionType(null);
        setResizable(false);

        jPanel1.setBackground(new java.awt.Color(255, 255, 255));
        jPanel1.setLayout(new org.netbeans.lib.awtextra.AbsoluteLayout());

        lbCPU.setFont(new java.awt.Font("Arial", 0, 18)); // NOI18N
        lbCPU.setText("CPU");
        jPanel1.add(lbCPU, new org.netbeans.lib.awtextra.AbsoluteConstraints(20, 80, -1, 36));

        lbMemoria.setFont(new java.awt.Font("Arial", 0, 18)); // NOI18N
        lbMemoria.setText("Memória");
        lbMemoria.setName(""); // NOI18N
        jPanel1.add(lbMemoria, new org.netbeans.lib.awtextra.AbsoluteConstraints(10, 140, -1, 36));

        lbPercentMemoria.setFont(new java.awt.Font("Tahoma", 0, 14)); // NOI18N
        lbPercentMemoria.setHorizontalAlignment(javax.swing.SwingConstants.RIGHT);
        lbPercentMemoria.setText("0%");
        jPanel1.add(lbPercentMemoria, new org.netbeans.lib.awtextra.AbsoluteConstraints(90, 150, 30, -1));

        lbDisco.setFont(new java.awt.Font("Arial", 0, 18)); // NOI18N
        lbDisco.setText("Disco");
        jPanel1.add(lbDisco, new org.netbeans.lib.awtextra.AbsoluteConstraints(20, 200, -1, 36));

        lbPercentDisco.setFont(new java.awt.Font("Tahoma", 0, 14)); // NOI18N
        lbPercentDisco.setHorizontalAlignment(javax.swing.SwingConstants.RIGHT);
        lbPercentDisco.setText("0%");
        jPanel1.add(lbPercentDisco, new org.netbeans.lib.awtextra.AbsoluteConstraints(90, 210, 30, -1));
        lbPercentDisco.getAccessibleContext().setAccessibleName("10%");

        lbPercentCPU.setFont(new java.awt.Font("Tahoma", 0, 14)); // NOI18N
        lbPercentCPU.setHorizontalAlignment(javax.swing.SwingConstants.RIGHT);
        lbPercentCPU.setText("0%");
        jPanel1.add(lbPercentCPU, new org.netbeans.lib.awtextra.AbsoluteConstraints(90, 90, 30, -1));

        jPanel4.setBackground(new java.awt.Color(0, 0, 0));
        jPanel4.setPreferredSize(new java.awt.Dimension(100, 3));

        javax.swing.GroupLayout jPanel4Layout = new javax.swing.GroupLayout(jPanel4);
        jPanel4.setLayout(jPanel4Layout);
        jPanel4Layout.setHorizontalGroup(
            jPanel4Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGap(0, 340, Short.MAX_VALUE)
        );
        jPanel4Layout.setVerticalGroup(
            jPanel4Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGap(0, 0, Short.MAX_VALUE)
        );

        jPanel1.add(jPanel4, new org.netbeans.lib.awtextra.AbsoluteConstraints(110, 250, 340, -1));

        jPanel3.setBackground(new java.awt.Color(0, 0, 0));
        jPanel3.setPreferredSize(new java.awt.Dimension(3, 100));

        javax.swing.GroupLayout jPanel3Layout = new javax.swing.GroupLayout(jPanel3);
        jPanel3.setLayout(jPanel3Layout);
        jPanel3Layout.setHorizontalGroup(
            jPanel3Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGap(0, 3, Short.MAX_VALUE)
        );
        jPanel3Layout.setVerticalGroup(
            jPanel3Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGap(0, 200, Short.MAX_VALUE)
        );

        jPanel1.add(jPanel3, new org.netbeans.lib.awtextra.AbsoluteConstraints(130, 70, -1, 200));

        jpChartDisco.setBackground(new java.awt.Color(255, 0, 51));
        jpChartDisco.setAlignmentX(10.5F);
        jpChartDisco.setPreferredSize(new java.awt.Dimension(0, 30));

        javax.swing.GroupLayout jpChartDiscoLayout = new javax.swing.GroupLayout(jpChartDisco);
        jpChartDisco.setLayout(jpChartDiscoLayout);
        jpChartDiscoLayout.setHorizontalGroup(
            jpChartDiscoLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGap(0, 0, Short.MAX_VALUE)
        );
        jpChartDiscoLayout.setVerticalGroup(
            jpChartDiscoLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGap(0, 30, Short.MAX_VALUE)
        );

        jPanel1.add(jpChartDisco, new org.netbeans.lib.awtextra.AbsoluteConstraints(133, 205, -1, 30));

        jpChartMemoria.setBackground(new java.awt.Color(0, 255, 204));
        jpChartMemoria.setAlignmentX(10.5F);
        jpChartMemoria.setPreferredSize(new java.awt.Dimension(0, 30));

        javax.swing.GroupLayout jpChartMemoriaLayout = new javax.swing.GroupLayout(jpChartMemoria);
        jpChartMemoria.setLayout(jpChartMemoriaLayout);
        jpChartMemoriaLayout.setHorizontalGroup(
            jpChartMemoriaLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGap(0, 0, Short.MAX_VALUE)
        );
        jpChartMemoriaLayout.setVerticalGroup(
            jpChartMemoriaLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGap(0, 30, Short.MAX_VALUE)
        );

        jPanel1.add(jpChartMemoria, new org.netbeans.lib.awtextra.AbsoluteConstraints(133, 145, -1, -1));

        jpChartCPU.setBackground(new java.awt.Color(0, 255, 0));
        jpChartCPU.setAlignmentX(10.5F);
        jpChartCPU.setPreferredSize(new java.awt.Dimension(0, 30));

        javax.swing.GroupLayout jpChartCPULayout = new javax.swing.GroupLayout(jpChartCPU);
        jpChartCPU.setLayout(jpChartCPULayout);
        jpChartCPULayout.setHorizontalGroup(
            jpChartCPULayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGap(0, 0, Short.MAX_VALUE)
        );
        jpChartCPULayout.setVerticalGroup(
            jpChartCPULayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGap(0, 30, Short.MAX_VALUE)
        );

        jPanel1.add(jpChartCPU, new org.netbeans.lib.awtextra.AbsoluteConstraints(133, 85, -1, -1));

        jLabel2.setFont(new java.awt.Font("Arial", 0, 24)); // NOI18N
        jLabel2.setText("Monitoramento");
        jPanel1.add(jLabel2, new org.netbeans.lib.awtextra.AbsoluteConstraints(150, 10, -1, -1));

        lbAtualizar.setIcon(new javax.swing.ImageIcon("C:\\Users\\cavecchio\\Desktop\\Projetos_Java\\IntregavelLinguagemProgramacao\\src\\main\\java\\com\\mycompany\\intregavellinguagemprogramacao\\refrescar.png")); // NOI18N
        lbAtualizar.setBorder(javax.swing.BorderFactory.createLineBorder(new java.awt.Color(0, 0, 0)));
        lbAtualizar.setCursor(new java.awt.Cursor(java.awt.Cursor.HAND_CURSOR));
        lbAtualizar.setPreferredSize(new java.awt.Dimension(35, 35));
        lbAtualizar.addMouseListener(new java.awt.event.MouseAdapter() {
            public void mouseClicked(java.awt.event.MouseEvent evt) {
                lbAtualizarMouseClicked(evt);
            }
        });

        jLabel1.setHorizontalAlignment(javax.swing.SwingConstants.CENTER);
        jLabel1.setText("Atualizar");

        javax.swing.GroupLayout layout = new javax.swing.GroupLayout(getContentPane());
        getContentPane().setLayout(layout);
        layout.setHorizontalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addComponent(jPanel1, javax.swing.GroupLayout.Alignment.TRAILING, javax.swing.GroupLayout.DEFAULT_SIZE, 470, Short.MAX_VALUE)
            .addGroup(layout.createSequentialGroup()
                .addGap(219, 219, 219)
                .addComponent(lbAtualizar, javax.swing.GroupLayout.PREFERRED_SIZE, 36, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addContainerGap(javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
            .addGroup(javax.swing.GroupLayout.Alignment.TRAILING, layout.createSequentialGroup()
                .addContainerGap(javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                .addComponent(jLabel1, javax.swing.GroupLayout.PREFERRED_SIZE, 52, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addGap(207, 207, 207))
        );
        layout.setVerticalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(layout.createSequentialGroup()
                .addComponent(jPanel1, javax.swing.GroupLayout.DEFAULT_SIZE, 287, Short.MAX_VALUE)
                .addGap(18, 18, 18)
                .addComponent(lbAtualizar, javax.swing.GroupLayout.PREFERRED_SIZE, 36, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(jLabel1)
                .addContainerGap())
        );

        pack();
    }// </editor-fold>//GEN-END:initComponents

    private void lbAtualizarMouseClicked(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_lbAtualizarMouseClicked
        Random gerador = new Random();
        Integer pcCPU, pcDisco, pcMemoria;
        Timer timer = new Timer();
        
        
        pcCPU = gerador.nextInt(101);
        pcMemoria = gerador.nextInt(101);
        pcDisco = gerador.nextInt(101);

        
        jpChartCPU.setPreferredSize(new Dimension(pcCPU * 3, jpChartCPU.getHeight()));       
        jpChartMemoria.setPreferredSize(new Dimension(pcMemoria * 3, jpChartMemoria.getHeight()));
        jpChartDisco.setPreferredSize(new Dimension(pcDisco * 3, jpChartDisco.getHeight()));
       
                
        
        lbPercentCPU.setText(pcCPU+"%");
        lbPercentMemoria.setText(pcMemoria+"%");
        lbPercentDisco.setText(pcDisco+"%"); 
        
        
    }//GEN-LAST:event_lbAtualizarMouseClicked

    /**
     * @param args the command line arguments
     */
    public static void main(String args[]) {
        /* Set the Nimbus look and feel */
        //<editor-fold defaultstate="collapsed" desc=" Look and feel setting code (optional) ">
        /* If Nimbus (introduced in Java SE 6) is not available, stay with the default look and feel.
         * For details see http://download.oracle.com/javase/tutorial/uiswing/lookandfeel/plaf.html 
         */
        try {
            for (javax.swing.UIManager.LookAndFeelInfo info : javax.swing.UIManager.getInstalledLookAndFeels()) {
                if ("Nimbus".equals(info.getName())) {
                    javax.swing.UIManager.setLookAndFeel(info.getClassName());
                    break;
                }
            }
        } catch (ClassNotFoundException ex) {
            java.util.logging.Logger.getLogger(Monitoramento.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        } catch (InstantiationException ex) {
            java.util.logging.Logger.getLogger(Monitoramento.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        } catch (IllegalAccessException ex) {
            java.util.logging.Logger.getLogger(Monitoramento.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        } catch (javax.swing.UnsupportedLookAndFeelException ex) {
            java.util.logging.Logger.getLogger(Monitoramento.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        }
        //</editor-fold>

        /* Create and display the form */
        java.awt.EventQueue.invokeLater(new Runnable() {
            public void run() {
                new Monitoramento().setVisible(true);
            }
        });
    }

    // Variables declaration - do not modify//GEN-BEGIN:variables
    private javax.swing.JLabel jLabel1;
    private javax.swing.JLabel jLabel2;
    private javax.swing.JPanel jPanel1;
    private javax.swing.JPanel jPanel3;
    private javax.swing.JPanel jPanel4;
    private javax.swing.JPanel jpChartCPU;
    private javax.swing.JPanel jpChartDisco;
    private javax.swing.JPanel jpChartMemoria;
    private javax.swing.JLabel lbAtualizar;
    private javax.swing.JLabel lbCPU;
    private javax.swing.JLabel lbDisco;
    private javax.swing.JLabel lbMemoria;
    private javax.swing.JLabel lbPercentCPU;
    private javax.swing.JLabel lbPercentDisco;
    private javax.swing.JLabel lbPercentMemoria;
    // End of variables declaration//GEN-END:variables
}