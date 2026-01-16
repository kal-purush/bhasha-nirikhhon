
package Presentacion;

import LogicaNegocio.DatosProfesionales;
import LogicaNegocio.Menu;
import LogicaNegocio.DatosUsuario;
import LogicaNegocio.Prueba;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;
import javax.swing.DefaultListModel;
import javax.swing.JComboBox;
import javax.swing.JOptionPane;
import javax.swing.table.DefaultTableModel;

public class DatosPrueba extends javax.swing.JFrame {
    
    private Prueba DPrueba=new Prueba();
    private ArrayList<Prueba> listaPrueba=new ArrayList<Prueba>();
    private Menu archivos=new Menu();
    private DefaultTableModel dtmPrueba=new DefaultTableModel();
    private DefaultListModel dlmPrueba=new DefaultListModel();
    private JComboBox combo;
    private Object[] datos=new Object[3];
    private ArrayList arregloLista=new ArrayList();
    private File archivo =new File ("Datos.txt");
    private FileReader fr;// = new FileReader (archivo);
    private BufferedReader  br; // = new BufferedReader(fr);
    private FileWriter fw; //=new FileWriter("estudiantes.txt", true);
    
    //----- CONSTRUCTORES -------
     public DatosPrueba() {
        initComponents();
        dtmPrueba=(DefaultTableModel) tablaPrueba.getModel();
    }
  
    public void inicializarArchivoEscritura() throws IOException 
    {
        fw = new FileWriter("Datos.txt", true);//ubica y abre el archivo MODO APPEND no OVERRIDE
        BufferedWriter bw = new BufferedWriter(fw); //activa Buffer memoria con puntero de EscribirArchivo
    }
    public void inicializarArchivoLectura() throws IOException 
    {
        fr = new FileReader("Datos.txt");//ubica y abre el archivo MODO READ
        BufferedReader bw = new BufferedReader(fr); //activa Buffer memoria con puntero de LeerArchivo
    }
    @SuppressWarnings("unchecked")
    // <editor-fold defaultstate="collapsed" desc="Generated Code">//GEN-BEGIN:initComponents
    private void initComponents() {

        jLabel1 = new javax.swing.JLabel();
        lblEscritorio = new javax.swing.JLabel();
        lblBiblioteca = new javax.swing.JLabel();
        lblRepisa = new javax.swing.JLabel();
        txtEscritorio = new javax.swing.JTextField();
        txtBiblioteca = new javax.swing.JTextField();
        btnActualizar = new javax.swing.JButton();
        txtRepisa = new javax.swing.JTextField();
        jScrollPane1 = new javax.swing.JScrollPane();
        tablaPrueba = new javax.swing.JTable();
        btnGuardarArchivo = new javax.swing.JButton();
        btnSalir = new javax.swing.JButton();
        btnTablaformulario = new javax.swing.JButton();

        setDefaultCloseOperation(javax.swing.WindowConstants.EXIT_ON_CLOSE);

        jLabel1.setFont(new java.awt.Font("Bertha Melanie", 1, 24)); // NOI18N
        jLabel1.setText("DATOS OFERTA");

        lblEscritorio.setFont(new java.awt.Font("Bodoni MT", 0, 18)); // NOI18N
        lblEscritorio.setText("Descuento");

        lblBiblioteca.setFont(new java.awt.Font("Bodoni MT", 0, 18)); // NOI18N
        lblBiblioteca.setText("Temporada");

        lblRepisa.setFont(new java.awt.Font("Bodoni MT", 0, 18)); // NOI18N
        lblRepisa.setText("Gasto en oferta");

        txtEscritorio.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                txtEscritorioActionPerformed(evt);
            }
        });

        btnActualizar.setText("Eliminar Fila");
        btnActualizar.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                btnActualizarActionPerformed(evt);
            }
        });

        tablaPrueba.setBorder(javax.swing.BorderFactory.createLineBorder(new java.awt.Color(0, 0, 0)));
        tablaPrueba.setModel(new javax.swing.table.DefaultTableModel(
            new Object [][] {

            },
            new String [] {
                "DESCUENTO", "TEMPORADA", "GASTO"
            }
        ));
        jScrollPane1.setViewportView(tablaPrueba);

        btnGuardarArchivo.setText("Guardar Archivo");
        btnGuardarArchivo.addMouseListener(new java.awt.event.MouseAdapter() {
            public void mouseClicked(java.awt.event.MouseEvent evt) {
                btnGuardarArchivoMouseClicked(evt);
            }
        });
        btnGuardarArchivo.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                btnGuardarArchivoActionPerformed(evt);
            }
        });

        btnSalir.setBackground(new java.awt.Color(255, 204, 204));
        btnSalir.setFont(new java.awt.Font("Bodoni MT", 0, 18)); // NOI18N
        btnSalir.setForeground(new java.awt.Color(204, 0, 0));
        btnSalir.setText("SALIR");
        btnSalir.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                btnSalirActionPerformed(evt);
            }
        });

        btnTablaformulario.setText("Formulario a Tabla");
        btnTablaformulario.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                btnTablaformularioActionPerformed(evt);
            }
        });

        javax.swing.GroupLayout layout = new javax.swing.GroupLayout(getContentPane());
        getContentPane().setLayout(layout);
        layout.setHorizontalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(layout.createSequentialGroup()
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.TRAILING)
                    .addGroup(layout.createSequentialGroup()
                        .addGap(116, 125, Short.MAX_VALUE)
                        .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                            .addComponent(jScrollPane1, javax.swing.GroupLayout.Alignment.TRAILING, javax.swing.GroupLayout.PREFERRED_SIZE, 460, javax.swing.GroupLayout.PREFERRED_SIZE)
                            .addGroup(javax.swing.GroupLayout.Alignment.TRAILING, layout.createSequentialGroup()
                                .addComponent(jLabel1, javax.swing.GroupLayout.PREFERRED_SIZE, 215, javax.swing.GroupLayout.PREFERRED_SIZE)
                                .addGap(132, 132, 132))))
                    .addGroup(layout.createSequentialGroup()
                        .addGap(73, 73, 73)
                        .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                            .addComponent(lblEscritorio, javax.swing.GroupLayout.PREFERRED_SIZE, 124, javax.swing.GroupLayout.PREFERRED_SIZE)
                            .addComponent(lblBiblioteca, javax.swing.GroupLayout.PREFERRED_SIZE, 124, javax.swing.GroupLayout.PREFERRED_SIZE)
                            .addComponent(lblRepisa, javax.swing.GroupLayout.PREFERRED_SIZE, 124, javax.swing.GroupLayout.PREFERRED_SIZE))
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                        .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.TRAILING, false)
                            .addComponent(txtEscritorio, javax.swing.GroupLayout.Alignment.LEADING, javax.swing.GroupLayout.DEFAULT_SIZE, 115, Short.MAX_VALUE)
                            .addComponent(txtRepisa, javax.swing.GroupLayout.Alignment.LEADING)
                            .addComponent(txtBiblioteca))
                        .addGap(69, 69, 69)))
                .addGap(0, 115, Short.MAX_VALUE))
            .addGroup(layout.createSequentialGroup()
                .addGap(87, 87, 87)
                .addComponent(btnActualizar)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                .addComponent(btnGuardarArchivo, javax.swing.GroupLayout.PREFERRED_SIZE, 115, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addGap(38, 38, 38)
                .addComponent(btnTablaformulario)
                .addGap(84, 84, 84)
                .addComponent(btnSalir)
                .addGap(47, 47, 47))
        );
        layout.setVerticalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(layout.createSequentialGroup()
                .addGap(28, 28, 28)
                .addComponent(jLabel1)
                .addGap(27, 27, 27)
                .addComponent(jScrollPane1, javax.swing.GroupLayout.PREFERRED_SIZE, 73, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addGap(48, 48, 48)
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING, false)
                    .addComponent(lblEscritorio, javax.swing.GroupLayout.DEFAULT_SIZE, 31, Short.MAX_VALUE)
                    .addComponent(txtEscritorio))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.UNRELATED, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING, false)
                    .addComponent(lblBiblioteca, javax.swing.GroupLayout.PREFERRED_SIZE, 30, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addGroup(layout.createSequentialGroup()
                        .addGap(7, 7, 7)
                        .addComponent(txtBiblioteca, javax.swing.GroupLayout.DEFAULT_SIZE, 28, Short.MAX_VALUE)))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING, false)
                    .addComponent(lblRepisa, javax.swing.GroupLayout.DEFAULT_SIZE, 30, Short.MAX_VALUE)
                    .addComponent(txtRepisa))
                .addGap(65, 65, 65)
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(btnActualizar, javax.swing.GroupLayout.PREFERRED_SIZE, 36, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(btnGuardarArchivo, javax.swing.GroupLayout.PREFERRED_SIZE, 36, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(btnSalir)
                    .addComponent(btnTablaformulario, javax.swing.GroupLayout.PREFERRED_SIZE, 36, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addGap(74, 74, 74))
        );

        pack();
    }// </editor-fold>//GEN-END:initComponents

    private void txtEscritorioActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_txtEscritorioActionPerformed
        // TODO add your handling code here:
    }//GEN-LAST:event_txtEscritorioActionPerformed

    private void btnActualizarActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_btnActualizarActionPerformed
        try{
            JOptionPane.showMessageDialog(null, "Elimina 1 registro ");
            dtmPrueba.removeRow(tablaPrueba.getSelectedRow());
        }catch(ArrayIndexOutOfBoundsException ae)
        {
            JOptionPane.showMessageDialog(null, "final del archivo "+ ae.getMessage());
        }
    }//GEN-LAST:event_btnActualizarActionPerformed

    private void btnGuardarArchivoMouseClicked(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_btnGuardarArchivoMouseClicked
        
    }//GEN-LAST:event_btnGuardarArchivoMouseClicked

    private void btnGuardarArchivoActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_btnGuardarArchivoActionPerformed
         guardarTablaArchivo();
        // TODO add your handling code here:
    }//GEN-LAST:event_btnGuardarArchivoActionPerformed

    private void btnSalirActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_btnSalirActionPerformed
        System.exit(0);
        // TODO add your handling code here:
    }//GEN-LAST:event_btnSalirActionPerformed

    private void btnTablaformularioActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_btnTablaformularioActionPerformed
        llenarTablaOficina(); 
        // TODO add your handling code here:
    }//GEN-LAST:event_btnTablaformularioActionPerformed

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
            java.util.logging.Logger.getLogger(DatosOficina.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        } catch (InstantiationException ex) {
            java.util.logging.Logger.getLogger(DatosOficina.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        } catch (IllegalAccessException ex) {
            java.util.logging.Logger.getLogger(DatosOficina.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        } catch (javax.swing.UnsupportedLookAndFeelException ex) {
            java.util.logging.Logger.getLogger(DatosOficina.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        }
        //</editor-fold>

        /* Create and display the form */
        java.awt.EventQueue.invokeLater(new Runnable() {
            public void run() {
                new DatosPrueba().setVisible(true);
            }
        });
    }

    // Variables declaration - do not modify//GEN-BEGIN:variables
    private javax.swing.JButton btnActualizar;
    private javax.swing.JButton btnGuardarArchivo;
    private javax.swing.JButton btnSalir;
    private javax.swing.JButton btnTablaformulario;
    private javax.swing.JLabel jLabel1;
    private javax.swing.JScrollPane jScrollPane1;
    private javax.swing.JLabel lblBiblioteca;
    private javax.swing.JLabel lblEscritorio;
    private javax.swing.JLabel lblRepisa;
    private javax.swing.JTable tablaPrueba;
    private javax.swing.JTextField txtBiblioteca;
    private javax.swing.JTextField txtEscritorio;
    private javax.swing.JTextField txtRepisa;
    // End of variables declaration//GEN-END:variables


     public void limpiarTabla()
    {
        try{
            tablaPrueba.removeAll();
            JOptionPane.showMessageDialog(null, "Tabla Registro # : ");
            for(int i=0; i<=tablaPrueba.getRowCount(); i++)
            {
                JOptionPane.showMessageDialog(null, "Tabla Registro # : "+i);
                dtmPrueba.removeRow(i);
            }
        }catch(Exception ex)
        {
            JOptionPane.showMessageDialog(null, "Revisar : "+ ex.getMessage());
        }
            
    }
     
     public void cargarLista()
    {
        String vescritotio, vbiblioteca, vrepisa;
            for(int i=0; i<tablaPrueba.getRowCount(); i++)
            {
                vescritotio=dtmPrueba.getValueAt(i,0).toString();
                vbiblioteca=dtmPrueba.getValueAt(i,1).toString();
                vrepisa=dtmPrueba.getValueAt(i,2).toString();
                listaPrueba.add(new Prueba(vescritotio, vbiblioteca, vrepisa));
            }  
            JOptionPane.showMessageDialog(null, "desplegar lista ");
            for(Prueba oficina : listaPrueba)
            {
                vescritotio=oficina.getEscritorio();
                vbiblioteca=oficina.getBiblioteca();
                vrepisa=oficina.getRepisa();
                JOptionPane.showMessageDialog(null, "---> " +
                        vescritotio + "="+ vbiblioteca +  "="+ vrepisa);
            }
            JOptionPane.showMessageDialog(null, "JTable ---> Lista ok ");
    }
       

public void cargarTablaLista()
    {
        String vnombre, vid, vlista;
        try{
            String titulos[] = {"ESCRITORIO","BIBLIOTECAS","REPISAS"};
            dtmPrueba.setColumnIdentifiers(titulos);
             for(Prueba DPrueba : listaPrueba)
            {
                datos[0]=DPrueba.getEscritorio();
                datos[1]=DPrueba.getBiblioteca();
                datos[2]=DPrueba.getRepisa();
                dtmPrueba.addRow(datos);
            }
            tablaPrueba.setModel(dtmPrueba);
            JOptionPane.showMessageDialog(null, "Lista---> Tabla ok");
        } catch(Exception e) {
            JOptionPane.showMessageDialog(null,e.getMessage(),"mensaje obtiene ROL",JOptionPane.ERROR_MESSAGE);
        }
    }
 

    private void guardarTablaArchivo() {
          try{
            inicializarArchivoEscritura();
            for(int i=0; i<tablaPrueba.getRowCount(); i++)
            {
                fw.write(dtmPrueba.getValueAt(i,0).toString()+ "\n");
                fw.write(dtmPrueba.getValueAt(i,1).toString()+ "\n");
                fw.write(dtmPrueba.getValueAt(i,2).toString()+ "\n");
            }
            fw.close();
            JOptionPane.showMessageDialog(null, "ARCHIVO GUARDADO SATISFACTORIAMENTE");
        }catch(Exception ex)
        {
            JOptionPane.showMessageDialog(null, "Revisar : "+ ex.getMessage());
        }
}

        public void actualizarTablaArchivo() throws IOException
    {
            JOptionPane.showConfirmDialog(null, "cargar Tabal--> lista");
            cargarLista();
            JOptionPane.showConfirmDialog(null, "limpiar tabla");
            limpiarTabla();
            JOptionPane.showConfirmDialog(null, "cargar lista---> Tabla");
            cargarTablaLista();
            JOptionPane.showConfirmDialog(null, "guardar Tabla--> Archivo");
            guardarTablaArchivo();
    }  
        
      public void llenarTablaOficina() {
        
        try{
            String titulos[] = {"ESCRITORIO","BIBLIOTECA","REPISA"};
            dtmPrueba.setColumnIdentifiers(titulos);
            datos[0]=txtEscritorio.getText();
            datos[1]=txtBiblioteca.getText();
            datos[2]=txtRepisa.getText();
            dtmPrueba.addRow(datos);
            tablaPrueba.setModel(dtmPrueba);
            
        } catch(Exception e) {
            JOptionPane.showMessageDialog(null,e.getMessage(),"mensaje obtiene ROL",JOptionPane.ERROR_MESSAGE);
        }
        limpiar();
             
    }  

    private void limpiar() {
            txtEscritorio.setText(" ");
            txtBiblioteca.setText(" ");
            txtRepisa.setText(" ");
    }
    
    public void llenarTablaArchivo()
    {
            String idCandidatos, nombreCandidatos,listaCandidatos, linea;
        try {
            inicializarArchivoLectura();
            String titulos[] = {"MESAS","SILLAS","BUTACAS"};
            dtmPrueba.setColumnIdentifiers(titulos);
            Scanner registro=null;
            registro=new Scanner(archivo);
          
            while(registro.hasNextLine())
            {
                idCandidatos=registro.nextLine();
                nombreCandidatos=registro.nextLine();
                listaCandidatos=registro.nextLine();
                dtmPrueba.addRow(new Object[]{idCandidatos, nombreCandidatos,listaCandidatos });
            }
      }
      catch(Exception e){
         e.printStackTrace();
      }
    }    
    
     public void tomarDatosTabla(){
        int columna =tablaPrueba.getSelectedRow();
        String  codigo= (tablaPrueba.getValueAt(columna,0).toString());
        
        txtEscritorio.setText(tablaPrueba.getValueAt(columna,0).toString());
        txtBiblioteca.setText(tablaPrueba.getValueAt(columna,1).toString());
        txtRepisa.setText(tablaPrueba.getValueAt(columna,2).toString());
      }

}