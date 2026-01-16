package sniffer_redes;

import java.awt.event.KeyEvent;
import java.io.File;
import java.util.ArrayList;
import javax.swing.JFileChooser;
import javax.swing.filechooser.FileNameExtensionFilter;
import javax.swing.table.DefaultTableModel;
import org.jnetpcap.Pcap;
import org.jnetpcap.packet.JPacket;
import org.jnetpcap.packet.PcapPacket;
import org.jnetpcap.packet.PcapPacketHandler;
import org.jnetpcap.util.PcapPacketArrayList;


public class Analisis_Archivo extends javax.swing.JFrame {
    /* CONSTANTES */
    final int IPV4 = 2048;
    final int ARP = 2054;
    final int IPV6 = 34525;
    
    private File archivo;
    DefaultTableModel modeloP = new DefaultTableModel() {@Override
    public boolean isCellEditable(int rowIndex, int colIndex) { return false; } };
    private ArrayList <Trama> paquetes = new ArrayList<>();
    private Trama paquete;
    PcapPacketArrayList paq;
    private ArrayList <Aux_Cuenta> longitudes = new ArrayList <>();
    private ArrayList <Aux_Cuenta> tipos = new ArrayList <>();
    
    public Analisis_Archivo() {
        initComponents();
        this.setLocationRelativeTo(null);
        JFileChooser file = new JFileChooser();
        FileNameExtensionFilter filtroPcap = new FileNameExtensionFilter("*.PCAP", "pcap", ".pcap");
        file.setFileFilter(filtroPcap);
        file.showOpenDialog(this);
        archivo = file.getSelectedFile();
        lblNombreArchivo.setText("El archivo a analizar es: " + archivo.getName());
        paq = leerArchivo();
        llenarTabla();
        System.out.println("Tabla llena!!\nLista de tamaños: ");
        for(int h = 0 ; h < longitudes.size() ; h++) {
            System.out.println("Nombre: " + longitudes.get(h).getNombre() + " Veces: " + longitudes.get(h).getCuenta());
        }
        System.out.println("**********");
        for(int h = 0 ; h < tipos.size() ; h++) {
            System.out.println("Nombre: " + tipos.get(h).getNombre() + " Veces: " + tipos.get(h).getCuenta());
        }
        
        areaInformacion.setVisible(false);
        areaInformacion.setEditable(false);
        btnEstadistica.setToolTipText("Generar Estadística.");
    }
    
    private void llenarTabla() {
        String entrada_tabla[] = new String[6]; 
        String tempo;
        modeloP.addColumn("Número");
        modeloP.addColumn("MAC Fuente");
        modeloP.addColumn("MAC Destino");
        modeloP.addColumn("Tipo");
        modeloP.addColumn("Longitud Total");           
        for(int k = 0 ; k < paq.size() ; k++) {
            paquete = new Trama();
            /* Columna 1 */
            tempo = "";
            PcapPacket t_paquete = paq.get(k);    
            paquete.setLongitud(t_paquete.size());            
            entrada_tabla[0] = String.valueOf(k + 1);
            for(int i = 0; i < 6; i++){
                tempo = tempo.concat(String.format("%02X", t_paquete.getUByte(i)));
                tempo = tempo.concat(" ");                
            }
            entrada_tabla[1] = tempo;
            paquete.setMacd(tempo);
            /* Columna 2 */
            tempo = "";
            for(int i = 6; i < 12; i++){
                tempo = tempo.concat(String.format("%02X", t_paquete.getUByte(i)));
                tempo = tempo.concat(" ");                
            }
            entrada_tabla[2] = tempo;
            paquete.setMaco(tempo);
            /* Columna 3 */
            String tempo_1;
            int tipo = (t_paquete.getUByte(12) * 256 ) + (t_paquete.getUByte(13));
            
            if(String.valueOf(t_paquete.getUByte(12)).length() == 1) {
                tempo_1 = '0' + String.valueOf(t_paquete.getUByte(12));               
            } else {
                tempo_1 = Integer.toHexString(t_paquete.getUByte(12));
            }
            
            paquete.setTipo_byte1(tempo_1);
            
            if(String.valueOf(t_paquete.getUByte(13)).length() == 1) {
                tempo_1 = '0' + String.valueOf(t_paquete.getUByte(13));               
            } else {
                tempo_1 = Integer.toHexString(t_paquete.getUByte(13));
            }
            paquete.setTipo_byte2(tempo_1);
            paquete.setTipo(tipo);
            
            if(tipo > 1500) {
                /* Ethernet */
                entrada_tabla[3] = "Ethernet";              
            } else {
                /* IEEE */
                entrada_tabla[3] = "IEEE";
            }  
            cuentaTamano(entrada_tabla[3], tipos);
            /* Columna 4 */
            entrada_tabla[4] = String.valueOf(t_paquete.size()) + " bytes";
            cuentaTamano(String.valueOf(t_paquete.size()), longitudes);
            
            
            modeloP.addRow(entrada_tabla);
            paquetes.add(paquete);
        }
        tableTramas.setModel(modeloP); 
        tableTramas.getColumnModel().getColumn(0).setMaxWidth(60);
        //tableTramas.getColumnModel().getColumn(4).setMaxWidth(120);                 
    }

    private void mostrarInfo(){
        int fila_selec = tableTramas.getSelectedRow();
       JPacket pakete = paq.get(fila_selec);
       Trama paquete;
       String texto = "";
       String type = "Unknown";
       if(fila_selec != -1) {
           areaInformacion.setVisible(true);
           paquete = paquetes.get(fila_selec);
           if(paquete.getTipo() > 1500) {
               /* ETHERNET */
               texto = texto + "ETHERNET\n";
               texto = texto + "\tMAC Destino:\t" + paquete.getMacd();
               texto = texto + ".\n";
               texto = texto + "\tMAC Origen:\t" + paquete.getMaco();
               texto = texto + ".\n";               
               
               String temp_red = "";
               if(paquete.getTipo() == IPV4) {
                   type = "IPv4";
                   //temp_red = esIPv4(pakete);
                   Informacion_IPv4 info_ipv4 = new Informacion_IPv4(pakete);
                   temp_red = info_ipv4.getInformacionDetallada();
               } else if(paquete.getTipo() == ARP) {
                   type = "ARP";
                   Informacion_ARP info_arp = new Informacion_ARP(pakete);
                   //temp_red = esARP(pakete);
                   temp_red = info_arp.getInformacionDetallada();
               } else if(paquete.getTipo() == IPV6) {
                   type = "IPv6";
               }                
               texto = texto + "\tTipo:\t" + type + " (0x"+ paquete.getTipo_byte1() + paquete.getTipo_byte2() + ")";      
               texto += temp_red;
               areaInformacion.setText(texto);
            } else { /* ES IEEE */
               Informacion_LLC iL = new Informacion_LLC();
               areaInformacion.setText(iL.impresionInformacion(pakete));
            }
       }
    }
    
    public void cuentaTamano(String nombre, ArrayList <Aux_Cuenta> arr_auxCuenta) {               
        Aux_Cuenta ac;
        boolean flag = false;
        int tempo;
        for(int r = 0 ; r < arr_auxCuenta.size() ; r++) {
            if(nombre.compareTo(arr_auxCuenta.get(r).getNombre()) == 0) {
                /* Ya está en la lista, solo sumar su atributo de num */
                tempo = arr_auxCuenta.get(r).getCuenta();
                tempo++;
                arr_auxCuenta.get(r).setCuenta(tempo);
                flag = true;
            } 
        }
        if(!flag) {
            ac = new Aux_Cuenta(nombre, 1);
            arr_auxCuenta.add(ac);
        }
    }
    
    public PcapPacketArrayList leerArchivo() {
        final String FILENAME = archivo.getPath();  
        final StringBuilder errbuf = new StringBuilder(); 
        int count = 0;
        final Pcap pcap = Pcap.openOffline(FILENAME, errbuf);  
        if (pcap == null) {  
            System.err.println(errbuf); // Error is stored in errbuf if any  
            return null;  
        }
        
        PcapPacketHandler<PcapPacketArrayList> jpacketHandler = new PcapPacketHandler<PcapPacketArrayList>() {  
            
            @Override
            public void nextPacket(PcapPacket packet, PcapPacketArrayList PaketsList) {      
                PaketsList.add(packet);
            }  
        };
        
        try {  
            PcapPacketArrayList packets = new PcapPacketArrayList();
            pcap.loop(-1,jpacketHandler,packets);
            
            return packets;
          } finally {              
             pcap.close();  
          } 
    } //Función
    /**
     * This method is called from within the constructor to initialize the form.
     * WARNING: Do NOT modify this code. The content of this method is always
     * regenerated by the Form Editor.
     */
    @SuppressWarnings("unchecked")
    // <editor-fold defaultstate="collapsed" desc="Generated Code">//GEN-BEGIN:initComponents
    private void initComponents() {

        jLabel2 = new javax.swing.JLabel();
        lblNombreArchivo = new javax.swing.JLabel();
        jScrollPane1 = new javax.swing.JScrollPane();
        tableTramas = new javax.swing.JTable();
        jScrollPane2 = new javax.swing.JScrollPane();
        areaInformacion = new javax.swing.JTextArea();
        btnEstadistica = new javax.swing.JButton();
        jLabel1 = new javax.swing.JLabel();

        setDefaultCloseOperation(javax.swing.WindowConstants.EXIT_ON_CLOSE);
        setResizable(false);
        getContentPane().setLayout(new org.netbeans.lib.awtextra.AbsoluteLayout());

        jLabel2.setFont(new java.awt.Font("AR ESSENCE", 1, 24)); // NOI18N
        jLabel2.setForeground(new java.awt.Color(25, 25, 112));
        jLabel2.setText("ANÁLISIS DE PAQUETES DESDE ARCHIVO .PCAP");
        getContentPane().add(jLabel2, new org.netbeans.lib.awtextra.AbsoluteConstraints(140, 10, -1, -1));

        lblNombreArchivo.setFont(new java.awt.Font("Segoe UI Symbol", 1, 12)); // NOI18N
        lblNombreArchivo.setForeground(new java.awt.Color(25, 25, 112));
        lblNombreArchivo.setText("lblNombreArchivo");
        getContentPane().add(lblNombreArchivo, new org.netbeans.lib.awtextra.AbsoluteConstraints(10, 50, 530, -1));

        tableTramas.setModel(new javax.swing.table.DefaultTableModel(
            new Object [][] {
                {null, null, null, null},
                {null, null, null, null},
                {null, null, null, null},
                {null, null, null, null}
            },
            new String [] {
                "Title 1", "Title 2", "Title 3", "Title 4"
            }
        ));
        tableTramas.setAutoResizeMode(javax.swing.JTable.AUTO_RESIZE_ALL_COLUMNS);
        tableTramas.addMouseListener(new java.awt.event.MouseAdapter() {
            public void mousePressed(java.awt.event.MouseEvent evt) {
                tableTramasMousePressed(evt);
            }
        });
        tableTramas.addKeyListener(new java.awt.event.KeyAdapter() {
            public void keyPressed(java.awt.event.KeyEvent evt) {
                tableTramasKeyPressed(evt);
            }
        });
        jScrollPane1.setViewportView(tableTramas);

        getContentPane().add(jScrollPane1, new org.netbeans.lib.awtextra.AbsoluteConstraints(10, 80, 820, 150));

        areaInformacion.setColumns(20);
        areaInformacion.setRows(5);
        jScrollPane2.setViewportView(areaInformacion);

        getContentPane().add(jScrollPane2, new org.netbeans.lib.awtextra.AbsoluteConstraints(10, 250, 820, 120));

        btnEstadistica.setIcon(new javax.swing.ImageIcon(getClass().getResource("/imagenes/estadistica.png"))); // NOI18N
        btnEstadistica.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                btnEstadisticaActionPerformed(evt);
            }
        });
        getContentPane().add(btnEstadistica, new org.netbeans.lib.awtextra.AbsoluteConstraints(750, 10, 70, 50));

        jLabel1.setIcon(new javax.swing.ImageIcon(getClass().getResource("/sniffer_redes/Fondo.png"))); // NOI18N
        getContentPane().add(jLabel1, new org.netbeans.lib.awtextra.AbsoluteConstraints(0, 0, 840, 385));

        pack();
    }// </editor-fold>//GEN-END:initComponents

    private void tableTramasMousePressed(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_tableTramasMousePressed
       mostrarInfo();
    }//GEN-LAST:event_tableTramasMousePressed

    
    private String fillZ (String binary) {
        int t_ctrl = 8 - binary.length();
        String tempo = "";
        for(int b = 0 ; b < t_ctrl ; b++) {
            tempo = tempo.concat("0");
        }                             
        tempo = tempo.concat(binary);
        binary = tempo.substring(0);
        
        return binary;
    }
    
    private void btnEstadisticaActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_btnEstadisticaActionPerformed
        /*Estadistica_Archivo ea = new Estadistica_Archivo();
        ea.setVisible(true);
        ea.cargaArreglos(longitudes, tipos);
        ea.hacerGrafica();*/
    }//GEN-LAST:event_btnEstadisticaActionPerformed

    private void tableTramasKeyPressed(java.awt.event.KeyEvent evt) {//GEN-FIRST:event_tableTramasKeyPressed
        if(evt.getKeyCode()==KeyEvent.VK_DOWN || evt.getKeyCode()==KeyEvent.VK_UP){
            mostrarInfo();
        }
    }//GEN-LAST:event_tableTramasKeyPressed

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
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | javax.swing.UnsupportedLookAndFeelException ex) {
            java.util.logging.Logger.getLogger(Analisis_Archivo.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        }
        //</editor-fold>
        
        //</editor-fold>

        /* Create and display the form */
        java.awt.EventQueue.invokeLater(() -> {
            new Analisis_Archivo().setVisible(true);
        });
    }

    // Variables declaration - do not modify//GEN-BEGIN:variables
    private javax.swing.JTextArea areaInformacion;
    private javax.swing.JButton btnEstadistica;
    private javax.swing.JLabel jLabel1;
    private javax.swing.JLabel jLabel2;
    private javax.swing.JScrollPane jScrollPane1;
    private javax.swing.JScrollPane jScrollPane2;
    private javax.swing.JLabel lblNombreArchivo;
    private javax.swing.JTable tableTramas;
    // End of variables declaration//GEN-END:variables
}