/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dent.Int;
 
import static com.dent.Int.IntFactura.xml;
import com.dent.clases.*;
import com.dent.dato.Conexion;
import com.dent.dato.Validaciones;
import java.awt.Color;
import java.awt.event.KeyEvent;
import static java.lang.Double.parseDouble;
import java.net.InetAddress;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.swing.JFrame;
import javax.swing.JOptionPane;
import javax.swing.table.DefaultTableModel;

/**
 *
 * @author DELL
 */
public class IntAltaFac extends javax.swing.JFrame {

    /**
     * Creates new form IntP
     */
  DefaultTableModel modelo=new DefaultTableModel();
  java.util.Date fecha = new Date();
      
        
        int año =fecha.getYear()+1900;
        int mes =fecha.getMonth()+1;
        int dia =fecha.getDate();
 
  ConexionMySQL mysql = new ConexionMySQL();
  public String[] moG= {"","",""};
  double total=0;
  String ipUser;
  String maquinaUser;
  String nombreUser;
   static boolean facturas;
   
  double monedasAnterior; 
   
 

    public static String ipRegistro;
    String fechaCompra = "";
    String fechaFactura = "";

    public IntAltaFac() {
        initComponents();
         facturas= true;
         ipRegistro= maquina();
         String titu[]={"Codigo","Ubicacion","Cantidad","Stock ","Bodega","Tienda"};
         this.setLocationRelativeTo(null);
         Emisor.setLocationRelativeTo(null);
         Ti_pago.setLocationRelativeTo(null);
         modelo.setColumnIdentifiers(titu);
         monedas.setLocationRelativeTo(null);
//        Tabla.setModel(modelo);
        
        
        }
    
    public String maquina(){
       String maquina="";
       try {
            InetAddress localHost = InetAddress.getLocalHost();

            ipUser = localHost.getHostAddress();
            maquinaUser = localHost.getHostName();
            nombreUser = System.getProperty("user.name");
            
            maquina=ipUser+" "+maquinaUser+" ("+nombreUser+")";
            
            
        } catch (Exception e) {
                 JOptionPane.showMessageDialog(null, "Se genero un error al obtener los datos de la red.\n Error: e ");
        }
        
        
        
        
        
       return maquina ; 
    }
    
    public void Calculadora(){
         borram();
        maquinaMo.setText("Maquina: "+nombreUser);
        System.out.println("monedas ");
        String[] mo=mysql.ConsultaMoid();
        moG=mo;
        fechaAn.setText("Fecha: "+mo[1]);
        monAn.setText("$"+mo[2]);
        moDife.setText( "$-"+String.valueOf(mo[2]));
        moDife.setForeground(Color.red);
        System.out.println(mo[0]+" "+mo[1]+" "+mo[2]+" ");
       
        monedas.setVisible(true);
        this.setVisible(false);        
        monedas.setLocationRelativeTo(null);
        Date date = new Date();
        DateFormat hourdateFormat = new SimpleDateFormat("HH:mm:ss dd/MM/yyyy");
        System.out.println("Hora y fecha: "+hourdateFormat.format(date));
        fechaMo.setText(hourdateFormat.format(date));
        
        
        
    }
    
    

   
    @SuppressWarnings("unchecked")
    // <editor-fold defaultstate="collapsed" desc="Generated Code">//GEN-BEGIN:initComponents
    private void initComponents() {

        monedas = new javax.swing.JFrame();
        jPanel4 = new javax.swing.JPanel();
        bi1000i = new javax.swing.JTextField();
        bi500i = new javax.swing.JTextField();
        bi200i = new javax.swing.JTextField();
        bi100i = new javax.swing.JTextField();
        bi50i = new javax.swing.JTextField();
        bi20i = new javax.swing.JTextField();
        mo10i = new javax.swing.JTextField();
        mo5i = new javax.swing.JTextField();
        mo2i = new javax.swing.JTextField();
        mo1i = new javax.swing.JTextField();
        mo05i = new javax.swing.JTextField();
        jLabel6 = new javax.swing.JLabel();
        jLabel7 = new javax.swing.JLabel();
        jLabel8 = new javax.swing.JLabel();
        jLabel9 = new javax.swing.JLabel();
        jLabel11 = new javax.swing.JLabel();
        jLabel12 = new javax.swing.JLabel();
        jLabel13 = new javax.swing.JLabel();
        jLabel14 = new javax.swing.JLabel();
        jLabel15 = new javax.swing.JLabel();
        jLabel28 = new javax.swing.JLabel();
        jLabel30 = new javax.swing.JLabel();
        jPanel5 = new javax.swing.JPanel();
        d500 = new javax.swing.JLabel();
        d200 = new javax.swing.JLabel();
        d50 = new javax.swing.JLabel();
        d2 = new javax.swing.JLabel();
        d1000 = new javax.swing.JLabel();
        d100 = new javax.swing.JLabel();
        d20 = new javax.swing.JLabel();
        d5 = new javax.swing.JLabel();
        d10 = new javax.swing.JLabel();
        d1 = new javax.swing.JLabel();
        d05 = new javax.swing.JLabel();
        jPanel6 = new javax.swing.JPanel();
        totalCam = new javax.swing.JLabel();
        jPanel7 = new javax.swing.JPanel();
        moDife = new javax.swing.JLabel();
        fechaAn = new javax.swing.JLabel();
        monAn = new javax.swing.JLabel();
        fechaMo = new javax.swing.JLabel();
        maquinaMo = new javax.swing.JLabel();
        moComen = new javax.swing.JTextField();
        jButton1 = new javax.swing.JButton();
        jLabel5 = new javax.swing.JLabel();
        Emisor = new javax.swing.JFrame();
        jLabel19 = new javax.swing.JLabel();
        jPanel12 = new javax.swing.JPanel();
        jLabel20 = new javax.swing.JLabel();
        rfcEmi = new javax.swing.JTextField();
        jLabel21 = new javax.swing.JLabel();
        direccionEmi = new javax.swing.JTextField();
        nombreEmi = new javax.swing.JTextField();
        jLabel23 = new javax.swing.JLabel();
        jLabel24 = new javax.swing.JLabel();
        tipoEmi = new javax.swing.JComboBox();
        jButton6 = new javax.swing.JButton();
        jButton3 = new javax.swing.JButton();
        jLabel22 = new javax.swing.JLabel();
        Fondo1 = new javax.swing.JLabel();
        jFrame1 = new javax.swing.JFrame();
        jPanel14 = new javax.swing.JPanel();
        jButton8 = new javax.swing.JButton();
        jComboBox1 = new javax.swing.JComboBox<>();
        jTextField1 = new javax.swing.JTextField();
        Aceptar_cate = new javax.swing.JButton();
        jLabel26 = new javax.swing.JLabel();
        jLabel10 = new javax.swing.JLabel();
        jLabel27 = new javax.swing.JLabel();
        Ti_pago = new javax.swing.JFrame();
        jLabel31 = new javax.swing.JLabel();
        jPanel15 = new javax.swing.JPanel();
        nombreEmi1 = new javax.swing.JTextField();
        jLabel34 = new javax.swing.JLabel();
        jLabel35 = new javax.swing.JLabel();
        tipoEmi1 = new javax.swing.JComboBox();
        jButton7 = new javax.swing.JButton();
        jButton9 = new javax.swing.JButton();
        Fondo2 = new javax.swing.JLabel();
        jLabel18 = new javax.swing.JLabel();
        jPanel9 = new javax.swing.JPanel();
        Folio = new javax.swing.JLabel();
        Folio1 = new javax.swing.JLabel();
        estado = new javax.swing.JTextField();
        jPanel10 = new javax.swing.JPanel();
        Folio3 = new javax.swing.JLabel();
        Folio4 = new javax.swing.JLabel();
        mesFactura = new javax.swing.JTextField();
        diaFactura = new javax.swing.JTextField();
        añoFactura = new javax.swing.JTextField();
        Folio5 = new javax.swing.JLabel();
        Folio6 = new javax.swing.JLabel();
        Folio7 = new javax.swing.JLabel();
        nombre = new javax.swing.JTextField();
        jPanel11 = new javax.swing.JPanel();
        Folio8 = new javax.swing.JLabel();
        Folio9 = new javax.swing.JLabel();
        mesCompra = new javax.swing.JTextField();
        diaCompra = new javax.swing.JTextField();
        añoCompra = new javax.swing.JTextField();
        Folio10 = new javax.swing.JLabel();
        Folio11 = new javax.swing.JLabel();
        Folio12 = new javax.swing.JLabel();
        folio = new javax.swing.JTextField();
        Etiqueta = new javax.swing.JLabel();
        facturado = new javax.swing.JComboBox();
        Folio2 = new javax.swing.JLabel();
        totalFA = new javax.swing.JTextField();
        Folio14 = new javax.swing.JLabel();
        iva = new javax.swing.JCheckBox();
        Total1 = new javax.swing.JLabel();
        ivaTotal = new javax.swing.JTextField();
        tipo = new javax.swing.JComboBox();
        Folio13 = new javax.swing.JLabel();
        rfc = new javax.swing.JTextField();
        Folio15 = new javax.swing.JLabel();
        subTotal1 = new javax.swing.JTextField();
        comFis2 = new javax.swing.JCheckBox();
        jButton4 = new javax.swing.JButton();
        jButton5 = new javax.swing.JButton();
        tipo1 = new javax.swing.JComboBox();
        Folio16 = new javax.swing.JLabel();
        tipo2 = new javax.swing.JComboBox();
        Folio17 = new javax.swing.JLabel();
        nombre1 = new javax.swing.JTextField();
        Folio18 = new javax.swing.JLabel();
        jButton2 = new javax.swing.JButton();
        Fondo = new javax.swing.JLabel();

        monedas.setDefaultCloseOperation(javax.swing.WindowConstants.DISPOSE_ON_CLOSE);
        monedas.setMinimumSize(new java.awt.Dimension(500, 720));
        monedas.setResizable(false);
        monedas.setSize(new java.awt.Dimension(500, 720));
        monedas.addWindowListener(new java.awt.event.WindowAdapter() {
            public void windowClosed(java.awt.event.WindowEvent evt) {
                monedasWindowClosed(evt);
            }
            public void windowClosing(java.awt.event.WindowEvent evt) {
                monedasWindowClosing(evt);
            }
            public void windowOpened(java.awt.event.WindowEvent evt) {
                monedasWindowOpened(evt);
            }
        });
        monedas.getContentPane().setLayout(null);

        jPanel4.setBorder(javax.swing.BorderFactory.createTitledBorder(null, "Dinero", javax.swing.border.TitledBorder.RIGHT, javax.swing.border.TitledBorder.DEFAULT_POSITION, new java.awt.Font("Tahoma", 0, 18), new java.awt.Color(255, 255, 255))); // NOI18N
        jPanel4.setForeground(new java.awt.Color(255, 255, 255));
        jPanel4.setOpaque(false);
        jPanel4.setLayout(new org.netbeans.lib.awtextra.AbsoluteLayout());

        bi1000i.setHorizontalAlignment(javax.swing.JTextField.RIGHT);
        bi1000i.setText("0");
        bi1000i.addFocusListener(new java.awt.event.FocusAdapter() {
            public void focusGained(java.awt.event.FocusEvent evt) {
                bi1000iFocusGained(evt);
            }
            public void focusLost(java.awt.event.FocusEvent evt) {
                calcuTotal(evt);
            }
        });
        bi1000i.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                bi1000iActionPerformed(evt);
            }
        });
        bi1000i.addKeyListener(new java.awt.event.KeyAdapter() {
            public void keyPressed(java.awt.event.KeyEvent evt) {
                bi1000iKeyPressed(evt);
            }
            public void keyTyped(java.awt.event.KeyEvent evt) {
                bi200iKeyTyped(evt);
            }
        });
        jPanel4.add(bi1000i, new org.netbeans.lib.awtextra.AbsoluteConstraints(120, 40, 120, -1));

        bi500i.setHorizontalAlignment(javax.swing.JTextField.RIGHT);
        bi500i.setText("0");
        bi500i.addFocusListener(new java.awt.event.FocusAdapter() {
            public void focusGained(java.awt.event.FocusEvent evt) {
                bi500iFocusGained(evt);
            }
            public void focusLost(java.awt.event.FocusEvent evt) {
                calcuTotal(evt);
            }
        });
        bi500i.addKeyListener(new java.awt.event.KeyAdapter() {
            public void keyPressed(java.awt.event.KeyEvent evt) {
                bi500iKeyPressed(evt);
            }
            public void keyTyped(java.awt.event.KeyEvent evt) {
                bi500iKeyTyped(evt);
            }
        });
        jPanel4.add(bi500i, new org.netbeans.lib.awtextra.AbsoluteConstraints(120, 80, 120, -1));

        bi200i.setHorizontalAlignment(javax.swing.JTextField.RIGHT);
        bi200i.setText("0");
        bi200i.addFocusListener(new java.awt.event.FocusAdapter() {
            public void focusGained(java.awt.event.FocusEvent evt) {
                bi200iFocusGained(evt);
            }
            public void focusLost(java.awt.event.FocusEvent evt) {
                calcuTotal(evt);
            }
        });
        bi200i.addKeyListener(new java.awt.event.KeyAdapter() {
            public void keyPressed(java.awt.event.KeyEvent evt) {
                bi200iKeyPressed(evt);
            }
            public void keyTyped(java.awt.event.KeyEvent evt) {
                bi200iKeyTyped(evt);
                nuevo(evt);
            }
        });
        jPanel4.add(bi200i, new org.netbeans.lib.awtextra.AbsoluteConstraints(120, 120, 120, -1));

        bi100i.setHorizontalAlignment(javax.swing.JTextField.RIGHT);
        bi100i.setText("0");
        bi100i.addFocusListener(new java.awt.event.FocusAdapter() {
            public void focusGained(java.awt.event.FocusEvent evt) {
                bi100iFocusGained(evt);
            }
            public void focusLost(java.awt.event.FocusEvent evt) {
                calcuTotal(evt);
            }
        });
        bi100i.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                bi100iActionPerformed(evt);
            }
        });
        bi100i.addKeyListener(new java.awt.event.KeyAdapter() {
            public void keyPressed(java.awt.event.KeyEvent evt) {
                bi100iKeyPressed(evt);
            }
            public void keyTyped(java.awt.event.KeyEvent evt) {
                bi200iKeyTyped(evt);
            }
        });
        jPanel4.add(bi100i, new org.netbeans.lib.awtextra.AbsoluteConstraints(120, 160, 120, -1));

        bi50i.setHorizontalAlignment(javax.swing.JTextField.RIGHT);
        bi50i.setText("0");
        bi50i.addFocusListener(new java.awt.event.FocusAdapter() {
            public void focusGained(java.awt.event.FocusEvent evt) {
                bi50iFocusGained(evt);
            }
            public void focusLost(java.awt.event.FocusEvent evt) {
                calcuTotal(evt);
            }
        });
        bi50i.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                bi50iActionPerformed(evt);
            }
        });
        bi50i.addKeyListener(new java.awt.event.KeyAdapter() {
            public void keyPressed(java.awt.event.KeyEvent evt) {
                bi50iKeyPressed(evt);
            }
            public void keyTyped(java.awt.event.KeyEvent evt) {
                bi200iKeyTyped(evt);
            }
        });
        jPanel4.add(bi50i, new org.netbeans.lib.awtextra.AbsoluteConstraints(120, 200, 120, -1));

        bi20i.setHorizontalAlignment(javax.swing.JTextField.RIGHT);
        bi20i.setText("0");
        bi20i.addFocusListener(new java.awt.event.FocusAdapter() {
            public void focusGained(java.awt.event.FocusEvent evt) {
                bi20iFocusGained(evt);
            }
            public void focusLost(java.awt.event.FocusEvent evt) {
                calcuTotal(evt);
            }
        });
        bi20i.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                bi20iActionPerformed(evt);
            }
        });
        bi20i.addKeyListener(new java.awt.event.KeyAdapter() {
            public void keyPressed(java.awt.event.KeyEvent evt) {
                bi20iKeyPressed(evt);
            }
            public void keyTyped(java.awt.event.KeyEvent evt) {
                bi200iKeyTyped(evt);
            }
        });
        jPanel4.add(bi20i, new org.netbeans.lib.awtextra.AbsoluteConstraints(120, 240, 120, -1));

        mo10i.setHorizontalAlignment(javax.swing.JTextField.RIGHT);
        mo10i.setText("0");
        mo10i.addFocusListener(new java.awt.event.FocusAdapter() {
            public void focusGained(java.awt.event.FocusEvent evt) {
                mo10iFocusGained(evt);
            }
            public void focusLost(java.awt.event.FocusEvent evt) {
                calcuTotal(evt);
            }
        });
        mo10i.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                mo10iActionPerformed(evt);
            }
        });
        mo10i.addKeyListener(new java.awt.event.KeyAdapter() {
            public void keyPressed(java.awt.event.KeyEvent evt) {
                mo10iKeyPressed(evt);
            }
            public void keyTyped(java.awt.event.KeyEvent evt) {
                bi200iKeyTyped(evt);
            }
        });
        jPanel4.add(mo10i, new org.netbeans.lib.awtextra.AbsoluteConstraints(120, 280, 120, -1));

        mo5i.setHorizontalAlignment(javax.swing.JTextField.RIGHT);
        mo5i.setText("0");
        mo5i.addFocusListener(new java.awt.event.FocusAdapter() {
            public void focusGained(java.awt.event.FocusEvent evt) {
                mo5iFocusGained(evt);
            }
            public void focusLost(java.awt.event.FocusEvent evt) {
                calcuTotal(evt);
            }
        });
        mo5i.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                mo5iActionPerformed(evt);
            }
        });
        mo5i.addKeyListener(new java.awt.event.KeyAdapter() {
            public void keyPressed(java.awt.event.KeyEvent evt) {
                mo5iKeyPressed(evt);
            }
            public void keyTyped(java.awt.event.KeyEvent evt) {
                bi200iKeyTyped(evt);
            }
        });
        jPanel4.add(mo5i, new org.netbeans.lib.awtextra.AbsoluteConstraints(120, 320, 120, -1));

        mo2i.setHorizontalAlignment(javax.swing.JTextField.RIGHT);
        mo2i.setText("0");
        mo2i.addFocusListener(new java.awt.event.FocusAdapter() {
            public void focusGained(java.awt.event.FocusEvent evt) {
                mo2iFocusGained(evt);
            }
            public void focusLost(java.awt.event.FocusEvent evt) {
                calcuTotal(evt);
            }
        });
        mo2i.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                mo2iActionPerformed(evt);
            }
        });
        mo2i.addKeyListener(new java.awt.event.KeyAdapter() {
            public void keyPressed(java.awt.event.KeyEvent evt) {
                mo2iKeyPressed(evt);
            }
            public void keyTyped(java.awt.event.KeyEvent evt) {
                bi200iKeyTyped(evt);
            }
        });
        jPanel4.add(mo2i, new org.netbeans.lib.awtextra.AbsoluteConstraints(120, 360, 120, -1));

        mo1i.setHorizontalAlignment(javax.swing.JTextField.RIGHT);
        mo1i.setText("0");
        mo1i.addFocusListener(new java.awt.event.FocusAdapter() {
            public void focusGained(java.awt.event.FocusEvent evt) {
                mo1iFocusGained(evt);
            }
            public void focusLost(java.awt.event.FocusEvent evt) {
                calcuTotal(evt);
            }
        });
        mo1i.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                mo1iActionPerformed(evt);
            }
        });
        mo1i.addKeyListener(new java.awt.event.KeyAdapter() {
            public void keyPressed(java.awt.event.KeyEvent evt) {
                mo1iKeyPressed(evt);
            }
            public void keyTyped(java.awt.event.KeyEvent evt) {
                bi200iKeyTyped(evt);
            }
        });
        jPanel4.add(mo1i, new org.netbeans.lib.awtextra.AbsoluteConstraints(120, 400, 120, -1));

        mo05i.setHorizontalAlignment(javax.swing.JTextField.RIGHT);
        mo05i.setText("0");
        mo05i.addFocusListener(new java.awt.event.FocusAdapter() {
            public void focusGained(java.awt.event.FocusEvent evt) {
                mo05iFocusGained(evt);
            }
            public void focusLost(java.awt.event.FocusEvent evt) {
                calcuTotal(evt);
            }
        });
        mo05i.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                mo05iActionPerformed(evt);
            }
        });
        mo05i.addKeyListener(new java.awt.event.KeyAdapter() {
            public void keyPressed(java.awt.event.KeyEvent evt) {
                mo05iKeyPressed(evt);
            }
            public void keyTyped(java.awt.event.KeyEvent evt) {
                bi200iKeyTyped(evt);
            }
        });
        jPanel4.add(mo05i, new org.netbeans.lib.awtextra.AbsoluteConstraints(120, 440, 120, -1));

        jLabel6.setFont(new java.awt.Font("Tahoma", 0, 14)); // NOI18N
        jLabel6.setForeground(new java.awt.Color(255, 255, 255));
        jLabel6.setText("$ 500");
        jPanel4.add(jLabel6, new org.netbeans.lib.awtextra.AbsoluteConstraints(30, 80, -1, -1));

        jLabel7.setFont(new java.awt.Font("Tahoma", 0, 14)); // NOI18N
        jLabel7.setForeground(new java.awt.Color(255, 255, 255));
        jLabel7.setText("$ 200");
        jPanel4.add(jLabel7, new org.netbeans.lib.awtextra.AbsoluteConstraints(30, 120, -1, -1));

        jLabel8.setFont(new java.awt.Font("Tahoma", 0, 14)); // NOI18N
        jLabel8.setForeground(new java.awt.Color(255, 255, 255));
        jLabel8.setText("$ 50");
        jPanel4.add(jLabel8, new org.netbeans.lib.awtextra.AbsoluteConstraints(30, 200, -1, -1));

        jLabel9.setFont(new java.awt.Font("Tahoma", 0, 14)); // NOI18N
        jLabel9.setForeground(new java.awt.Color(255, 255, 255));
        jLabel9.setText("$ 2");
        jPanel4.add(jLabel9, new org.netbeans.lib.awtextra.AbsoluteConstraints(30, 360, -1, -1));

        jLabel11.setFont(new java.awt.Font("Tahoma", 0, 14)); // NOI18N
        jLabel11.setForeground(new java.awt.Color(255, 255, 255));
        jLabel11.setText("$ 100");
        jPanel4.add(jLabel11, new org.netbeans.lib.awtextra.AbsoluteConstraints(30, 160, -1, -1));

        jLabel12.setFont(new java.awt.Font("Tahoma", 0, 14)); // NOI18N
        jLabel12.setForeground(new java.awt.Color(255, 255, 255));
        jLabel12.setText("$ 20");
        jPanel4.add(jLabel12, new org.netbeans.lib.awtextra.AbsoluteConstraints(30, 240, -1, -1));

        jLabel13.setFont(new java.awt.Font("Tahoma", 0, 14)); // NOI18N
        jLabel13.setForeground(new java.awt.Color(255, 255, 255));
        jLabel13.setText("$ 5");
        jPanel4.add(jLabel13, new org.netbeans.lib.awtextra.AbsoluteConstraints(30, 320, -1, -1));

        jLabel14.setFont(new java.awt.Font("Tahoma", 0, 14)); // NOI18N
        jLabel14.setForeground(new java.awt.Color(255, 255, 255));
        jLabel14.setText("$ 10");
        jPanel4.add(jLabel14, new org.netbeans.lib.awtextra.AbsoluteConstraints(30, 280, -1, -1));

        jLabel15.setFont(new java.awt.Font("Tahoma", 0, 14)); // NOI18N
        jLabel15.setForeground(new java.awt.Color(255, 255, 255));
        jLabel15.setText("$ 1");
        jPanel4.add(jLabel15, new org.netbeans.lib.awtextra.AbsoluteConstraints(30, 400, -1, -1));

        jLabel28.setFont(new java.awt.Font("Tahoma", 0, 14)); // NOI18N
        jLabel28.setForeground(new java.awt.Color(255, 255, 255));
        jLabel28.setText("$ 1000");
        jPanel4.add(jLabel28, new org.netbeans.lib.awtextra.AbsoluteConstraints(30, 40, -1, -1));

        jLabel30.setFont(new java.awt.Font("Tahoma", 0, 14)); // NOI18N
        jLabel30.setForeground(new java.awt.Color(255, 255, 255));
        jLabel30.setText("$ 0.5");
        jPanel4.add(jLabel30, new org.netbeans.lib.awtextra.AbsoluteConstraints(30, 440, -1, -1));

        monedas.getContentPane().add(jPanel4);
        jPanel4.setBounds(40, 40, 260, 500);

        jPanel5.setBorder(javax.swing.BorderFactory.createTitledBorder(null, "Dinero", javax.swing.border.TitledBorder.RIGHT, javax.swing.border.TitledBorder.DEFAULT_POSITION, new java.awt.Font("Tahoma", 0, 18), new java.awt.Color(255, 255, 255))); // NOI18N
        jPanel5.setForeground(new java.awt.Color(255, 255, 255));
        jPanel5.setOpaque(false);
        jPanel5.setLayout(new org.netbeans.lib.awtextra.AbsoluteLayout());

        d500.setFont(new java.awt.Font("Tahoma", 0, 14)); // NOI18N
        d500.setForeground(new java.awt.Color(255, 255, 255));
        d500.setText("$0");
        jPanel5.add(d500, new org.netbeans.lib.awtextra.AbsoluteConstraints(20, 80, -1, -1));

        d200.setFont(new java.awt.Font("Tahoma", 0, 14)); // NOI18N
        d200.setForeground(new java.awt.Color(255, 255, 255));
        d200.setText("$0");
        jPanel5.add(d200, new org.netbeans.lib.awtextra.AbsoluteConstraints(20, 120, -1, -1));

        d50.setFont(new java.awt.Font("Tahoma", 0, 14)); // NOI18N
        d50.setForeground(new java.awt.Color(255, 255, 255));
        d50.setText("$0");
        jPanel5.add(d50, new org.netbeans.lib.awtextra.AbsoluteConstraints(20, 200, -1, -1));

        d2.setFont(new java.awt.Font("Tahoma", 0, 14)); // NOI18N
        d2.setForeground(new java.awt.Color(255, 255, 255));
        d2.setText("$0");
        jPanel5.add(d2, new org.netbeans.lib.awtextra.AbsoluteConstraints(20, 360, -1, -1));

        d1000.setFont(new java.awt.Font("Tahoma", 0, 14)); // NOI18N
        d1000.setForeground(new java.awt.Color(255, 255, 255));
        d1000.setText("$0");
        jPanel5.add(d1000, new org.netbeans.lib.awtextra.AbsoluteConstraints(20, 40, -1, -1));

        d100.setFont(new java.awt.Font("Tahoma", 0, 14)); // NOI18N
        d100.setForeground(new java.awt.Color(255, 255, 255));
        d100.setText("$0");
        jPanel5.add(d100, new org.netbeans.lib.awtextra.AbsoluteConstraints(20, 160, -1, -1));

        d20.setFont(new java.awt.Font("Tahoma", 0, 14)); // NOI18N
        d20.setForeground(new java.awt.Color(255, 255, 255));
        d20.setText("$0");
        jPanel5.add(d20, new org.netbeans.lib.awtextra.AbsoluteConstraints(20, 240, -1, -1));

        d5.setFont(new java.awt.Font("Tahoma", 0, 14)); // NOI18N
        d5.setForeground(new java.awt.Color(255, 255, 255));
        d5.setText("$0");
        jPanel5.add(d5, new org.netbeans.lib.awtextra.AbsoluteConstraints(20, 320, -1, -1));

        d10.setFont(new java.awt.Font("Tahoma", 0, 14)); // NOI18N
        d10.setForeground(new java.awt.Color(255, 255, 255));
        d10.setText("$0");
        jPanel5.add(d10, new org.netbeans.lib.awtextra.AbsoluteConstraints(20, 280, -1, -1));

        d1.setFont(new java.awt.Font("Tahoma", 0, 14)); // NOI18N
        d1.setForeground(new java.awt.Color(255, 255, 255));
        d1.setText("$0");
        jPanel5.add(d1, new org.netbeans.lib.awtextra.AbsoluteConstraints(20, 400, -1, -1));

        d05.setFont(new java.awt.Font("Tahoma", 0, 14)); // NOI18N
        d05.setForeground(new java.awt.Color(255, 255, 255));
        d05.setText("$0");
        jPanel5.add(d05, new org.netbeans.lib.awtextra.AbsoluteConstraints(20, 440, -1, -1));

        monedas.getContentPane().add(jPanel5);
        jPanel5.setBounds(340, 40, 110, 500);

        jPanel6.setBorder(javax.swing.BorderFactory.createTitledBorder(null, "Total", javax.swing.border.TitledBorder.RIGHT, javax.swing.border.TitledBorder.DEFAULT_POSITION, new java.awt.Font("Tahoma", 0, 18), new java.awt.Color(255, 255, 255))); // NOI18N
        jPanel6.setForeground(new java.awt.Color(255, 255, 255));
        jPanel6.setOpaque(false);
        jPanel6.setLayout(new org.netbeans.lib.awtextra.AbsoluteLayout());

        totalCam.setFont(new java.awt.Font("Tahoma", 1, 18)); // NOI18N
        totalCam.setForeground(new java.awt.Color(255, 255, 255));
        totalCam.setText("$ 0.0");
        jPanel6.add(totalCam, new org.netbeans.lib.awtextra.AbsoluteConstraints(10, 30, -1, -1));

        monedas.getContentPane().add(jPanel6);
        jPanel6.setBounds(340, 550, 110, 70);

        jPanel7.setBorder(javax.swing.BorderFactory.createTitledBorder(null, "Diferencia de corte", javax.swing.border.TitledBorder.LEFT, javax.swing.border.TitledBorder.DEFAULT_POSITION, new java.awt.Font("Tahoma", 0, 14), new java.awt.Color(255, 255, 255))); // NOI18N
        jPanel7.setForeground(new java.awt.Color(255, 255, 255));
        jPanel7.setOpaque(false);
        jPanel7.setLayout(new org.netbeans.lib.awtextra.AbsoluteLayout());

        moDife.setBackground(new java.awt.Color(204, 204, 204));
        moDife.setFont(new java.awt.Font("Tahoma", 1, 18)); // NOI18N
        moDife.setHorizontalAlignment(javax.swing.SwingConstants.CENTER);
        moDife.setText("$ 0.0");
        moDife.setOpaque(true);
        jPanel7.add(moDife, new org.netbeans.lib.awtextra.AbsoluteConstraints(140, 40, 140, -1));

        fechaAn.setFont(new java.awt.Font("Tahoma", 0, 14)); // NOI18N
        fechaAn.setForeground(new java.awt.Color(255, 255, 255));
        fechaAn.setText("Fecha");
        jPanel7.add(fechaAn, new org.netbeans.lib.awtextra.AbsoluteConstraints(20, 20, 260, -1));

        monAn.setFont(new java.awt.Font("Tahoma", 0, 14)); // NOI18N
        monAn.setForeground(new java.awt.Color(255, 255, 255));
        monAn.setText("$0.0");
        jPanel7.add(monAn, new org.netbeans.lib.awtextra.AbsoluteConstraints(20, 40, 150, -1));

        monedas.getContentPane().add(jPanel7);
        jPanel7.setBounds(40, 550, 290, 80);

        fechaMo.setFont(new java.awt.Font("Tahoma", 0, 14)); // NOI18N
        fechaMo.setForeground(new java.awt.Color(255, 255, 255));
        fechaMo.setText("Fecha");
        monedas.getContentPane().add(fechaMo);
        fechaMo.setBounds(40, 20, 250, 17);

        maquinaMo.setFont(new java.awt.Font("Tahoma", 0, 14)); // NOI18N
        maquinaMo.setForeground(new java.awt.Color(255, 255, 255));
        maquinaMo.setText("Maquina");
        monedas.getContentPane().add(maquinaMo);
        maquinaMo.setBounds(340, 20, 110, 17);

        moComen.setForeground(new java.awt.Color(102, 102, 102));
        moComen.setText("Comentario");
        moComen.addFocusListener(new java.awt.event.FocusAdapter() {
            public void focusGained(java.awt.event.FocusEvent evt) {
                moComenFocusGained(evt);
            }
        });
        moComen.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                moComenActionPerformed(evt);
            }
        });
        moComen.addKeyListener(new java.awt.event.KeyAdapter() {
            public void keyTyped(java.awt.event.KeyEvent evt) {
                moComenKeyTyped(evt);
            }
        });
        monedas.getContentPane().add(moComen);
        moComen.setBounds(50, 640, 270, 30);

        jButton1.setText("Guardar");
        jButton1.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jButton1ActionPerformed(evt);
            }
        });
        monedas.getContentPane().add(jButton1);
        jButton1.setBounds(340, 630, 110, 40);

        jLabel5.setIcon(new javax.swing.ImageIcon(getClass().getResource("/com/dent/imag/red-simple-wallpapers-1920x1080.jpg"))); // NOI18N
        jLabel5.setMaximumSize(new java.awt.Dimension(500, 560));
        jLabel5.setMinimumSize(new java.awt.Dimension(500, 560));
        jLabel5.setRequestFocusEnabled(false);
        monedas.getContentPane().add(jLabel5);
        jLabel5.setBounds(0, 0, 551, 1080);

        Emisor.setMinimumSize(new java.awt.Dimension(536, 420));
        Emisor.setSize(new java.awt.Dimension(536, 420));
        Emisor.addWindowListener(new java.awt.event.WindowAdapter() {
            public void windowClosing(java.awt.event.WindowEvent evt) {
                EmisorWindowClosing(evt);
            }
            public void windowOpened(java.awt.event.WindowEvent evt) {
                EmisorWindowOpened(evt);
            }
        });
        Emisor.getContentPane().setLayout(new org.netbeans.lib.awtextra.AbsoluteLayout());

        jLabel19.setBackground(new java.awt.Color(51, 102, 255));
        jLabel19.setFont(new java.awt.Font("Tahoma", 0, 24)); // NOI18N
        jLabel19.setForeground(new java.awt.Color(255, 255, 255));
        jLabel19.setHorizontalAlignment(javax.swing.SwingConstants.CENTER);
        jLabel19.setText("Insertar Emisor de Factura");
        jLabel19.setOpaque(true);
        Emisor.getContentPane().add(jLabel19, new org.netbeans.lib.awtextra.AbsoluteConstraints(0, 0, 520, 50));

        jPanel12.setBorder(javax.swing.BorderFactory.createTitledBorder(null, "Emisor", javax.swing.border.TitledBorder.DEFAULT_JUSTIFICATION, javax.swing.border.TitledBorder.DEFAULT_POSITION, new java.awt.Font("Tahoma", 0, 14), new java.awt.Color(255, 255, 255))); // NOI18N
        jPanel12.setOpaque(false);
        jPanel12.setLayout(new org.netbeans.lib.awtextra.AbsoluteLayout());

        jLabel20.setFont(new java.awt.Font("Tahoma", 0, 14)); // NOI18N
        jLabel20.setForeground(new java.awt.Color(255, 255, 255));
        jLabel20.setText("Rfc");
        jPanel12.add(jLabel20, new org.netbeans.lib.awtextra.AbsoluteConstraints(40, 80, -1, -1));
        jPanel12.add(rfcEmi, new org.netbeans.lib.awtextra.AbsoluteConstraints(110, 80, 180, -1));

        jLabel21.setFont(new java.awt.Font("Tahoma", 0, 14)); // NOI18N
        jLabel21.setForeground(new java.awt.Color(255, 255, 255));
        jLabel21.setText("Direccion");
        jPanel12.add(jLabel21, new org.netbeans.lib.awtextra.AbsoluteConstraints(40, 120, -1, -1));
        jPanel12.add(direccionEmi, new org.netbeans.lib.awtextra.AbsoluteConstraints(110, 120, 290, -1));

        nombreEmi.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                nombreEmiActionPerformed(evt);
            }
        });
        jPanel12.add(nombreEmi, new org.netbeans.lib.awtextra.AbsoluteConstraints(110, 40, 290, -1));

        jLabel23.setFont(new java.awt.Font("Tahoma", 0, 14)); // NOI18N
        jLabel23.setForeground(new java.awt.Color(255, 255, 255));
        jLabel23.setText("Tipo");
        jPanel12.add(jLabel23, new org.netbeans.lib.awtextra.AbsoluteConstraints(40, 160, -1, -1));

        jLabel24.setFont(new java.awt.Font("Tahoma", 0, 14)); // NOI18N
        jLabel24.setForeground(new java.awt.Color(255, 255, 255));
        jLabel24.setText("Nombre");
        jPanel12.add(jLabel24, new org.netbeans.lib.awtextra.AbsoluteConstraints(40, 40, -1, -1));

        tipoEmi.setModel(new javax.swing.DefaultComboBoxModel(new String[] { "Proveedor", "Gastos de administración", "Gastos de venta", "Sueldos empleados" }));
        tipoEmi.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                tipoEmiActionPerformed(evt);
            }
        });
        jPanel12.add(tipoEmi, new org.netbeans.lib.awtextra.AbsoluteConstraints(110, 160, 180, 20));

        Emisor.getContentPane().add(jPanel12, new org.netbeans.lib.awtextra.AbsoluteConstraints(30, 80, 450, 210));

        jButton6.setText("Cancelar");
        jButton6.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jButton6ActionPerformed(evt);
            }
        });
        Emisor.getContentPane().add(jButton6, new org.netbeans.lib.awtextra.AbsoluteConstraints(340, 310, 140, 30));

        jButton3.setText("Guardar");
        jButton3.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jButton3ActionPerformed(evt);
            }
        });
        Emisor.getContentPane().add(jButton3, new org.netbeans.lib.awtextra.AbsoluteConstraints(160, 310, 140, 30));

        jLabel22.setFont(new java.awt.Font("Tahoma", 0, 14)); // NOI18N
        jLabel22.setForeground(new java.awt.Color(255, 255, 255));
        jLabel22.setText("No se encontro el rfc del emisor. Porfavor agrege un nuevo emisor");
        Emisor.getContentPane().add(jLabel22, new org.netbeans.lib.awtextra.AbsoluteConstraints(50, 60, -1, -1));

        Fondo1.setIcon(new javax.swing.ImageIcon(getClass().getResource("/com/dent/imag/red-simple-wallpapers-1920x1080.jpg"))); // NOI18N
        Fondo1.setText("cx");
        Fondo1.setMaximumSize(new java.awt.Dimension(500, 560));
        Fondo1.setMinimumSize(new java.awt.Dimension(500, 560));
        Fondo1.setRequestFocusEnabled(false);
        Emisor.getContentPane().add(Fondo1, new org.netbeans.lib.awtextra.AbsoluteConstraints(-1370, 0, -1, -1));

        jFrame1.setDefaultCloseOperation(javax.swing.WindowConstants.DISPOSE_ON_CLOSE);
        jFrame1.setResizable(false);
        jFrame1.getContentPane().setLayout(new org.netbeans.lib.awtextra.AbsoluteLayout());

        jPanel14.setLayout(null);

        jButton8.setText("Cancelar");
        jButton8.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jButton8ActionPerformed(evt);
            }
        });
        jPanel14.add(jButton8);
        jButton8.setBounds(250, 140, 80, 30);

        jComboBox1.setModel(new javax.swing.DefaultComboBoxModel<>(new String[] { "Item 1", "Item 2", "Item 3", "Item 4" }));
        jPanel14.add(jComboBox1);
        jComboBox1.setBounds(100, 100, 110, 20);

        jTextField1.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jTextField1ActionPerformed(evt);
            }
        });
        jPanel14.add(jTextField1);
        jTextField1.setBounds(100, 60, 230, 20);

        Aceptar_cate.setText("Aceptar");
        Aceptar_cate.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                Aceptar_cateActionPerformed(evt);
            }
        });
        jPanel14.add(Aceptar_cate);
        Aceptar_cate.setBounds(160, 140, 80, 30);

        jLabel26.setFont(new java.awt.Font("Tahoma", 0, 12)); // NOI18N
        jLabel26.setText("Categoria");
        jPanel14.add(jLabel26);
        jLabel26.setBounds(20, 60, 60, 15);

        jLabel10.setFont(new java.awt.Font("Tahoma", 0, 12)); // NOI18N
        jLabel10.setText("Tipo");
        jPanel14.add(jLabel10);
        jLabel10.setBounds(20, 100, 50, 15);

        jLabel27.setFont(new java.awt.Font("Tahoma", 0, 18)); // NOI18N
        jLabel27.setText("Añadir categoria: ");
        jPanel14.add(jLabel27);
        jLabel27.setBounds(20, 10, 140, 22);

        jFrame1.getContentPane().add(jPanel14, new org.netbeans.lib.awtextra.AbsoluteConstraints(0, 10, 360, 180));

        Ti_pago.setMinimumSize(new java.awt.Dimension(530, 350));
        Ti_pago.setSize(new java.awt.Dimension(530, 350));
        Ti_pago.addWindowListener(new java.awt.event.WindowAdapter() {
            public void windowClosing(java.awt.event.WindowEvent evt) {
                Ti_pagoWindowClosing(evt);
            }
            public void windowOpened(java.awt.event.WindowEvent evt) {
                Ti_pagoWindowOpened(evt);
            }
        });
        Ti_pago.getContentPane().setLayout(new org.netbeans.lib.awtextra.AbsoluteLayout());

        jLabel31.setBackground(new java.awt.Color(51, 102, 255));
        jLabel31.setFont(new java.awt.Font("Tahoma", 0, 24)); // NOI18N
        jLabel31.setForeground(new java.awt.Color(255, 255, 255));
        jLabel31.setHorizontalAlignment(javax.swing.SwingConstants.CENTER);
        jLabel31.setText("Tipo de pago");
        jLabel31.setOpaque(true);
        Ti_pago.getContentPane().add(jLabel31, new org.netbeans.lib.awtextra.AbsoluteConstraints(0, 0, 520, 50));

        jPanel15.setBorder(javax.swing.BorderFactory.createTitledBorder(null, "Tipo de Pago", javax.swing.border.TitledBorder.DEFAULT_JUSTIFICATION, javax.swing.border.TitledBorder.DEFAULT_POSITION, new java.awt.Font("Tahoma", 0, 14), new java.awt.Color(255, 255, 255))); // NOI18N
        jPanel15.setOpaque(false);
        jPanel15.setLayout(new org.netbeans.lib.awtextra.AbsoluteLayout());

        nombreEmi1.setEnabled(false);
        nombreEmi1.addFocusListener(new java.awt.event.FocusAdapter() {
            public void focusGained(java.awt.event.FocusEvent evt) {
                nombreEmi1FocusGained(evt);
            }
            public void focusLost(java.awt.event.FocusEvent evt) {
                nombreEmi1FocusLost(evt);
            }
        });
        nombreEmi1.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                nombreEmi1ActionPerformed(evt);
            }
        });
        nombreEmi1.addKeyListener(new java.awt.event.KeyAdapter() {
            public void keyReleased(java.awt.event.KeyEvent evt) {
                nombreEmi1KeyReleased(evt);
            }
            public void keyTyped(java.awt.event.KeyEvent evt) {
                nombreEmi1KeyTyped(evt);
            }
        });
        jPanel15.add(nombreEmi1, new org.netbeans.lib.awtextra.AbsoluteConstraints(240, 90, 160, -1));

        jLabel34.setFont(new java.awt.Font("Tahoma", 0, 14)); // NOI18N
        jLabel34.setForeground(new java.awt.Color(255, 255, 255));
        jLabel34.setText("Tipo");
        jPanel15.add(jLabel34, new org.netbeans.lib.awtextra.AbsoluteConstraints(40, 40, -1, 30));

        jLabel35.setFont(new java.awt.Font("Tahoma", 0, 14)); // NOI18N
        jLabel35.setForeground(new java.awt.Color(255, 255, 255));
        jLabel35.setText("Ultimos 4 digitos");
        jPanel15.add(jLabel35, new org.netbeans.lib.awtextra.AbsoluteConstraints(40, 90, -1, -1));

        tipoEmi1.setModel(new javax.swing.DefaultComboBoxModel(new String[] { "Proveedor", "Gastos de administración", "Gastos de venta", "Sueldos empleados" }));
        tipoEmi1.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                tipoEmi1ActionPerformed(evt);
            }
        });
        jPanel15.add(tipoEmi1, new org.netbeans.lib.awtextra.AbsoluteConstraints(110, 40, 290, 30));

        Ti_pago.getContentPane().add(jPanel15, new org.netbeans.lib.awtextra.AbsoluteConstraints(30, 80, 450, 140));

        jButton7.setText("Cancelar");
        jButton7.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jButton7ActionPerformed(evt);
            }
        });
        Ti_pago.getContentPane().add(jButton7, new org.netbeans.lib.awtextra.AbsoluteConstraints(340, 250, 140, 30));

        jButton9.setText("Guardar");
        jButton9.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jButton9ActionPerformed(evt);
            }
        });
        Ti_pago.getContentPane().add(jButton9, new org.netbeans.lib.awtextra.AbsoluteConstraints(160, 250, 140, 30));

        Fondo2.setIcon(new javax.swing.ImageIcon(getClass().getResource("/com/dent/imag/red-simple-wallpapers-1920x1080.jpg"))); // NOI18N
        Fondo2.setText("cx");
        Fondo2.setMaximumSize(new java.awt.Dimension(500, 560));
        Fondo2.setMinimumSize(new java.awt.Dimension(500, 560));
        Fondo2.setRequestFocusEnabled(false);
        Ti_pago.getContentPane().add(Fondo2, new org.netbeans.lib.awtextra.AbsoluteConstraints(-1370, 0, -1, -1));

        setDefaultCloseOperation(javax.swing.WindowConstants.DISPOSE_ON_CLOSE);
        setTitle("Dentitodo Inventario");
        setMaximumSize(new java.awt.Dimension(890, 600));
        setMinimumSize(new java.awt.Dimension(890, 600));
        setResizable(false);
        addWindowListener(new java.awt.event.WindowAdapter() {
            public void windowClosing(java.awt.event.WindowEvent evt) {
                formWindowClosing(evt);
            }
            public void windowOpened(java.awt.event.WindowEvent evt) {
                formWindowOpened(evt);
            }
        });
        getContentPane().setLayout(null);

        jLabel18.setBackground(new java.awt.Color(51, 102, 255));
        jLabel18.setFont(new java.awt.Font("Tahoma", 0, 24)); // NOI18N
        jLabel18.setForeground(new java.awt.Color(255, 255, 255));
        jLabel18.setHorizontalAlignment(javax.swing.SwingConstants.CENTER);
        jLabel18.setText("Insertar Factura");
        jLabel18.setOpaque(true);
        getContentPane().add(jLabel18);
        jLabel18.setBounds(0, 0, 890, 50);

        jPanel9.setOpaque(false);
        jPanel9.setLayout(new org.netbeans.lib.awtextra.AbsoluteLayout());

        Folio.setFont(new java.awt.Font("Tahoma", 0, 18)); // NOI18N
        Folio.setForeground(new java.awt.Color(255, 255, 255));
        Folio.setText("Serie");
        Folio.addMouseMotionListener(new java.awt.event.MouseMotionAdapter() {
            public void mouseDragged(java.awt.event.MouseEvent evt) {
                NULL(evt);
            }
        });
        Folio.addMouseListener(new java.awt.event.MouseAdapter() {
            public void mouseClicked(java.awt.event.MouseEvent evt) {
                NULL(evt);
            }
            public void mouseEntered(java.awt.event.MouseEvent evt) {
                NULL(evt);
            }
        });
        jPanel9.add(Folio, new org.netbeans.lib.awtextra.AbsoluteConstraints(610, 30, 50, 20));

        Folio1.setFont(new java.awt.Font("Tahoma", 0, 18)); // NOI18N
        Folio1.setForeground(new java.awt.Color(255, 255, 255));
        Folio1.setText("Estado");
        jPanel9.add(Folio1, new org.netbeans.lib.awtextra.AbsoluteConstraints(30, 270, 110, 20));

        estado.setText("Factura");
        estado.setEnabled(false);
        estado.addMouseMotionListener(new java.awt.event.MouseMotionAdapter() {
            public void mouseDragged(java.awt.event.MouseEvent evt) {
                NULL(evt);
            }
        });
        estado.addMouseListener(new java.awt.event.MouseAdapter() {
            public void mouseClicked(java.awt.event.MouseEvent evt) {
                NULL(evt);
            }
            public void mouseEntered(java.awt.event.MouseEvent evt) {
                NULL(evt);
            }
        });
        estado.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                estadoActionPerformed(evt);
            }
        });
        jPanel9.add(estado, new org.netbeans.lib.awtextra.AbsoluteConstraints(140, 270, 200, -1));

        jPanel10.setBorder(javax.swing.BorderFactory.createTitledBorder(null, "Fecha de la factura", javax.swing.border.TitledBorder.RIGHT, javax.swing.border.TitledBorder.DEFAULT_POSITION, new java.awt.Font("Tahoma", 0, 14), new java.awt.Color(255, 255, 255))); // NOI18N
        jPanel10.setOpaque(false);
        jPanel10.setLayout(new org.netbeans.lib.awtextra.AbsoluteLayout());

        Folio3.setFont(new java.awt.Font("Tahoma", 0, 14)); // NOI18N
        Folio3.setForeground(new java.awt.Color(255, 255, 255));
        Folio3.setHorizontalAlignment(javax.swing.SwingConstants.CENTER);
        Folio3.setText("Año");
        Folio3.addMouseMotionListener(new java.awt.event.MouseMotionAdapter() {
            public void mouseDragged(java.awt.event.MouseEvent evt) {
                NULL(evt);
            }
        });
        Folio3.addMouseListener(new java.awt.event.MouseAdapter() {
            public void mouseClicked(java.awt.event.MouseEvent evt) {
                NULL(evt);
            }
            public void mouseEntered(java.awt.event.MouseEvent evt) {
                NULL(evt);
            }
        });
        jPanel10.add(Folio3, new org.netbeans.lib.awtextra.AbsoluteConstraints(230, 30, 70, 20));

        Folio4.setFont(new java.awt.Font("Tahoma", 0, 14)); // NOI18N
        Folio4.setForeground(new java.awt.Color(255, 255, 255));
        Folio4.setHorizontalAlignment(javax.swing.SwingConstants.CENTER);
        Folio4.setText("Mes");
        Folio4.addMouseMotionListener(new java.awt.event.MouseMotionAdapter() {
            public void mouseDragged(java.awt.event.MouseEvent evt) {
                NULL(evt);
            }
        });
        Folio4.addMouseListener(new java.awt.event.MouseAdapter() {
            public void mouseClicked(java.awt.event.MouseEvent evt) {
                NULL(evt);
            }
            public void mouseEntered(java.awt.event.MouseEvent evt) {
                NULL(evt);
            }
        });
        jPanel10.add(Folio4, new org.netbeans.lib.awtextra.AbsoluteConstraints(130, 30, 70, 20));

        mesFactura.setHorizontalAlignment(javax.swing.JTextField.CENTER);
        mesFactura.setText("1");
        mesFactura.addMouseMotionListener(new java.awt.event.MouseMotionAdapter() {
            public void mouseDragged(java.awt.event.MouseEvent evt) {
                NULL(evt);
            }
        });
        mesFactura.addMouseListener(new java.awt.event.MouseAdapter() {
            public void mouseClicked(java.awt.event.MouseEvent evt) {
                NULL(evt);
            }
            public void mouseEntered(java.awt.event.MouseEvent evt) {
                NULL(evt);
            }
        });
        mesFactura.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                mesFacturaActionPerformed(evt);
            }
        });
        jPanel10.add(mesFactura, new org.netbeans.lib.awtextra.AbsoluteConstraints(130, 60, 70, -1));

        diaFactura.setHorizontalAlignment(javax.swing.JTextField.CENTER);
        diaFactura.setText("1");
        diaFactura.addMouseMotionListener(new java.awt.event.MouseMotionAdapter() {
            public void mouseDragged(java.awt.event.MouseEvent evt) {
                NULL(evt);
            }
        });
        diaFactura.addFocusListener(new java.awt.event.FocusAdapter() {
            public void focusGained(java.awt.event.FocusEvent evt) {
                diaFacturaFocusGained(evt);
            }
            public void focusLost(java.awt.event.FocusEvent evt) {
                diaFacturaFocusLost(evt);
            }
        });
        diaFactura.addMouseListener(new java.awt.event.MouseAdapter() {
            public void mouseClicked(java.awt.event.MouseEvent evt) {
                NULL(evt);
            }
            public void mouseEntered(java.awt.event.MouseEvent evt) {
                NULL(evt);
            }
        });
        diaFactura.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                diaFacturaActionPerformed(evt);
            }
        });
        jPanel10.add(diaFactura, new org.netbeans.lib.awtextra.AbsoluteConstraints(30, 60, 70, -1));

        añoFactura.setHorizontalAlignment(javax.swing.JTextField.CENTER);
        añoFactura.setText("2007");
        añoFactura.addMouseMotionListener(new java.awt.event.MouseMotionAdapter() {
            public void mouseDragged(java.awt.event.MouseEvent evt) {
                NULL(evt);
            }
        });
        añoFactura.addMouseListener(new java.awt.event.MouseAdapter() {
            public void mouseClicked(java.awt.event.MouseEvent evt) {
                NULL(evt);
            }
            public void mouseEntered(java.awt.event.MouseEvent evt) {
                NULL(evt);
            }
        });
        añoFactura.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                añoFacturaActionPerformed(evt);
            }
        });
        jPanel10.add(añoFactura, new org.netbeans.lib.awtextra.AbsoluteConstraints(230, 60, 70, -1));

        Folio5.setFont(new java.awt.Font("Tahoma", 0, 14)); // NOI18N
        Folio5.setForeground(new java.awt.Color(255, 255, 255));
        Folio5.setHorizontalAlignment(javax.swing.SwingConstants.CENTER);
        Folio5.setText("/");
        jPanel10.add(Folio5, new org.netbeans.lib.awtextra.AbsoluteConstraints(180, 60, 70, 20));

        Folio6.setFont(new java.awt.Font("Tahoma", 0, 14)); // NOI18N
        Folio6.setForeground(new java.awt.Color(255, 255, 255));
        Folio6.setHorizontalAlignment(javax.swing.SwingConstants.CENTER);
        Folio6.setText("Dia");
        Folio6.addMouseMotionListener(new java.awt.event.MouseMotionAdapter() {
            public void mouseDragged(java.awt.event.MouseEvent evt) {
                NULL(evt);
            }
        });
        Folio6.addMouseListener(new java.awt.event.MouseAdapter() {
            public void mouseClicked(java.awt.event.MouseEvent evt) {
                NULL(evt);
            }
            public void mouseEntered(java.awt.event.MouseEvent evt) {
                NULL(evt);
            }
        });
        jPanel10.add(Folio6, new org.netbeans.lib.awtextra.AbsoluteConstraints(30, 30, 70, 20));

        Folio7.setFont(new java.awt.Font("Tahoma", 0, 14)); // NOI18N
        Folio7.setForeground(new java.awt.Color(255, 255, 255));
        Folio7.setHorizontalAlignment(javax.swing.SwingConstants.CENTER);
        Folio7.setText("/");
        jPanel10.add(Folio7, new org.netbeans.lib.awtextra.AbsoluteConstraints(80, 60, 70, 20));

        jPanel9.add(jPanel10, new org.netbeans.lib.awtextra.AbsoluteConstraints(470, 140, 320, 100));

        nombre.addMouseMotionListener(new java.awt.event.MouseMotionAdapter() {
            public void mouseDragged(java.awt.event.MouseEvent evt) {
                NULL(evt);
            }
        });
        nombre.addMouseListener(new java.awt.event.MouseAdapter() {
            public void mouseClicked(java.awt.event.MouseEvent evt) {
                NULL(evt);
            }
            public void mouseEntered(java.awt.event.MouseEvent evt) {
                NULL(evt);
            }
        });
        nombre.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                nombreActionPerformed(evt);
            }
        });
        jPanel9.add(nombre, new org.netbeans.lib.awtextra.AbsoluteConstraints(550, 110, 240, -1));

        jPanel11.setBorder(javax.swing.BorderFactory.createTitledBorder(null, "Fecha de compra", javax.swing.border.TitledBorder.LEFT, javax.swing.border.TitledBorder.DEFAULT_POSITION, new java.awt.Font("Tahoma", 0, 14), new java.awt.Color(255, 255, 255))); // NOI18N
        jPanel11.setOpaque(false);
        jPanel11.setLayout(new org.netbeans.lib.awtextra.AbsoluteLayout());

        Folio8.setFont(new java.awt.Font("Tahoma", 0, 14)); // NOI18N
        Folio8.setForeground(new java.awt.Color(255, 255, 255));
        Folio8.setHorizontalAlignment(javax.swing.SwingConstants.CENTER);
        Folio8.setText("Año");
        Folio8.addMouseMotionListener(new java.awt.event.MouseMotionAdapter() {
            public void mouseDragged(java.awt.event.MouseEvent evt) {
                NULL(evt);
            }
        });
        Folio8.addMouseListener(new java.awt.event.MouseAdapter() {
            public void mouseClicked(java.awt.event.MouseEvent evt) {
                NULL(evt);
            }
            public void mouseEntered(java.awt.event.MouseEvent evt) {
                NULL(evt);
            }
        });
        jPanel11.add(Folio8, new org.netbeans.lib.awtextra.AbsoluteConstraints(230, 30, 70, 20));

        Folio9.setFont(new java.awt.Font("Tahoma", 0, 14)); // NOI18N
        Folio9.setForeground(new java.awt.Color(255, 255, 255));
        Folio9.setHorizontalAlignment(javax.swing.SwingConstants.CENTER);
        Folio9.setText("Mes");
        Folio9.addMouseMotionListener(new java.awt.event.MouseMotionAdapter() {
            public void mouseDragged(java.awt.event.MouseEvent evt) {
                NULL(evt);
            }
        });
        Folio9.addMouseListener(new java.awt.event.MouseAdapter() {
            public void mouseClicked(java.awt.event.MouseEvent evt) {
                NULL(evt);
            }
            public void mouseEntered(java.awt.event.MouseEvent evt) {
                NULL(evt);
            }
        });
        jPanel11.add(Folio9, new org.netbeans.lib.awtextra.AbsoluteConstraints(130, 30, 70, 20));

        mesCompra.setHorizontalAlignment(javax.swing.JTextField.CENTER);
        mesCompra.setText("1");
        mesCompra.addMouseMotionListener(new java.awt.event.MouseMotionAdapter() {
            public void mouseDragged(java.awt.event.MouseEvent evt) {
                NULL(evt);
            }
        });
        mesCompra.addMouseListener(new java.awt.event.MouseAdapter() {
            public void mouseClicked(java.awt.event.MouseEvent evt) {
                NULL(evt);
            }
            public void mouseEntered(java.awt.event.MouseEvent evt) {
                NULL(evt);
            }
        });
        mesCompra.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                mesCompraActionPerformed(evt);
            }
        });
        jPanel11.add(mesCompra, new org.netbeans.lib.awtextra.AbsoluteConstraints(130, 60, 70, -1));

        diaCompra.setHorizontalAlignment(javax.swing.JTextField.CENTER);
        diaCompra.setText("1");
        diaCompra.addMouseMotionListener(new java.awt.event.MouseMotionAdapter() {
            public void mouseDragged(java.awt.event.MouseEvent evt) {
                NULL(evt);
            }
        });
        diaCompra.addFocusListener(new java.awt.event.FocusAdapter() {
            public void focusGained(java.awt.event.FocusEvent evt) {
                diaCompraFocusGained(evt);
            }
            public void focusLost(java.awt.event.FocusEvent evt) {
                diaCompraFocusLost(evt);
            }
        });
        diaCompra.addMouseListener(new java.awt.event.MouseAdapter() {
            public void mouseClicked(java.awt.event.MouseEvent evt) {
                NULL(evt);
            }
            public void mouseEntered(java.awt.event.MouseEvent evt) {
                NULL(evt);
            }
        });
        diaCompra.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                diaCompraActionPerformed(evt);
            }
        });
        jPanel11.add(diaCompra, new org.netbeans.lib.awtextra.AbsoluteConstraints(30, 60, 70, -1));

        añoCompra.setHorizontalAlignment(javax.swing.JTextField.CENTER);
        añoCompra.setText("2007");
        añoCompra.addMouseMotionListener(new java.awt.event.MouseMotionAdapter() {
            public void mouseDragged(java.awt.event.MouseEvent evt) {
                NULL(evt);
            }
        });
        añoCompra.addMouseListener(new java.awt.event.MouseAdapter() {
            public void mouseClicked(java.awt.event.MouseEvent evt) {
                NULL(evt);
            }
            public void mouseEntered(java.awt.event.MouseEvent evt) {
                NULL(evt);
            }
        });
        añoCompra.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                añoCompraActionPerformed(evt);
            }
        });
        jPanel11.add(añoCompra, new org.netbeans.lib.awtextra.AbsoluteConstraints(230, 60, 70, -1));

        Folio10.setFont(new java.awt.Font("Tahoma", 0, 14)); // NOI18N
        Folio10.setForeground(new java.awt.Color(255, 255, 255));
        Folio10.setHorizontalAlignment(javax.swing.SwingConstants.CENTER);
        Folio10.setText("/");
        jPanel11.add(Folio10, new org.netbeans.lib.awtextra.AbsoluteConstraints(180, 60, 70, 20));

        Folio11.setFont(new java.awt.Font("Tahoma", 0, 14)); // NOI18N
        Folio11.setForeground(new java.awt.Color(255, 255, 255));
        Folio11.setHorizontalAlignment(javax.swing.SwingConstants.CENTER);
        Folio11.setText("Dia");
        Folio11.addMouseMotionListener(new java.awt.event.MouseMotionAdapter() {
            public void mouseDragged(java.awt.event.MouseEvent evt) {
                NULL(evt);
            }
        });
        Folio11.addMouseListener(new java.awt.event.MouseAdapter() {
            public void mouseClicked(java.awt.event.MouseEvent evt) {
                NULL(evt);
            }
            public void mouseEntered(java.awt.event.MouseEvent evt) {
                NULL(evt);
            }
        });
        jPanel11.add(Folio11, new org.netbeans.lib.awtextra.AbsoluteConstraints(30, 30, 70, 20));

        Folio12.setFont(new java.awt.Font("Tahoma", 0, 14)); // NOI18N
        Folio12.setForeground(new java.awt.Color(255, 255, 255));
        Folio12.setHorizontalAlignment(javax.swing.SwingConstants.CENTER);
        Folio12.setText("/");
        jPanel11.add(Folio12, new org.netbeans.lib.awtextra.AbsoluteConstraints(80, 60, 70, 20));

        jPanel9.add(jPanel11, new org.netbeans.lib.awtextra.AbsoluteConstraints(20, 140, 320, 100));

        folio.setEnabled(false);
        folio.addMouseMotionListener(new java.awt.event.MouseMotionAdapter() {
            public void mouseDragged(java.awt.event.MouseEvent evt) {
                NULL(evt);
            }
        });
        folio.addMouseListener(new java.awt.event.MouseAdapter() {
            public void mouseClicked(java.awt.event.MouseEvent evt) {
                NULL(evt);
            }
            public void mouseEntered(java.awt.event.MouseEvent evt) {
                NULL(evt);
            }
        });
        folio.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                folioActionPerformed(evt);
            }
        });
        jPanel9.add(folio, new org.netbeans.lib.awtextra.AbsoluteConstraints(660, 30, 130, -1));

        Etiqueta.setFont(new java.awt.Font("Tahoma", 0, 18)); // NOI18N
        Etiqueta.setForeground(new java.awt.Color(255, 255, 255));
        Etiqueta.setText("Total    $");
        jPanel9.add(Etiqueta, new org.netbeans.lib.awtextra.AbsoluteConstraints(600, 330, 90, 20));

        facturado.setModel(new javax.swing.DefaultComboBoxModel(new String[] { "Rosa Isela Mota Robles", "Odontologia Especializada" }));
        facturado.addMouseMotionListener(new java.awt.event.MouseMotionAdapter() {
            public void mouseDragged(java.awt.event.MouseEvent evt) {
                NULL(evt);
            }
        });
        facturado.addMouseListener(new java.awt.event.MouseAdapter() {
            public void mouseClicked(java.awt.event.MouseEvent evt) {
                NULL(evt);
            }
            public void mouseEntered(java.awt.event.MouseEvent evt) {
                NULL(evt);
            }
        });
        jPanel9.add(facturado, new org.netbeans.lib.awtextra.AbsoluteConstraints(110, 30, 230, 20));

        Folio2.setFont(new java.awt.Font("Tahoma", 0, 18)); // NOI18N
        Folio2.setForeground(new java.awt.Color(255, 255, 255));
        Folio2.setText("Razon social:");
        Folio2.addMouseMotionListener(new java.awt.event.MouseMotionAdapter() {
            public void mouseDragged(java.awt.event.MouseEvent evt) {
                NULL(evt);
            }
        });
        Folio2.addMouseListener(new java.awt.event.MouseAdapter() {
            public void mouseClicked(java.awt.event.MouseEvent evt) {
                NULL(evt);
            }
            public void mouseEntered(java.awt.event.MouseEvent evt) {
                NULL(evt);
            }
        });
        jPanel9.add(Folio2, new org.netbeans.lib.awtextra.AbsoluteConstraints(20, 70, 110, 20));

        totalFA.setHorizontalAlignment(javax.swing.JTextField.LEFT);
        totalFA.setText("0");
        totalFA.setEnabled(false);
        totalFA.addMouseMotionListener(new java.awt.event.MouseMotionAdapter() {
            public void mouseDragged(java.awt.event.MouseEvent evt) {
                NULL(evt);
            }
        });
        totalFA.addFocusListener(new java.awt.event.FocusAdapter() {
            public void focusGained(java.awt.event.FocusEvent evt) {
                totalFAFocusGained(evt);
            }
            public void focusLost(java.awt.event.FocusEvent evt) {
                totalFAFocusLost(evt);
            }
        });
        totalFA.addMouseListener(new java.awt.event.MouseAdapter() {
            public void mouseClicked(java.awt.event.MouseEvent evt) {
                NULL(evt);
            }
            public void mouseEntered(java.awt.event.MouseEvent evt) {
                NULL(evt);
            }
        });
        totalFA.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                totalFAActionPerformed(evt);
            }
        });
        totalFA.addKeyListener(new java.awt.event.KeyAdapter() {
            public void keyReleased(java.awt.event.KeyEvent evt) {
                totalFAKeyReleased(evt);
            }
            public void keyTyped(java.awt.event.KeyEvent evt) {
                totalFAKeyTyped(evt);
            }
        });
        jPanel9.add(totalFA, new org.netbeans.lib.awtextra.AbsoluteConstraints(690, 330, 100, -1));

        Folio14.setFont(new java.awt.Font("Tahoma", 0, 18)); // NOI18N
        Folio14.setForeground(new java.awt.Color(255, 255, 255));
        Folio14.setText("R.F.C.");
        jPanel9.add(Folio14, new org.netbeans.lib.awtextra.AbsoluteConstraints(20, 110, 110, 20));

        iva.setFont(new java.awt.Font("Tahoma", 0, 18)); // NOI18N
        iva.setForeground(new java.awt.Color(255, 255, 255));
        iva.setSelected(true);
        iva.setText("   Iva   $");
        iva.setOpaque(false);
        iva.setRequestFocusEnabled(false);
        iva.setRolloverEnabled(false);
        iva.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                ivaActionPerformed(evt);
            }
        });
        jPanel9.add(iva, new org.netbeans.lib.awtextra.AbsoluteConstraints(580, 300, 100, 20));

        Total1.setBackground(new java.awt.Color(204, 204, 204));
        Total1.setFont(new java.awt.Font("Tahoma", 0, 18)); // NOI18N
        Total1.setForeground(new java.awt.Color(204, 204, 204));
        Total1.setText("Sub total    $");
        jPanel9.add(Total1, new org.netbeans.lib.awtextra.AbsoluteConstraints(570, 270, 110, 20));

        ivaTotal.setHorizontalAlignment(javax.swing.JTextField.LEFT);
        ivaTotal.setText("0");
        ivaTotal.setEnabled(false);
        ivaTotal.addMouseMotionListener(new java.awt.event.MouseMotionAdapter() {
            public void mouseDragged(java.awt.event.MouseEvent evt) {
                NULL(evt);
            }
        });
        ivaTotal.addFocusListener(new java.awt.event.FocusAdapter() {
            public void focusGained(java.awt.event.FocusEvent evt) {
                ivaTotalFocusGained(evt);
            }
            public void focusLost(java.awt.event.FocusEvent evt) {
                ivaTotalFocusLost(evt);
            }
        });
        ivaTotal.addMouseListener(new java.awt.event.MouseAdapter() {
            public void mouseClicked(java.awt.event.MouseEvent evt) {
                NULL(evt);
            }
            public void mouseEntered(java.awt.event.MouseEvent evt) {
                NULL(evt);
            }
        });
        ivaTotal.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                ivaTotalActionPerformed(evt);
            }
        });
        ivaTotal.addKeyListener(new java.awt.event.KeyAdapter() {
            public void keyReleased(java.awt.event.KeyEvent evt) {
                ivaTotalKeyReleased(evt);
            }
            public void keyTyped(java.awt.event.KeyEvent evt) {
                ivaTotalKeyTyped(evt);
            }
        });
        jPanel9.add(ivaTotal, new org.netbeans.lib.awtextra.AbsoluteConstraints(690, 300, 100, -1));

        tipo.addMouseMotionListener(new java.awt.event.MouseMotionAdapter() {
            public void mouseDragged(java.awt.event.MouseEvent evt) {
                tipoNULL(evt);
            }
        });
        tipo.addMouseListener(new java.awt.event.MouseAdapter() {
            public void mouseClicked(java.awt.event.MouseEvent evt) {
                tipoNULL1(evt);
            }
            public void mouseEntered(java.awt.event.MouseEvent evt) {
                tipoNULL2(evt);
            }
        });
        tipo.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                tipoActionPerformed(evt);
            }
        });
        jPanel9.add(tipo, new org.netbeans.lib.awtextra.AbsoluteConstraints(140, 310, 200, -1));

        Folio13.setFont(new java.awt.Font("Tahoma", 0, 18)); // NOI18N
        Folio13.setForeground(new java.awt.Color(255, 255, 255));
        Folio13.setText("Facturado");
        jPanel9.add(Folio13, new org.netbeans.lib.awtextra.AbsoluteConstraints(20, 20, 110, 30));

        rfc.addMouseMotionListener(new java.awt.event.MouseMotionAdapter() {
            public void mouseDragged(java.awt.event.MouseEvent evt) {
                rfcNULL(evt);
            }
        });
        rfc.addFocusListener(new java.awt.event.FocusAdapter() {
            public void focusGained(java.awt.event.FocusEvent evt) {
                rfcFocusGained(evt);
            }
            public void focusLost(java.awt.event.FocusEvent evt) {
                rfcFocusLost(evt);
            }
        });
        rfc.addMouseListener(new java.awt.event.MouseAdapter() {
            public void mouseClicked(java.awt.event.MouseEvent evt) {
                rfcNULL1(evt);
            }
            public void mouseEntered(java.awt.event.MouseEvent evt) {
                rfcNULL2(evt);
            }
        });
        rfc.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                rfcActionPerformed(evt);
            }
        });
        jPanel9.add(rfc, new org.netbeans.lib.awtextra.AbsoluteConstraints(80, 110, 260, -1));

        Folio15.setFont(new java.awt.Font("Tahoma", 0, 18)); // NOI18N
        Folio15.setForeground(new java.awt.Color(255, 255, 255));
        Folio15.setText("Tipo");
        jPanel9.add(Folio15, new org.netbeans.lib.awtextra.AbsoluteConstraints(30, 350, 110, 20));

        subTotal1.setHorizontalAlignment(javax.swing.JTextField.LEFT);
        subTotal1.setText("0");
        subTotal1.addMouseMotionListener(new java.awt.event.MouseMotionAdapter() {
            public void mouseDragged(java.awt.event.MouseEvent evt) {
                subTotal1NULL(evt);
            }
        });
        subTotal1.addFocusListener(new java.awt.event.FocusAdapter() {
            public void focusGained(java.awt.event.FocusEvent evt) {
                subTotal1FocusGained(evt);
            }
            public void focusLost(java.awt.event.FocusEvent evt) {
                subTotal1FocusLost(evt);
            }
        });
        subTotal1.addMouseListener(new java.awt.event.MouseAdapter() {
            public void mouseClicked(java.awt.event.MouseEvent evt) {
                subTotal1NULL1(evt);
            }
            public void mouseEntered(java.awt.event.MouseEvent evt) {
                subTotal1NULL2(evt);
            }
        });
        subTotal1.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                subTotal1ActionPerformed(evt);
            }
        });
        subTotal1.addKeyListener(new java.awt.event.KeyAdapter() {
            public void keyPressed(java.awt.event.KeyEvent evt) {
                subTotal1KeyPressed(evt);
            }
            public void keyReleased(java.awt.event.KeyEvent evt) {
                subTotal1KeyReleased(evt);
            }
            public void keyTyped(java.awt.event.KeyEvent evt) {
                subTotal1KeyTyped(evt);
            }
        });
        jPanel9.add(subTotal1, new org.netbeans.lib.awtextra.AbsoluteConstraints(690, 270, 100, -1));

        comFis2.setFont(new java.awt.Font("Tahoma", 0, 18)); // NOI18N
        comFis2.setForeground(new java.awt.Color(255, 255, 255));
        comFis2.setText("Nota (Sin Factura)");
        comFis2.setOpaque(false);
        comFis2.addMouseMotionListener(new java.awt.event.MouseMotionAdapter() {
            public void mouseDragged(java.awt.event.MouseEvent evt) {
                comFis2NULL(evt);
            }
        });
        comFis2.addMouseListener(new java.awt.event.MouseAdapter() {
            public void mouseClicked(java.awt.event.MouseEvent evt) {
                comFis2NULL1(evt);
            }
            public void mouseEntered(java.awt.event.MouseEvent evt) {
                comFis2NULL2(evt);
            }
        });
        comFis2.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                comFis2ActionPerformed(evt);
            }
        });
        jPanel9.add(comFis2, new org.netbeans.lib.awtextra.AbsoluteConstraints(380, 30, -1, 20));

        jButton4.setText("Agregar");
        jButton4.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jButton4ActionPerformed(evt);
            }
        });
        jPanel9.add(jButton4, new org.netbeans.lib.awtextra.AbsoluteConstraints(380, 310, -1, -1));

        jButton5.setText("Agregar");
        jButton5.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jButton5ActionPerformed(evt);
            }
        });
        jPanel9.add(jButton5, new org.netbeans.lib.awtextra.AbsoluteConstraints(380, 70, -1, -1));

        tipo1.setModel(new javax.swing.DefaultComboBoxModel(new String[] { "Dentitodo", "Odontología Especializada" }));
        tipo1.setToolTipText("");
        tipo1.addMouseMotionListener(new java.awt.event.MouseMotionAdapter() {
            public void mouseDragged(java.awt.event.MouseEvent evt) {
                tipo1NULL(evt);
            }
        });
        tipo1.addMouseListener(new java.awt.event.MouseAdapter() {
            public void mouseClicked(java.awt.event.MouseEvent evt) {
                tipo1NULL1(evt);
            }
            public void mouseEntered(java.awt.event.MouseEvent evt) {
                tipo1NULL2(evt);
            }
        });
        tipo1.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                tipo1ActionPerformed(evt);
            }
        });
        jPanel9.add(tipo1, new org.netbeans.lib.awtextra.AbsoluteConstraints(140, 70, 200, -1));

        Folio16.setFont(new java.awt.Font("Tahoma", 0, 18)); // NOI18N
        Folio16.setForeground(new java.awt.Color(255, 255, 255));
        Folio16.setText("Folio");
        Folio16.addMouseMotionListener(new java.awt.event.MouseMotionAdapter() {
            public void mouseDragged(java.awt.event.MouseEvent evt) {
                Folio16NULL(evt);
            }
        });
        Folio16.addMouseListener(new java.awt.event.MouseAdapter() {
            public void mouseClicked(java.awt.event.MouseEvent evt) {
                Folio16NULL1(evt);
            }
            public void mouseEntered(java.awt.event.MouseEvent evt) {
                Folio16NULL2(evt);
            }
        });
        jPanel9.add(Folio16, new org.netbeans.lib.awtextra.AbsoluteConstraints(470, 70, 110, 20));

        tipo2.setModel(new javax.swing.DefaultComboBoxModel(new String[] { "Gasto", "Compra" }));
        tipo2.addMouseMotionListener(new java.awt.event.MouseMotionAdapter() {
            public void mouseDragged(java.awt.event.MouseEvent evt) {
                tipo2NULL(evt);
            }
        });
        tipo2.addMouseListener(new java.awt.event.MouseAdapter() {
            public void mouseClicked(java.awt.event.MouseEvent evt) {
                tipo2NULL1(evt);
            }
            public void mouseEntered(java.awt.event.MouseEvent evt) {
                tipo2NULL2(evt);
            }
        });
        tipo2.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                tipo2ActionPerformed(evt);
            }
        });
        jPanel9.add(tipo2, new org.netbeans.lib.awtextra.AbsoluteConstraints(140, 350, 200, -1));

        Folio17.setFont(new java.awt.Font("Tahoma", 0, 18)); // NOI18N
        Folio17.setForeground(new java.awt.Color(255, 255, 255));
        Folio17.setText("Categoria");
        jPanel9.add(Folio17, new org.netbeans.lib.awtextra.AbsoluteConstraints(30, 310, 110, 20));

        nombre1.addMouseMotionListener(new java.awt.event.MouseMotionAdapter() {
            public void mouseDragged(java.awt.event.MouseEvent evt) {
                nombre1NULL(evt);
            }
        });
        nombre1.addMouseListener(new java.awt.event.MouseAdapter() {
            public void mouseClicked(java.awt.event.MouseEvent evt) {
                nombre1NULL1(evt);
            }
            public void mouseEntered(java.awt.event.MouseEvent evt) {
                nombre1NULL2(evt);
            }
        });
        nombre1.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                nombre1ActionPerformed(evt);
            }
        });
        jPanel9.add(nombre1, new org.netbeans.lib.awtextra.AbsoluteConstraints(550, 70, 240, -1));

        Folio18.setFont(new java.awt.Font("Tahoma", 0, 18)); // NOI18N
        Folio18.setForeground(new java.awt.Color(255, 255, 255));
        Folio18.setText("Nombre");
        Folio18.addMouseMotionListener(new java.awt.event.MouseMotionAdapter() {
            public void mouseDragged(java.awt.event.MouseEvent evt) {
                Folio18NULL(evt);
            }
        });
        Folio18.addMouseListener(new java.awt.event.MouseAdapter() {
            public void mouseClicked(java.awt.event.MouseEvent evt) {
                Folio18NULL1(evt);
            }
            public void mouseEntered(java.awt.event.MouseEvent evt) {
                Folio18NULL2(evt);
            }
        });
        jPanel9.add(Folio18, new org.netbeans.lib.awtextra.AbsoluteConstraints(470, 110, 110, 20));

        getContentPane().add(jPanel9);
        jPanel9.setBounds(50, 70, 810, 410);

        jButton2.setText("Guardar");
        jButton2.setEnabled(false);
        jButton2.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jButton2ActionPerformed(evt);
            }
        });
        getContentPane().add(jButton2);
        jButton2.setBounds(660, 460, 170, 50);

        Fondo.setIcon(new javax.swing.ImageIcon(getClass().getResource("/com/dent/imag/red-simple-wallpapers-1920x1080.jpg"))); // NOI18N
        Fondo.setText("cx");
        Fondo.setMaximumSize(new java.awt.Dimension(500, 560));
        Fondo.setMinimumSize(new java.awt.Dimension(500, 560));
        Fondo.setRequestFocusEnabled(false);
        getContentPane().add(Fondo);
        Fondo.setBounds(0, 0, 970, 1080);

        pack();
    }// </editor-fold>//GEN-END:initComponents

    private void mo2iActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_mo2iActionPerformed
        // TODO add your handling code here:
    }//GEN-LAST:event_mo2iActionPerformed

    private void bi50iActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_bi50iActionPerformed
        // TODO add your handling code here:
    }//GEN-LAST:event_bi50iActionPerformed

    private void bi1000iActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_bi1000iActionPerformed
        // TODO add your handling code here:
        
    }//GEN-LAST:event_bi1000iActionPerformed

    private void bi20iActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_bi20iActionPerformed
        // TODO add your handling code here:
    }//GEN-LAST:event_bi20iActionPerformed

    private void mo5iActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_mo5iActionPerformed
        // TODO add your handling code here:
    }//GEN-LAST:event_mo5iActionPerformed

    private void mo10iActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_mo10iActionPerformed
        // TODO add your handling code here:
    }//GEN-LAST:event_mo10iActionPerformed

    private void mo1iActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_mo1iActionPerformed
        // TODO add your handling code here:
    }//GEN-LAST:event_mo1iActionPerformed

    private void mo05iActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_mo05iActionPerformed
        // TODO add your handling code here:
    }//GEN-LAST:event_mo05iActionPerformed

    private void monedasWindowOpened(java.awt.event.WindowEvent evt) {//GEN-FIRST:event_monedasWindowOpened
        // TODO add your handling code here:
        
    }//GEN-LAST:event_monedasWindowOpened

    public void borram() {
        d1000.setText("$0");
        d500.setText("$0");
        d200.setText("$0");
        d100.setText("$0");
        d50.setText("$0");
        d20.setText("$0");
        d10.setText("$0");
        d5.setText("$0");
        d2.setText("$0");
        d1.setText("$0");
        d05.setText("$0");
        totalCam.setText("$0.0");
        bi1000i.setText("0");
        bi500i.setText("0");
        bi200i.setText("0");
        bi100i.setText("0");
        bi50i.setText("0");
        bi20i.setText("0");
        mo10i.setText("0");
        mo5i.setText("0");
        mo2i.setText("0");
        mo1i.setText("0");
        mo05i.setText("0");
        fechaAn.setText("Fecha: ");
        monAn.setText("$0.0");
        moDife.setText("$0.0");
        moComen.setText("Comentario");

    }
    
    
    private void moComenActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_moComenActionPerformed
        // TODO add your handling code here:
    }//GEN-LAST:event_moComenActionPerformed

    private void monedasWindowClosing(java.awt.event.WindowEvent evt) {//GEN-FIRST:event_monedasWindowClosing
        // TODO add your handling code here:
//        nombreEmi.setText("");
//        rfcEmi.setText("");
//        direccionEmi.setText("");
//        this.setVisible(true);
        monedas.dispose();
    }//GEN-LAST:event_monedasWindowClosing

    private void monedasWindowClosed(java.awt.event.WindowEvent evt) {//GEN-FIRST:event_monedasWindowClosed
        
        // TODO add your handling code here:
    }//GEN-LAST:event_monedasWindowClosed

    private void jButton1ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jButton1ActionPerformed
        // TODO add your handling code here:

        int[] cantidad = {Integer.parseInt(bi1000i.getText()), Integer.parseInt(bi500i.getText()),
            Integer.parseInt(bi200i.getText()), Integer.parseInt(bi100i.getText()), Integer.parseInt(bi50i.getText()),
            Integer.parseInt(bi20i.getText()), Integer.parseInt(mo10i.getText()), Integer.parseInt(mo5i.getText()),
            Integer.parseInt(mo2i.getText()), Integer.parseInt(mo1i.getText()), Integer.parseInt(mo05i.getText())};
        ImpreMonedas mo = new ImpreMonedas();

        String aux = bi1000i.getText()+","+bi500i.getText()+","+bi200i.getText()+","+bi100i.getText()+","+bi50i.getText()
                +","+bi20i.getText()+","+mo10i.getText()+","+mo5i.getText()+","+mo2i.getText()+","+mo1i.getText()
                +","+mo05i.getText()+",'"+moComen.getText()+"','"+total+"',"+moG[0]+",'"+ipUser+" "+maquinaUser+" "+nombreUser;
//        0,0,1,0,0,0,0,0,0,1,0,'esto es otrapueba',200,2,'sup'
        String consulta = "call dentitodo.registroMo (" + aux + "')";

        if (mysql.Ejecuta("root", "javac", consulta)) {
            boolean estado=true;
            do{
                estado=mo.impreMonedas(cantidad, Integer.parseInt(moG[0])+1, Double.parseDouble(moG[2]),moComen.getText());
            
            }while(estado==false);
        }
           
                JOptionPane.showMessageDialog(null, " Se guardo de forma exitosa");
                this.setVisible(true);
                monedas.dispose();

        
        
        
        
        
    }//GEN-LAST:event_jButton1ActionPerformed

    private void bi1000iKeyPressed(java.awt.event.KeyEvent evt) {//GEN-FIRST:event_bi1000iKeyPressed
        // TODO add your handling code here:        
        char c;
        c = evt.getKeyChar();
//         KeyEvent.VK_ENTER 
        if ( c ==KeyEvent.VK_ENTER ){            
            bi500i.requestFocus();    
                           }
       
        
    }//GEN-LAST:event_bi1000iKeyPressed

    private void bi500iKeyTyped(java.awt.event.KeyEvent evt) {//GEN-FIRST:event_bi500iKeyTyped
        // TODO add your handling code here:
        
         char c=evt.getKeyChar();
       if (c<'0'|| c>'9')evt.consume();
    }//GEN-LAST:event_bi500iKeyTyped

    private void bi200iKeyTyped(java.awt.event.KeyEvent evt) {//GEN-FIRST:event_bi200iKeyTyped
        // TODO add your handling code here:
        char c=evt.getKeyChar();
       if (c<'0'|| c>'9')evt.consume();
    }//GEN-LAST:event_bi200iKeyTyped

    private void nuevo(java.awt.event.KeyEvent evt) {//GEN-FIRST:event_nuevo
        // TODO add your handling code here:
        char c=evt.getKeyChar();
       if (c<'0'|| c>'9')evt.consume();
    }//GEN-LAST:event_nuevo

    private void bi500iKeyPressed(java.awt.event.KeyEvent evt) {//GEN-FIRST:event_bi500iKeyPressed
  
        char c;
        c = evt.getKeyChar();
        if ( c == KeyEvent.VK_ENTER )
        {
       
            this.bi200i.requestFocus();    
                   
        }
    }//GEN-LAST:event_bi500iKeyPressed

    private void bi200iKeyPressed(java.awt.event.KeyEvent evt) {//GEN-FIRST:event_bi200iKeyPressed

        char c;
        c = evt.getKeyChar();
        if ( c == KeyEvent.VK_ENTER )
        {
       
            this.bi100i.requestFocus();    
                   
        }// TODO add your handling code here:
    }//GEN-LAST:event_bi200iKeyPressed

    private void bi100iKeyPressed(java.awt.event.KeyEvent evt) {//GEN-FIRST:event_bi100iKeyPressed
        // TODO add your handling code here:
        char c;
        c = evt.getKeyChar();
        if ( c == KeyEvent.VK_ENTER )
        {
       
            this.bi50i.requestFocus();    
                   
        }
    }//GEN-LAST:event_bi100iKeyPressed

    private void bi50iKeyPressed(java.awt.event.KeyEvent evt) {//GEN-FIRST:event_bi50iKeyPressed
char c;
        c = evt.getKeyChar();
        if ( c == KeyEvent.VK_ENTER )
        {
       
            this.bi20i.requestFocus();    
                   
        }
        // TODO add your handling code here:
    }//GEN-LAST:event_bi50iKeyPressed

    private void bi20iKeyPressed(java.awt.event.KeyEvent evt) {//GEN-FIRST:event_bi20iKeyPressed
        // TODO add your handling code here:
        char c;
        c = evt.getKeyChar();
        if ( c == KeyEvent.VK_ENTER )
        {
       
            this.mo10i.requestFocus();    
                   
        }
    }//GEN-LAST:event_bi20iKeyPressed

    private void mo10iKeyPressed(java.awt.event.KeyEvent evt) {//GEN-FIRST:event_mo10iKeyPressed
        // TODO add your handling code here:
        char c;
        c = evt.getKeyChar();
        if ( c == KeyEvent.VK_ENTER )
        {
       
            this.mo5i.requestFocus();    
                   
        }
    }//GEN-LAST:event_mo10iKeyPressed

    private void mo5iKeyPressed(java.awt.event.KeyEvent evt) {//GEN-FIRST:event_mo5iKeyPressed
        // TODO add your handling code here:
        char c;
        c = evt.getKeyChar();
        if ( c == KeyEvent.VK_ENTER )
        {
       
            this.mo2i.requestFocus();    
                   
        }
    }//GEN-LAST:event_mo5iKeyPressed

    private void mo2iKeyPressed(java.awt.event.KeyEvent evt) {//GEN-FIRST:event_mo2iKeyPressed
        // TODO add your handling code here:
        char c;
        c = evt.getKeyChar();
        if ( c == KeyEvent.VK_ENTER )
        {
       
            this.mo1i.requestFocus();    
                   
        }
    }//GEN-LAST:event_mo2iKeyPressed

    private void mo1iKeyPressed(java.awt.event.KeyEvent evt) {//GEN-FIRST:event_mo1iKeyPressed
        // TODO add your handling code here:
        char c;
        c = evt.getKeyChar();
        if ( c == KeyEvent.VK_ENTER )
        {
       
            this.mo05i.requestFocus();    
                   
        }
    }//GEN-LAST:event_mo1iKeyPressed

    private void bi1000iFocusGained(java.awt.event.FocusEvent evt) {//GEN-FIRST:event_bi1000iFocusGained
        // TODO add your handling code here:
       
         
    }//GEN-LAST:event_bi1000iFocusGained

    private void bi500iFocusGained(java.awt.event.FocusEvent evt) {//GEN-FIRST:event_bi500iFocusGained
        // TODO add your handling code here:
         if ("0".equals(bi500i.getText())) {
        bi500i.setText("");}
    }//GEN-LAST:event_bi500iFocusGained

    private void bi200iFocusGained(java.awt.event.FocusEvent evt) {//GEN-FIRST:event_bi200iFocusGained
        // TODO add your handling code here:
         if ("0".equals(bi200i.getText())) {
        bi200i.setText("");}
    }//GEN-LAST:event_bi200iFocusGained

    private void bi100iActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_bi100iActionPerformed
        // TODO add your handling code here:
    }//GEN-LAST:event_bi100iActionPerformed

    private void bi100iFocusGained(java.awt.event.FocusEvent evt) {//GEN-FIRST:event_bi100iFocusGained
        // TODO add your handling code here:
         if ("0".equals(bi100i.getText())) {
        bi100i.setText("");}
    }//GEN-LAST:event_bi100iFocusGained

    private void bi50iFocusGained(java.awt.event.FocusEvent evt) {//GEN-FIRST:event_bi50iFocusGained
        // TODO add your handling code here:
         if ("0".equals(bi50i.getText())) {
        bi50i.setText("");}
    }//GEN-LAST:event_bi50iFocusGained

    private void bi20iFocusGained(java.awt.event.FocusEvent evt) {//GEN-FIRST:event_bi20iFocusGained
        // TODO add your handling code here:
        if ("0".equals(bi20i.getText())) {
        bi20i.setText("");}
    }//GEN-LAST:event_bi20iFocusGained

    private void mo10iFocusGained(java.awt.event.FocusEvent evt) {//GEN-FIRST:event_mo10iFocusGained
        // TODO add your handling code here:
        if ("0".equals(mo10i.getText())) {
        mo10i.setText("");}
    }//GEN-LAST:event_mo10iFocusGained

    private void mo5iFocusGained(java.awt.event.FocusEvent evt) {//GEN-FIRST:event_mo5iFocusGained
        // TODO add your handling code here:
        if ("0".equals(mo5i.getText())) {
         mo5i.setText("");}
    }//GEN-LAST:event_mo5iFocusGained

    private void mo2iFocusGained(java.awt.event.FocusEvent evt) {//GEN-FIRST:event_mo2iFocusGained
        // TODO add your handling code here:
        if ("0".equals(mo2i.getText())) {
            mo2i.setText("");
        }
         
    }//GEN-LAST:event_mo2iFocusGained

    private void mo1iFocusGained(java.awt.event.FocusEvent evt) {//GEN-FIRST:event_mo1iFocusGained
        // TODO add your handling code here:
        if ("0".equals(mo1i.getText())) {
         mo1i.setText("");}
    }//GEN-LAST:event_mo1iFocusGained

    private void mo05iFocusGained(java.awt.event.FocusEvent evt) {//GEN-FIRST:event_mo05iFocusGained
        // TODO add your handling code here:
        if ("0".equals(mo05i.getText())) {
         mo05i.setText("");}
    }//GEN-LAST:event_mo05iFocusGained

    private void calcuTotal(java.awt.event.FocusEvent evt) {//GEN-FIRST:event_calcuTotal
        // TODO add your handling code here:
        
        if ("".equals(bi1000i.getText())) {
            bi1000i.setText("0");            
        }
        if ("".equals(bi500i.getText())) {
            bi500i.setText("0");            
        }
        if ("".equals(bi200i.getText())) {
            bi200i.setText("0");            
        }
        if ("".equals(bi100i.getText())) {
            bi100i.setText("0");            
        }
        if ("".equals(bi50i.getText())) {
            bi50i.setText("0");            
        }
        if ("".equals(bi20i.getText())) {
            bi20i.setText("0");            
        }
        if ("".equals(mo10i.getText())) {
            mo10i.setText("0");            
        }
        if ("".equals(mo5i.getText())) {
            mo5i.setText("0");            
        }
        if ("".equals(mo2i.getText())) {
            mo2i.setText("0");            
        }
        if ("".equals(mo1i.getText())) {
            mo1i.setText("0");            
        }
        if ("".equals(mo05i.getText())) {
            mo05i.setText("0");            
        }
          total = Integer.parseInt(bi1000i.getText()) * 1000
                + Integer.parseInt(bi500i.getText()) * 500
                + Integer.parseInt(bi200i.getText()) * 200
                + Integer.parseInt(bi100i.getText()) * 100
                + Integer.parseInt(bi50i.getText()) * 50
                + Integer.parseInt(bi20i.getText()) * 20
                + Integer.parseInt(mo10i.getText()) * 10
                + Integer.parseInt(mo5i.getText()) * 5
                + Integer.parseInt(mo2i.getText()) * 2
                + Integer.parseInt(mo1i.getText()) * 1
                + Double.parseDouble(mo05i.getText()) * 0.5;
        
                d1000.setText("$"+ Integer.toString(Integer.parseInt(bi1000i.getText()) * 1000));
                d500.setText( "$"+ Integer.toString(Integer.parseInt(bi500i.getText()) * 500));
                d200.setText( "$"+ Integer.toString(Integer.parseInt(bi200i.getText()) * 200));
                d100.setText( "$"+ Integer.toString(Integer.parseInt(bi100i.getText()) * 100));
                d50.setText( "$"+ Integer.toString(Integer.parseInt(bi50i.getText()) * 50));
                d20.setText("$"+  Integer.toString(Integer.parseInt(bi20i.getText()) * 20));
                d10.setText( "$"+ Integer.toString(Integer.parseInt(mo10i.getText()) * 10));
                d5.setText( "$"+ Integer.toString(Integer.parseInt(mo5i.getText()) * 5));
                d2.setText( "$"+ Integer.toString(Integer.parseInt(mo2i.getText()) * 2));
                d1.setText("$"+  Integer.toString(Integer.parseInt(mo1i.getText()) * 1));
                d05.setText("$"+  String.valueOf(Double.parseDouble(mo05i.getText()) * 0.5));
                
        System.out.println(total);
        totalCam.setText("$" + String.valueOf(total));
        double calculo=total-Double.parseDouble(moG[2]);
        if (calculo==0) {
            moDife.setForeground(Color.black);
            
        }else if(calculo<0){
            moDife.setForeground(Color.red);
        }else{
            moDife.setForeground(Color.blue);
        }
       moDife.setText( "$"+String.valueOf(calculo));
        
        
        
        
    }//GEN-LAST:event_calcuTotal

    private void mo05iKeyPressed(java.awt.event.KeyEvent evt) {//GEN-FIRST:event_mo05iKeyPressed
        // TODO add your handling code here:
        char c;
        c = evt.getKeyChar();
        if ( c == KeyEvent.VK_ENTER )
        {
       
            mo05i.transferFocus();
                   
        }
        
    }//GEN-LAST:event_mo05iKeyPressed

    private void moComenFocusGained(java.awt.event.FocusEvent evt) {//GEN-FIRST:event_moComenFocusGained
        // TODO add your handling code here:
        
        moComen.setText("");
    }//GEN-LAST:event_moComenFocusGained

    private void moComenKeyTyped(java.awt.event.KeyEvent evt) {//GEN-FIRST:event_moComenKeyTyped
        // TODO add your handling code here
        char c=evt.getKeyChar();
       if ((c<123 && c>96)||(c<91 && c>64)||(c<58 && c>47)|| c==' '){
           
       }else{
           evt.consume();
       }
        
        
        
        
    }//GEN-LAST:event_moComenKeyTyped

    private void mesFacturaActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_mesFacturaActionPerformed
        // TODO add your handling code here:
    }//GEN-LAST:event_mesFacturaActionPerformed

    private void diaFacturaActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_diaFacturaActionPerformed
        // TODO add your handling code here:
    }//GEN-LAST:event_diaFacturaActionPerformed

    private void estadoActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_estadoActionPerformed
        // TODO add your handling code here:
    }//GEN-LAST:event_estadoActionPerformed

    private void añoFacturaActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_añoFacturaActionPerformed
        // TODO add your handling code here:
    }//GEN-LAST:event_añoFacturaActionPerformed

    private void nombreActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_nombreActionPerformed
        // TODO add your handling code here:
    }//GEN-LAST:event_nombreActionPerformed

    private void folioActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_folioActionPerformed
        // TODO add your handling code here:
    }//GEN-LAST:event_folioActionPerformed

    private void mesCompraActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_mesCompraActionPerformed
        // TODO add your handling code here:
    }//GEN-LAST:event_mesCompraActionPerformed

    private void diaCompraActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_diaCompraActionPerformed
        // TODO add your handling code here:
    }//GEN-LAST:event_diaCompraActionPerformed

    private void añoCompraActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_añoCompraActionPerformed
        // TODO add your handling code here:
    }//GEN-LAST:event_añoCompraActionPerformed

    private void totalFAActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_totalFAActionPerformed
        // TODO add your handling code here:
    }//GEN-LAST:event_totalFAActionPerformed

    private void ivaActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_ivaActionPerformed
        // TODO add your handling code here:

        if (iva.isSelected() == true) {
            totalFA.setEnabled(false);
            ivaTotal.setEnabled(false);
            subTotal1.setEnabled(true);
        } else {
            System.out.println("no entro ");
            totalFA.setEnabled(true);
            ivaTotal.setEnabled(true);
            subTotal1.setEnabled(false);
        }


    }//GEN-LAST:event_ivaActionPerformed

    private void ivaTotalActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_ivaTotalActionPerformed
        // TODO add your handling code here:
    }//GEN-LAST:event_ivaTotalActionPerformed

    private void NULL(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_NULL
        // TODO add your handling code here:
    }//GEN-LAST:event_NULL

    private void jButton2ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jButton2ActionPerformed
        // TODO add your handling code here:

//        (String folio,String nombre, String rfc,String fechaCompra,String fechaFactura, String estado,
//             String tipo, String Facturado, float subTotal, float ivaTotal,float total) {
//            
//        }
        int diaA_inicio = Integer.parseInt(diaCompra.getText());
        if (diaA_inicio>31) {
            diaCompra.setForeground(Color.red);
        }else{
            diaCompra.setForeground(Color.black);
        }

        int diaA_final = Integer.parseInt(diaFactura.getText());
        
        if (diaA_final>31) {
            diaFactura.setForeground(Color.red);
        }else{
            diaFactura.setForeground(Color.black);
        }

        int mesA_inicio = Integer.parseInt(mesCompra.getText());
        
        if (mesA_inicio>12) {
            mesCompra.setForeground(Color.red);
        }else{
            mesCompra.setForeground(Color.black);
        }

        int mesA_final = Integer.parseInt(mesFactura.getText());
        if (mesA_final > 12) {
            mesFactura.setForeground(Color.red);
        } else {
            mesFactura.setForeground(Color.black);
        }

        if (diaA_inicio <= 31 && diaA_final <= 31 && mesA_inicio <= 12 && mesA_final <= 12) {

            if (!"".equals(rfc.getText()) && !"".equals(folio.getText()) && !"".equals(nombre.getText())) {

                fechaCompra = añoCompra.getText() + "-" + mesCompra.getText() + "-" + diaCompra.getText();
                fechaFactura = añoFactura.getText() + "-" + mesFactura.getText() + "-" + diaFactura.getText();

                tipoEmi1.removeAllItems();
                Stack pila = new Stack();
                Stack pila2 = new Stack();
                pila = mysql.ConsultaPagos();
                while (!pila.isEmpty()) {
                    pila2.push(pila.pop());
                }                    
                while (!pila2.isEmpty()) {
                    tipoEmi1.addItem(pila2.pop());
                }    
                
                Ti_pago.show();
                

            }
        } else {
            JOptionPane.showMessageDialog(null, "Fecha incorrecta. Porfavor verifique");
        }


        
        
    }//GEN-LAST:event_jButton2ActionPerformed

    private void tipoNULL(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_tipoNULL
        // TODO add your handling code here:
    }//GEN-LAST:event_tipoNULL

    private void tipoNULL1(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_tipoNULL1
        // TODO add your handling code here:
    }//GEN-LAST:event_tipoNULL1

    private void tipoNULL2(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_tipoNULL2
        // TODO add your handling code here:
    }//GEN-LAST:event_tipoNULL2

    private void rfcNULL(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_rfcNULL
        // TODO add your handling code here:
    }//GEN-LAST:event_rfcNULL

    private void rfcNULL1(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_rfcNULL1
        // TODO add your handling code here:
    }//GEN-LAST:event_rfcNULL1

    private void rfcNULL2(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_rfcNULL2
        // TODO add your handling code here:
    }//GEN-LAST:event_rfcNULL2

    private void rfcActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_rfcActionPerformed
        // TODO add your handling code here:
    }//GEN-LAST:event_rfcActionPerformed

    private void tipoActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_tipoActionPerformed
        // TODO add your handling code here:
    }//GEN-LAST:event_tipoActionPerformed

    private void subTotal1NULL(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_subTotal1NULL
        // TODO add your handling code here:
    }//GEN-LAST:event_subTotal1NULL

    private void subTotal1NULL1(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_subTotal1NULL1
        // TODO add your handling code here:
    }//GEN-LAST:event_subTotal1NULL1

    private void subTotal1NULL2(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_subTotal1NULL2
        // TODO add your handling code here:
    }//GEN-LAST:event_subTotal1NULL2

    private void subTotal1ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_subTotal1ActionPerformed
        // TODO add your handling code here:
    }//GEN-LAST:event_subTotal1ActionPerformed

    private void subTotal1KeyPressed(java.awt.event.KeyEvent evt) {//GEN-FIRST:event_subTotal1KeyPressed
        // TODO add your handling code here:
        
        
        
       
    }//GEN-LAST:event_subTotal1KeyPressed

    private void subTotal1KeyTyped(java.awt.event.KeyEvent evt) {//GEN-FIRST:event_subTotal1KeyTyped
        // TODO add your handling code here:
         char c=evt.getKeyChar(); 
        System.out.println(subTotal1.getText().indexOf(".")); 
         if (!Character.isDigit(c) && c!='.' ) {            
             evt.consume();               
        }
          if (subTotal1.getText().indexOf(".")!=-1&&!Character.isDigit(c) ) {
                  evt.consume();  }
    }//GEN-LAST:event_subTotal1KeyTyped

    private void ivaTotalKeyTyped(java.awt.event.KeyEvent evt) {//GEN-FIRST:event_ivaTotalKeyTyped
        // TODO add your handling code here:
        
         char c=evt.getKeyChar(); 
        System.out.println(ivaTotal.getText().indexOf(".")); 
         if (!Character.isDigit(c) && c!='.' ) {            
             evt.consume();               
        }
          if (ivaTotal.getText().indexOf(".")!=-1&&!Character.isDigit(c) ) {
                  evt.consume();  }
    }//GEN-LAST:event_ivaTotalKeyTyped

    private void totalFAKeyTyped(java.awt.event.KeyEvent evt) {//GEN-FIRST:event_totalFAKeyTyped
        // TODO add your handling code here:
         char c=evt.getKeyChar(); 
        System.out.println(totalFA.getText().indexOf(".")); 
         if (!Character.isDigit(c) && c!='.' ) {            
             evt.consume();               
        }
          if (totalFA.getText().indexOf(".")!=-1&&!Character.isDigit(c) ) {
                  evt.consume();  }
        
        
    }//GEN-LAST:event_totalFAKeyTyped

    private void subTotal1FocusGained(java.awt.event.FocusEvent evt) {//GEN-FIRST:event_subTotal1FocusGained
        // TODO add your handling code here:
        subTotal1.setText("");
        
    }//GEN-LAST:event_subTotal1FocusGained

    private void ivaTotalFocusGained(java.awt.event.FocusEvent evt) {//GEN-FIRST:event_ivaTotalFocusGained
        // TODO add your handling code here:
        ivaTotal.setText("");
    }//GEN-LAST:event_ivaTotalFocusGained

    private void totalFAFocusGained(java.awt.event.FocusEvent evt) {//GEN-FIRST:event_totalFAFocusGained
        // TODO add your handling code here:
        
        totalFA.setText("");
    }//GEN-LAST:event_totalFAFocusGained

    private void subTotal1FocusLost(java.awt.event.FocusEvent evt) {//GEN-FIRST:event_subTotal1FocusLost
        // TODO add your handling code here:
        
        if ("".equals(subTotal1.getText())) {
             subTotal1.setText("0");
        }
    }//GEN-LAST:event_subTotal1FocusLost

    private void ivaTotalFocusLost(java.awt.event.FocusEvent evt) {//GEN-FIRST:event_ivaTotalFocusLost
        // TODO add your handling code here:
        if ("".equals(ivaTotal.getText())) {
             ivaTotal.setText("0");
        }
    }//GEN-LAST:event_ivaTotalFocusLost

    private void totalFAFocusLost(java.awt.event.FocusEvent evt) {//GEN-FIRST:event_totalFAFocusLost
        // TODO add your handling code here:
        if ("".equals(totalFA.getText())) {
             totalFA.setText("0");
        }
        
    }//GEN-LAST:event_totalFAFocusLost

    private void nombreEmiActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_nombreEmiActionPerformed
        // TODO add your handling code here:
    }//GEN-LAST:event_nombreEmiActionPerformed

    private void EmisorWindowOpened(java.awt.event.WindowEvent evt) {//GEN-FIRST:event_EmisorWindowOpened
        // TODO add your handling code here:
        nombreEmi.setText("");
        rfcEmi.setText("");
        direccionEmi.setText("");


        
        
    }//GEN-LAST:event_EmisorWindowOpened

    private void rfcFocusLost(java.awt.event.FocusEvent evt) {//GEN-FIRST:event_rfcFocusLost
        // TODO add your handling code here:
        
        
        System.out.println("Consulta "+rfc.getText());
        if (!"".equals(rfc.getText())) {
            if (mysql.ConsultaRfc(rfc.getText())) {
            jButton2.setEnabled(true);
            rfc.setEnabled(false);

            nombre.setText(mysql.ConsultaNombre(rfc.getText()));

        } else {
            
            nombreEmi.setText("");
            rfcEmi.setText("");
            direccionEmi.setText("");
            this.setVisible(false);
            Emisor.show();
            jButton2.setEnabled(false);
        }
        }
        
       
        
        
    }//GEN-LAST:event_rfcFocusLost

    private void rfcFocusGained(java.awt.event.FocusEvent evt) {//GEN-FIRST:event_rfcFocusGained
        // TODO add your handling code here:
        
        rfc.setText("");
    }//GEN-LAST:event_rfcFocusGained

    private void comFis2NULL(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_comFis2NULL
        // TODO add your handling code here:
    }//GEN-LAST:event_comFis2NULL

    private void comFis2NULL1(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_comFis2NULL1
        // TODO add your handling code here:
    }//GEN-LAST:event_comFis2NULL1

    private void comFis2NULL2(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_comFis2NULL2
        // TODO add your handling code here:
    }//GEN-LAST:event_comFis2NULL2

    private void comFis2ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_comFis2ActionPerformed
        // TODO add your handling code here:
        
        System.out.println("("+rfc.getText()+")");
           if (comFis2.isSelected()) {
            rfc.setText("XEXX010101000");  
            rfc.setEnabled(false);
            jButton2.setEnabled(true);
            nombre.setText(mysql.ConsultaNombre(rfc.getText()));
            estado.setText("Factura");
           
        }else{
             estado.setText("Nota de Remicion");
             rfc.setEnabled(true);
        } 
       
        
        
        
        
    }//GEN-LAST:event_comFis2ActionPerformed

    private void jButton4ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jButton4ActionPerformed
        // TODO add your handling code here:
        JFrame frame = new JFrame("Agragar");
        String name = JOptionPane.showInputDialog(frame, "Añadir categoria: ");
        if (name!=null) {
             if (mysql.InsertaCate(name)) {
            JOptionPane.showMessageDialog(null, "Se guardo correctamente la Categoria");
        }
        tipo.removeAllItems();
        Stack pila = new Stack();
        pila = mysql.ConsultaCategoria();
        while (pila.size() != 0) {
            tipo.addItem(pila.pop());
        }
        }
       


    }//GEN-LAST:event_jButton4ActionPerformed

    private void jButton3ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jButton3ActionPerformed
        // TODO add your handling code here:
        
        
        if (mysql.InsertaEmi( nombreEmi.getText() , rfcEmi.getText(), direccionEmi.getText(), tipoEmi.getSelectedItem().toString())) {
        rfc.setText(rfcEmi.getText());
       
        nombre.setEnabled(false);   
        jButton2.setEnabled(true);
        nombre.setText(mysql.ConsultaNombre(rfc.getText())) ;
        
        this.setVisible(true);
        Emisor.dispose();
        }else{
            
        }
        
        
        
    }//GEN-LAST:event_jButton3ActionPerformed

    private void subTotal1KeyReleased(java.awt.event.KeyEvent evt) {//GEN-FIRST:event_subTotal1KeyReleased
        // TODO add your handling code here:
        
        if (!"".equals(subTotal1.getText())&&!".".equals(subTotal1.getText())) {
           double calI_iva =  Double.parseDouble(subTotal1.getText());
         DecimalFormat df = new DecimalFormat("#0.00");
         System.out.println(df.format(calI_iva));
        ivaTotal.setText(subTotal1.getText());
       
        ivaTotal.setText(df.format(calI_iva*0.16));
        totalFA.setText(df.format(calI_iva*1.16));        
        }
         
        
        
        
        
        
        
    }//GEN-LAST:event_subTotal1KeyReleased

    private void jButton5ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jButton5ActionPerformed
        // TODO add your handling code here:
      
        nombreEmi.setText("");
        rfcEmi.setText("");
        direccionEmi.setText("");
        this.setVisible(false);
         Emisor.show();
    }//GEN-LAST:event_jButton5ActionPerformed

    private void tipoEmiActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_tipoEmiActionPerformed
        // TODO add your handling code here:
    }//GEN-LAST:event_tipoEmiActionPerformed

    private void tipo1NULL(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_tipo1NULL
        // TODO add your handling code here:
    }//GEN-LAST:event_tipo1NULL

    private void tipo1NULL1(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_tipo1NULL1
        // TODO add your handling code here:
    }//GEN-LAST:event_tipo1NULL1

    private void tipo1NULL2(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_tipo1NULL2
        // TODO add your handling code here:
    }//GEN-LAST:event_tipo1NULL2

    private void tipo1ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_tipo1ActionPerformed
        // TODO add your handling code here:
    }//GEN-LAST:event_tipo1ActionPerformed

    private void Folio16NULL(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_Folio16NULL
        // TODO add your handling code here:
    }//GEN-LAST:event_Folio16NULL

    private void Folio16NULL1(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_Folio16NULL1
        // TODO add your handling code here:
    }//GEN-LAST:event_Folio16NULL1

    private void Folio16NULL2(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_Folio16NULL2
        // TODO add your handling code here:
    }//GEN-LAST:event_Folio16NULL2

    private void jTextField1ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jTextField1ActionPerformed
        // TODO add your handling code here:
    }//GEN-LAST:event_jTextField1ActionPerformed

    private void Aceptar_cateActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_Aceptar_cateActionPerformed
        // TODO add your handling code here:
        if ("".equals(jTextField1.getText())) {
            JOptionPane.showMessageDialog(
                    null,
                    "Por favor, escoja un nombre para la categoría");
            
        }else{
            
        }
        
        
        
        
    }//GEN-LAST:event_Aceptar_cateActionPerformed

    private void jButton8ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jButton8ActionPerformed
        // TODO add your handling code here:
        jFrame1.dispose();
        
    }//GEN-LAST:event_jButton8ActionPerformed

    private void tipo2NULL(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_tipo2NULL
        // TODO add your handling code here:
    }//GEN-LAST:event_tipo2NULL

    private void tipo2NULL1(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_tipo2NULL1
        // TODO add your handling code here:
    }//GEN-LAST:event_tipo2NULL1

    private void tipo2NULL2(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_tipo2NULL2
        // TODO add your handling code here:
    }//GEN-LAST:event_tipo2NULL2

    private void tipo2ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_tipo2ActionPerformed
        // TODO add your handling code here:
    }//GEN-LAST:event_tipo2ActionPerformed

    private void nombre1NULL(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_nombre1NULL
        // TODO add your handling code here:
    }//GEN-LAST:event_nombre1NULL

    private void nombre1NULL1(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_nombre1NULL1
        // TODO add your handling code here:
    }//GEN-LAST:event_nombre1NULL1

    private void nombre1NULL2(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_nombre1NULL2
        // TODO add your handling code here:
    }//GEN-LAST:event_nombre1NULL2

    private void nombre1ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_nombre1ActionPerformed
        // TODO add your handling code here:
    }//GEN-LAST:event_nombre1ActionPerformed

    private void Folio18NULL(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_Folio18NULL
        // TODO add your handling code here:
    }//GEN-LAST:event_Folio18NULL

    private void Folio18NULL1(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_Folio18NULL1
        // TODO add your handling code here:
    }//GEN-LAST:event_Folio18NULL1

    private void Folio18NULL2(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_Folio18NULL2
        // TODO add your handling code here:
    }//GEN-LAST:event_Folio18NULL2

    private void ivaTotalKeyReleased(java.awt.event.KeyEvent evt) {//GEN-FIRST:event_ivaTotalKeyReleased
        // TODO add your handling code here:

        if (!"".equals(ivaTotal.getText()) && !".".equals(ivaTotal.getText())) {
            double calI_iva = Double.parseDouble(ivaTotal.getText());
            double calI_iva2 = Double.parseDouble(totalFA.getText());
            DecimalFormat df = new DecimalFormat("#0.00");
            System.out.println(df.format(calI_iva));

            subTotal1.setText(df.format(calI_iva2 - calI_iva));

        }
    }//GEN-LAST:event_ivaTotalKeyReleased

    private void totalFAKeyReleased(java.awt.event.KeyEvent evt) {//GEN-FIRST:event_totalFAKeyReleased
        // TODO add your handling code here:
        if (!"".equals(totalFA.getText()) && !".".equals(totalFA.getText())) {
            double calI_iva = Double.parseDouble(ivaTotal.getText());
            double calI_iva2 = Double.parseDouble(totalFA.getText());
            DecimalFormat df = new DecimalFormat("#0.00");
            System.out.println(df.format(calI_iva));

            subTotal1.setText(df.format(calI_iva2 - calI_iva));

        }
    }//GEN-LAST:event_totalFAKeyReleased

    private void EmisorWindowClosing(java.awt.event.WindowEvent evt) {//GEN-FIRST:event_EmisorWindowClosing
        // TODO add your handling code here:
        
        this.setVisible(true);
    }//GEN-LAST:event_EmisorWindowClosing

    private void jButton6ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jButton6ActionPerformed
        // TODO add your handling code here:
        this.setVisible(true);
        Emisor.dispose();
    }//GEN-LAST:event_jButton6ActionPerformed

    private void jButton7ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jButton7ActionPerformed
        // TODO add your handling code here:
        Ti_pago.dispose();

    }//GEN-LAST:event_jButton7ActionPerformed

    private void jButton9ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jButton9ActionPerformed
        // TODO add your handling code here:
         String numero =nombreEmi1.getText();
         
        if (numero.length()==4 || tipoEmi1.getSelectedIndex() == (0)) {
        int tipopago_no = tipoEmi1.getSelectedIndex() + 1;

        if (mysql.Consultatem(folio.getText(), nombre.getText(), rfc.getText(), fechaCompra, fechaFactura, comFis2.isSelected(), tipo.getSelectedItem().toString(), facturado.getSelectedItem().toString(),
                parseDouble(subTotal1.getText()), parseDouble(ivaTotal.getText()), parseDouble(totalFA.getText()), tipo1.getSelectedIndex(),
                tipo2.getSelectedItem().toString(), nombre1.getText(), String.valueOf(tipopago_no), nombreEmi1.getText())) {

//            
            String folio_con = "A001-" + mysql.ConsultaRfcFolio();
           

            subTotal1.setText("0");
            ivaTotal.setText("0");
            totalFA.setText("0");

            nombre1.setText("");
            nombre.setText("");
            rfc.setText("");
            comFis2.setSelected(false);

            rfc.setEnabled(true);
            nombre.setEnabled(true);
            nombreEmi1.setText("");

            folio.setText(folio_con);
            Ti_pago.dispose();
        } else {
            JOptionPane.showMessageDialog(null, "Se genero un error al guardar la factura");
        }
          }
    }//GEN-LAST:event_jButton9ActionPerformed

    private void Ti_pagoWindowClosing(java.awt.event.WindowEvent evt) {//GEN-FIRST:event_Ti_pagoWindowClosing
        // TODO add your handling code here:
    }//GEN-LAST:event_Ti_pagoWindowClosing

    private void Ti_pagoWindowOpened(java.awt.event.WindowEvent evt) {//GEN-FIRST:event_Ti_pagoWindowOpened
        // TODO add your handling code here:


    }//GEN-LAST:event_Ti_pagoWindowOpened

    private void tipoEmi1ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_tipoEmi1ActionPerformed
        // TODO add your handling code here:
        
        
         if (tipoEmi1.getSelectedIndex() == (0)) {
            nombreEmi1.setEnabled(false);
            jButton9.setEnabled(true);
            nombreEmi1.setText("0");
        } else {
            nombreEmi1.setEnabled(true);
            jButton9.setEnabled(false);
            nombreEmi1.setText("");
        }
        
        
    }//GEN-LAST:event_tipoEmi1ActionPerformed

    private void nombreEmi1ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_nombreEmi1ActionPerformed
        // TODO add your handling code here:
    }//GEN-LAST:event_nombreEmi1ActionPerformed

    private void nombreEmi1FocusGained(java.awt.event.FocusEvent evt) {//GEN-FIRST:event_nombreEmi1FocusGained
        // TODO add your handling code here:
        
        nombreEmi1.setText("");
    }//GEN-LAST:event_nombreEmi1FocusGained

    private void nombreEmi1FocusLost(java.awt.event.FocusEvent evt) {//GEN-FIRST:event_nombreEmi1FocusLost
        // TODO add your handling code here:
        
        
        
        
        veriNumero();
        
    }//GEN-LAST:event_nombreEmi1FocusLost

    private void veriNumero(){
        String numero =nombreEmi1.getText();
        boolean estado = false;
        
        System.out.println("palabra  = "+ numero);
        for (int i = 0; i < numero.length(); i++) {
            if (numero.charAt(i)>47 && numero.charAt(i)<58) {
                estado = true;                
            }else{
                estado = false;                 
                System.out.println("Error  no es numero "+ numero.charAt(i)+" "+estado);
                break;
            }
           
        }
        if (estado) {
          if (numero.length()!=4 ) {
            JOptionPane.showMessageDialog(null, "Pavor de colocar los 4 ultimos numeros ");
            jButton9.setEnabled(false);
        }else{
            jButton9.setEnabled(true);
        }  
        }else{
            JOptionPane.showMessageDialog(null, "Pavor de colocar los 4 ultimos numeros ");
            jButton9.setEnabled(false);
            
        }
    }
    
    
    
    private void nombreEmi1KeyReleased(java.awt.event.KeyEvent evt) {//GEN-FIRST:event_nombreEmi1KeyReleased
        // TODO add your handling code here:
        
//         char c;
//         if (!Character.isDigit(c) && c!='.' ) {            
//             evt.consume();               
//        }
//          if (ivaTotal.getText().indexOf(".")!=-1&&!Character.isDigit(c) ) {
//                  evt.consume();  }
//        
//        
//      
String numero = nombreEmi1.getText();
       if (numero.length() == 4 ) {  
              veriNumero();
            
             
        }


        
    }//GEN-LAST:event_nombreEmi1KeyReleased

    private void nombreEmi1KeyTyped(java.awt.event.KeyEvent evt) {//GEN-FIRST:event_nombreEmi1KeyTyped
        // TODO add your handling code here:     
    String numero = nombreEmi1.getText();
        char c=evt.getKeyChar(); 
         if (!Character.isDigit(c)&& numero.length() < 4 ) {            
             evt.consume();               
        }
          if (numero.length() > 3 ) {  
              veriNumero();
             evt.consume(); 
             
        }else{
             jButton9.setEnabled(false);
          }
          
    }//GEN-LAST:event_nombreEmi1KeyTyped

    private void diaCompraFocusGained(java.awt.event.FocusEvent evt) {//GEN-FIRST:event_diaCompraFocusGained
        // TODO add your handling code here:
        diaCompra.setText("");
    }//GEN-LAST:event_diaCompraFocusGained

    private void diaCompraFocusLost(java.awt.event.FocusEvent evt) {//GEN-FIRST:event_diaCompraFocusLost
        // TODO add your handling code here:
        if ("".equals(diaCompra.getText())) {
            diaCompra.setText( "1");
        }
        
    }//GEN-LAST:event_diaCompraFocusLost

    private void diaFacturaFocusGained(java.awt.event.FocusEvent evt) {//GEN-FIRST:event_diaFacturaFocusGained
        // TODO add your handling code here:
        diaFactura.setText("");
        
    }//GEN-LAST:event_diaFacturaFocusGained

    private void diaFacturaFocusLost(java.awt.event.FocusEvent evt) {//GEN-FIRST:event_diaFacturaFocusLost
        // TODO add your handling code here:
        if ("".equals(diaFactura.getText())) {
            diaFactura.setText( "1");
        }
        
        
        
    }//GEN-LAST:event_diaFacturaFocusLost

    private void formWindowClosing(java.awt.event.WindowEvent evt) {//GEN-FIRST:event_formWindowClosing
        // TODO add your handling code here:
    }//GEN-LAST:event_formWindowClosing

    private void formWindowOpened(java.awt.event.WindowEvent evt) {//GEN-FIRST:event_formWindowOpened
        // TODO add your handling code here:
        
        
        mesCompra.setText(String.valueOf(mes));
        mesFactura.setText(String.valueOf(mes));
        añoCompra.setText(String.valueOf(año));
        añoFactura.setText(String.valueOf(año));
                
                
                        
        Stack pila = new Stack();
        pila = mysql.ConsultaCategoria();
        while (pila.size() != 0) {
            tipo.addItem(pila.pop());
        }
        String folio_con = "A001-" + mysql.ConsultaRfcFolio();
            folio.setText(folio_con);
        
        
       
    }//GEN-LAST:event_formWindowOpened

    /**
     * @param args the command line arguments
     */
    
    public String getIpUser() {
        return ipUser;
    }

    public void setIpUser(String ipUser) {
        this.ipUser = ipUser;
    }

    public String getMaquinaUser() {
        return maquinaUser;
    }

    public void setMaquinaUser(String maquinaUser) {
        this.maquinaUser = maquinaUser;
    }

    public String getNombreUser() {
        return nombreUser;
    }

    public void setNombreUser(String nombreUser) {
        this.nombreUser = nombreUser;
    }
    public String getIpRegistro() {
        return ipRegistro;
    }

    public void setIpRegistro(String ipRegistro) {
        this.ipRegistro = ipRegistro;
    }
    public static boolean isFacturas() {
        return facturas;
    }

    public static void setFacturas(boolean facturas) {
        IntAltaFac.facturas = facturas;
    }
     
    // Variables declaration - do not modify//GEN-BEGIN:variables
    private javax.swing.JButton Aceptar_cate;
    private javax.swing.JFrame Emisor;
    private javax.swing.JLabel Etiqueta;
    private javax.swing.JLabel Folio;
    private javax.swing.JLabel Folio1;
    private javax.swing.JLabel Folio10;
    private javax.swing.JLabel Folio11;
    private javax.swing.JLabel Folio12;
    private javax.swing.JLabel Folio13;
    private javax.swing.JLabel Folio14;
    private javax.swing.JLabel Folio15;
    private javax.swing.JLabel Folio16;
    private javax.swing.JLabel Folio17;
    private javax.swing.JLabel Folio18;
    private javax.swing.JLabel Folio2;
    private javax.swing.JLabel Folio3;
    private javax.swing.JLabel Folio4;
    private javax.swing.JLabel Folio5;
    private javax.swing.JLabel Folio6;
    private javax.swing.JLabel Folio7;
    private javax.swing.JLabel Folio8;
    private javax.swing.JLabel Folio9;
    private javax.swing.JLabel Fondo;
    private javax.swing.JLabel Fondo1;
    private javax.swing.JLabel Fondo2;
    private javax.swing.JFrame Ti_pago;
    private javax.swing.JLabel Total1;
    private javax.swing.JTextField añoCompra;
    private javax.swing.JTextField añoFactura;
    private javax.swing.JTextField bi1000i;
    private javax.swing.JTextField bi100i;
    private javax.swing.JTextField bi200i;
    private javax.swing.JTextField bi20i;
    private javax.swing.JTextField bi500i;
    private javax.swing.JTextField bi50i;
    private javax.swing.JCheckBox comFis2;
    private javax.swing.JLabel d05;
    private javax.swing.JLabel d1;
    private javax.swing.JLabel d10;
    private javax.swing.JLabel d100;
    private javax.swing.JLabel d1000;
    private javax.swing.JLabel d2;
    private javax.swing.JLabel d20;
    private javax.swing.JLabel d200;
    private javax.swing.JLabel d5;
    private javax.swing.JLabel d50;
    private javax.swing.JLabel d500;
    private javax.swing.JTextField diaCompra;
    private javax.swing.JTextField diaFactura;
    private javax.swing.JTextField direccionEmi;
    private javax.swing.JTextField estado;
    private javax.swing.JComboBox facturado;
    private javax.swing.JLabel fechaAn;
    private javax.swing.JLabel fechaMo;
    private javax.swing.JTextField folio;
    private javax.swing.JCheckBox iva;
    private javax.swing.JTextField ivaTotal;
    private javax.swing.JButton jButton1;
    private javax.swing.JButton jButton2;
    private javax.swing.JButton jButton3;
    private javax.swing.JButton jButton4;
    private javax.swing.JButton jButton5;
    private javax.swing.JButton jButton6;
    private javax.swing.JButton jButton7;
    private javax.swing.JButton jButton8;
    private javax.swing.JButton jButton9;
    private javax.swing.JComboBox<String> jComboBox1;
    private javax.swing.JFrame jFrame1;
    private javax.swing.JLabel jLabel10;
    private javax.swing.JLabel jLabel11;
    private javax.swing.JLabel jLabel12;
    private javax.swing.JLabel jLabel13;
    private javax.swing.JLabel jLabel14;
    private javax.swing.JLabel jLabel15;
    private javax.swing.JLabel jLabel18;
    private javax.swing.JLabel jLabel19;
    private javax.swing.JLabel jLabel20;
    private javax.swing.JLabel jLabel21;
    private javax.swing.JLabel jLabel22;
    private javax.swing.JLabel jLabel23;
    private javax.swing.JLabel jLabel24;
    private javax.swing.JLabel jLabel26;
    private javax.swing.JLabel jLabel27;
    private javax.swing.JLabel jLabel28;
    private javax.swing.JLabel jLabel30;
    private javax.swing.JLabel jLabel31;
    private javax.swing.JLabel jLabel34;
    private javax.swing.JLabel jLabel35;
    private javax.swing.JLabel jLabel5;
    private javax.swing.JLabel jLabel6;
    private javax.swing.JLabel jLabel7;
    private javax.swing.JLabel jLabel8;
    private javax.swing.JLabel jLabel9;
    private javax.swing.JPanel jPanel10;
    private javax.swing.JPanel jPanel11;
    private javax.swing.JPanel jPanel12;
    private javax.swing.JPanel jPanel14;
    private javax.swing.JPanel jPanel15;
    private javax.swing.JPanel jPanel4;
    private javax.swing.JPanel jPanel5;
    private javax.swing.JPanel jPanel6;
    private javax.swing.JPanel jPanel7;
    private javax.swing.JPanel jPanel9;
    private javax.swing.JTextField jTextField1;
    private javax.swing.JLabel maquinaMo;
    private javax.swing.JTextField mesCompra;
    private javax.swing.JTextField mesFactura;
    private javax.swing.JTextField mo05i;
    private javax.swing.JTextField mo10i;
    private javax.swing.JTextField mo1i;
    private javax.swing.JTextField mo2i;
    private javax.swing.JTextField mo5i;
    private javax.swing.JTextField moComen;
    private javax.swing.JLabel moDife;
    private javax.swing.JLabel monAn;
    private javax.swing.JFrame monedas;
    private javax.swing.JTextField nombre;
    private javax.swing.JTextField nombre1;
    private javax.swing.JTextField nombreEmi;
    private javax.swing.JTextField nombreEmi1;
    private javax.swing.JTextField rfc;
    private javax.swing.JTextField rfcEmi;
    private javax.swing.JTextField subTotal1;
    private javax.swing.JComboBox tipo;
    private javax.swing.JComboBox tipo1;
    private javax.swing.JComboBox tipo2;
    private javax.swing.JComboBox tipoEmi;
    private javax.swing.JComboBox tipoEmi1;
    private javax.swing.JLabel totalCam;
    private javax.swing.JTextField totalFA;
    // End of variables declaration//GEN-END:variables
}