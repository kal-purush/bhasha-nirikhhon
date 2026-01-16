package com.softpos.discount;

import com.softpos.pos.core.model.DiscountBean;
import com.softpos.pos.core.model.CuponBean;
import com.softpos.pos.core.model.MemberBean;
import com.softpos.member.MemberControl;
import database.MySQLConnect;
import java.awt.KeyboardFocusManager;
import java.awt.event.KeyEvent;
import java.sql.ResultSet;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Locale;
import javax.swing.JOptionPane;
import com.softpos.main.program.BalanceControl;
import static com.softpos.main.program.BalanceControl.updateProSerTableMemVIP;
import com.softpos.main.program.CouponDiscount;
import com.softpos.main.program.MemberDialog;
import com.softpos.main.program.NumberControl;
import com.softpos.main.program.POSConfigSetup;
import com.softpos.pos.core.model.TableFileBean;
import com.softpos.main.program.TableFileControl;
import com.softpos.main.program.Value;
import java.sql.SQLException;
import java.sql.Statement;
import sun.natee.project.util.NumberFormat;
import sun.natee.project.util.ThaiUtil;
import util.MSG;

public class DiscountDialog extends javax.swing.JDialog {

    private String tableNo;
    private DecimalFormat dec1 = new DecimalFormat("#,##0.00");
    private DiscountBean discBean = null;
    private CuponBean cuponBean = null;
    private POSConfigSetup posConfig = null;
    private double totalAmount;
    private MemberBean memberBean;
    private String Member1;
    private String Member2;

    public DiscountDialog(java.awt.Dialog parent, boolean modal,
            String tableNo, double totalAmount, MemberBean memberBean, String Member1, String Member2) {
        super(parent, modal);
        initComponents();
        jPanel6.setVisible(false);
        jPanel2.setEnabled(false);
        LoadDiscPermis();

//        setBounds(GraphicsEnvironment.getLocalGraphicsEnvironment().getMaximumWindowBounds());
//        setSize(1024, 768);
//        setLocationRelativeTo(null);
        this.memberBean = memberBean;
        this.Member1 = Member1;
        this.Member2 = Member2;
        this.tableNo = tableNo;
        this.totalAmount = totalAmount;

        discBean = new DiscountBean();
        cuponBean = new CuponBean();
        discBean.setCuponBean(cuponBean);

        posConfig = POSConfigSetup.Bean();
        txtMember1.setText(Member1);
        txtMember2.setText(Member2);

        jPanel6.setVisible(false);
        loadDataToText();
    }

    public DiscountBean getDiscountBean() {
        return discBean;
    }

    @SuppressWarnings("unchecked")
    // <editor-fold defaultstate="collapsed" desc="Generated Code">//GEN-BEGIN:initComponents
    private void initComponents() {

        buttonGroup1 = new javax.swing.ButtonGroup();
        jPanel4 = new javax.swing.JPanel();
        jPanel2 = new javax.swing.JPanel();
        btnFest = new javax.swing.JButton();
        btnEmp = new javax.swing.JButton();
        btnMem = new javax.swing.JButton();
        btnTrain = new javax.swing.JButton();
        btnCupon = new javax.swing.JButton();
        txtFest = new javax.swing.JTextField();
        txtFestAmt = new javax.swing.JTextField();
        txtEmp = new javax.swing.JTextField();
        txtEmpAmt = new javax.swing.JTextField();
        txtMem = new javax.swing.JTextField();
        txtMemAmt = new javax.swing.JTextField();
        txtTrain = new javax.swing.JTextField();
        txtTrainAmt = new javax.swing.JTextField();
        txtCupon = new javax.swing.JTextField();
        txtCuponAmt = new javax.swing.JTextField();
        jPanel1 = new javax.swing.JPanel();
        btnBaht = new javax.swing.JButton();
        btnCuponSpecial = new javax.swing.JButton();
        txtBahtAmt = new javax.swing.JTextField();
        txtCuponDiscount = new javax.swing.JTextField();
        jPanel3 = new javax.swing.JPanel();
        jButton10 = new javax.swing.JButton();
        jButton11 = new javax.swing.JButton();
        jButton12 = new javax.swing.JButton();
        jButton13 = new javax.swing.JButton();
        jButton14 = new javax.swing.JButton();
        jButton2 = new javax.swing.JButton();
        jButton3 = new javax.swing.JButton();
        jButton1 = new javax.swing.JButton();
        jButton6 = new javax.swing.JButton();
        jButton5 = new javax.swing.JButton();
        jButton4 = new javax.swing.JButton();
        jButton8 = new javax.swing.JButton();
        jButton9 = new javax.swing.JButton();
        jButton7 = new javax.swing.JButton();
        jButton15 = new javax.swing.JButton();
        jButton16 = new javax.swing.JButton();
        jButton17 = new javax.swing.JButton();
        jPanel5 = new javax.swing.JPanel();
        btnSave = new javax.swing.JButton();
        btnClose = new javax.swing.JButton();
        btnClose1 = new javax.swing.JButton();
        jPanel6 = new javax.swing.JPanel();
        jLabel12 = new javax.swing.JLabel();
        txtMember1 = new javax.swing.JTextField();
        txtMember2 = new javax.swing.JTextField();

        setDefaultCloseOperation(javax.swing.WindowConstants.DISPOSE_ON_CLOSE);
        setTitle("ส่วนลดรายการต่าง ๆ");
        setUndecorated(true);
        addWindowListener(new java.awt.event.WindowAdapter() {
            public void windowOpened(java.awt.event.WindowEvent evt) {
                formWindowOpened(evt);
            }
        });

        jPanel4.setBackground(new java.awt.Color(255, 255, 255));
        jPanel4.setBorder(javax.swing.BorderFactory.createLineBorder(new java.awt.Color(0, 0, 0)));

        jPanel2.setBackground(new java.awt.Color(255, 255, 255));
        jPanel2.setBorder(javax.swing.BorderFactory.createEtchedBorder());
        jPanel2.setMaximumSize(new java.awt.Dimension(500, 550));

        btnFest.setFont(new java.awt.Font("Tahoma", 1, 14)); // NOI18N
        btnFest.setText("(F1) ส่วนลดเทศกาล");
        btnFest.setAlignmentY(0.0F);
        btnFest.setFocusable(false);
        btnFest.setHorizontalAlignment(javax.swing.SwingConstants.LEFT);
        btnFest.setMargin(new java.awt.Insets(0, 5, 0, 0));
        btnFest.setRequestFocusEnabled(false);
        btnFest.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                btnFestActionPerformed(evt);
            }
        });

        btnEmp.setFont(new java.awt.Font("Tahoma", 1, 14)); // NOI18N
        btnEmp.setText("(F2) ส่วนลดพนักงาน");
        btnEmp.setAlignmentY(0.0F);
        btnEmp.setFocusable(false);
        btnEmp.setHorizontalAlignment(javax.swing.SwingConstants.LEFT);
        btnEmp.setMargin(new java.awt.Insets(0, 5, 0, 0));
        btnEmp.setRequestFocusEnabled(false);
        btnEmp.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                btnEmpActionPerformed(evt);
            }
        });

        btnMem.setFont(new java.awt.Font("Tahoma", 1, 14)); // NOI18N
        btnMem.setText("(F3) ส่วนลดสมาชิก (VIP)");
        btnMem.setAlignmentY(0.0F);
        btnMem.setFocusable(false);
        btnMem.setHorizontalAlignment(javax.swing.SwingConstants.LEFT);
        btnMem.setMargin(new java.awt.Insets(0, 5, 0, 0));
        btnMem.setRequestFocusEnabled(false);
        btnMem.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                btnMemActionPerformed(evt);
            }
        });

        btnTrain.setFont(new java.awt.Font("Tahoma", 1, 14)); // NOI18N
        btnTrain.setText("(F4) ส่วนลด Trainee");
        btnTrain.setAlignmentY(0.0F);
        btnTrain.setFocusable(false);
        btnTrain.setHorizontalAlignment(javax.swing.SwingConstants.LEFT);
        btnTrain.setMargin(new java.awt.Insets(0, 5, 0, 0));
        btnTrain.setRequestFocusEnabled(false);
        btnTrain.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                btnTrainActionPerformed(evt);
            }
        });

        btnCupon.setFont(new java.awt.Font("Tahoma", 1, 14)); // NOI18N
        btnCupon.setText("(F5) ส่วนลดคูปอง");
        btnCupon.setAlignmentY(0.0F);
        btnCupon.setFocusable(false);
        btnCupon.setHorizontalAlignment(javax.swing.SwingConstants.LEFT);
        btnCupon.setMargin(new java.awt.Insets(0, 5, 0, 0));
        btnCupon.setRequestFocusEnabled(false);
        btnCupon.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                btnCuponActionPerformed(evt);
            }
        });

        txtFest.setFont(new java.awt.Font("Tahoma", 1, 14)); // NOI18N
        txtFest.setHorizontalAlignment(javax.swing.JTextField.CENTER);
        txtFest.setFocusable(false);
        txtFest.addKeyListener(new java.awt.event.KeyAdapter() {
            public void keyPressed(java.awt.event.KeyEvent evt) {
                txtFestKeyPressed(evt);
            }
        });

        txtFestAmt.setFont(new java.awt.Font("Tahoma", 1, 24)); // NOI18N
        txtFestAmt.setHorizontalAlignment(javax.swing.JTextField.RIGHT);
        txtFestAmt.setText("0.00");
        txtFestAmt.setFocusable(false);
        txtFestAmt.addKeyListener(new java.awt.event.KeyAdapter() {
            public void keyPressed(java.awt.event.KeyEvent evt) {
                txtFestAmtKeyPressed(evt);
            }
        });

        txtEmp.setFont(new java.awt.Font("Tahoma", 1, 14)); // NOI18N
        txtEmp.setHorizontalAlignment(javax.swing.JTextField.CENTER);
        txtEmp.setFocusable(false);
        txtEmp.addKeyListener(new java.awt.event.KeyAdapter() {
            public void keyPressed(java.awt.event.KeyEvent evt) {
                txtEmpKeyPressed(evt);
            }
        });

        txtEmpAmt.setFont(new java.awt.Font("Tahoma", 1, 24)); // NOI18N
        txtEmpAmt.setHorizontalAlignment(javax.swing.JTextField.RIGHT);
        txtEmpAmt.setText("0.00");
        txtEmpAmt.setFocusable(false);

        txtMem.setFont(new java.awt.Font("Tahoma", 1, 14)); // NOI18N
        txtMem.setHorizontalAlignment(javax.swing.JTextField.CENTER);
        txtMem.setFocusable(false);
        txtMem.addKeyListener(new java.awt.event.KeyAdapter() {
            public void keyPressed(java.awt.event.KeyEvent evt) {
                txtMemKeyPressed(evt);
            }
        });

        txtMemAmt.setFont(new java.awt.Font("Tahoma", 1, 24)); // NOI18N
        txtMemAmt.setHorizontalAlignment(javax.swing.JTextField.RIGHT);
        txtMemAmt.setText("0.00");
        txtMemAmt.setFocusable(false);

        txtTrain.setFont(new java.awt.Font("Tahoma", 1, 14)); // NOI18N
        txtTrain.setHorizontalAlignment(javax.swing.JTextField.CENTER);
        txtTrain.setFocusable(false);
        txtTrain.addKeyListener(new java.awt.event.KeyAdapter() {
            public void keyPressed(java.awt.event.KeyEvent evt) {
                txtTrainKeyPressed(evt);
            }
        });

        txtTrainAmt.setFont(new java.awt.Font("Tahoma", 1, 24)); // NOI18N
        txtTrainAmt.setHorizontalAlignment(javax.swing.JTextField.RIGHT);
        txtTrainAmt.setText("0.00");
        txtTrainAmt.setFocusable(false);

        txtCupon.setFont(new java.awt.Font("Tahoma", 1, 14)); // NOI18N
        txtCupon.setHorizontalAlignment(javax.swing.JTextField.CENTER);
        txtCupon.setFocusable(false);
        txtCupon.addKeyListener(new java.awt.event.KeyAdapter() {
            public void keyPressed(java.awt.event.KeyEvent evt) {
                txtCuponKeyPressed(evt);
            }
        });

        txtCuponAmt.setFont(new java.awt.Font("Tahoma", 1, 24)); // NOI18N
        txtCuponAmt.setHorizontalAlignment(javax.swing.JTextField.RIGHT);
        txtCuponAmt.setText("0.00");
        txtCuponAmt.setFocusable(false);

        javax.swing.GroupLayout jPanel2Layout = new javax.swing.GroupLayout(jPanel2);
        jPanel2.setLayout(jPanel2Layout);
        jPanel2Layout.setHorizontalGroup(
            jPanel2Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(jPanel2Layout.createSequentialGroup()
                .addContainerGap()
                .addGroup(jPanel2Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addGroup(jPanel2Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.TRAILING, false)
                        .addComponent(btnEmp, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                        .addComponent(btnMem, javax.swing.GroupLayout.Alignment.LEADING, javax.swing.GroupLayout.DEFAULT_SIZE, 264, Short.MAX_VALUE)
                        .addComponent(btnTrain, javax.swing.GroupLayout.Alignment.LEADING, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                        .addComponent(btnFest, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
                    .addComponent(btnCupon, javax.swing.GroupLayout.PREFERRED_SIZE, 264, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addGap(28, 28, 28)
                .addGroup(jPanel2Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addGroup(jPanel2Layout.createSequentialGroup()
                        .addComponent(txtFest, javax.swing.GroupLayout.PREFERRED_SIZE, 85, javax.swing.GroupLayout.PREFERRED_SIZE)
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                        .addComponent(txtFestAmt, javax.swing.GroupLayout.PREFERRED_SIZE, 101, javax.swing.GroupLayout.PREFERRED_SIZE))
                    .addGroup(jPanel2Layout.createSequentialGroup()
                        .addComponent(txtEmp, javax.swing.GroupLayout.PREFERRED_SIZE, 85, javax.swing.GroupLayout.PREFERRED_SIZE)
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                        .addComponent(txtEmpAmt, javax.swing.GroupLayout.PREFERRED_SIZE, 101, javax.swing.GroupLayout.PREFERRED_SIZE))
                    .addGroup(jPanel2Layout.createSequentialGroup()
                        .addComponent(txtMem, javax.swing.GroupLayout.PREFERRED_SIZE, 85, javax.swing.GroupLayout.PREFERRED_SIZE)
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                        .addComponent(txtMemAmt, javax.swing.GroupLayout.PREFERRED_SIZE, 101, javax.swing.GroupLayout.PREFERRED_SIZE))
                    .addGroup(jPanel2Layout.createSequentialGroup()
                        .addComponent(txtTrain, javax.swing.GroupLayout.PREFERRED_SIZE, 85, javax.swing.GroupLayout.PREFERRED_SIZE)
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                        .addComponent(txtTrainAmt, javax.swing.GroupLayout.PREFERRED_SIZE, 101, javax.swing.GroupLayout.PREFERRED_SIZE))
                    .addGroup(jPanel2Layout.createSequentialGroup()
                        .addComponent(txtCupon, javax.swing.GroupLayout.PREFERRED_SIZE, 85, javax.swing.GroupLayout.PREFERRED_SIZE)
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                        .addComponent(txtCuponAmt, javax.swing.GroupLayout.PREFERRED_SIZE, 101, javax.swing.GroupLayout.PREFERRED_SIZE)))
                .addContainerGap(javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
        );
        jPanel2Layout.setVerticalGroup(
            jPanel2Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(javax.swing.GroupLayout.Alignment.TRAILING, jPanel2Layout.createSequentialGroup()
                .addContainerGap()
                .addGroup(jPanel2Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.TRAILING, false)
                    .addComponent(txtFest, javax.swing.GroupLayout.PREFERRED_SIZE, 50, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(btnFest, javax.swing.GroupLayout.PREFERRED_SIZE, 50, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(txtFestAmt, javax.swing.GroupLayout.PREFERRED_SIZE, 50, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addGroup(jPanel2Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addComponent(txtEmp, javax.swing.GroupLayout.PREFERRED_SIZE, 50, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(btnEmp, javax.swing.GroupLayout.PREFERRED_SIZE, 50, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(txtEmpAmt, javax.swing.GroupLayout.Alignment.TRAILING, javax.swing.GroupLayout.PREFERRED_SIZE, 50, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addGroup(jPanel2Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addComponent(txtMem, javax.swing.GroupLayout.PREFERRED_SIZE, 50, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(btnMem, javax.swing.GroupLayout.PREFERRED_SIZE, 50, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(txtMemAmt, javax.swing.GroupLayout.PREFERRED_SIZE, 50, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addGroup(jPanel2Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addComponent(txtTrain, javax.swing.GroupLayout.PREFERRED_SIZE, 50, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(btnTrain, javax.swing.GroupLayout.PREFERRED_SIZE, 50, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(txtTrainAmt, javax.swing.GroupLayout.Alignment.TRAILING, javax.swing.GroupLayout.PREFERRED_SIZE, 50, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addGroup(jPanel2Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addGroup(jPanel2Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                        .addComponent(txtCupon, javax.swing.GroupLayout.PREFERRED_SIZE, 50, javax.swing.GroupLayout.PREFERRED_SIZE)
                        .addComponent(btnCupon, javax.swing.GroupLayout.PREFERRED_SIZE, 50, javax.swing.GroupLayout.PREFERRED_SIZE))
                    .addComponent(txtCuponAmt, javax.swing.GroupLayout.Alignment.TRAILING, javax.swing.GroupLayout.PREFERRED_SIZE, 50, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addContainerGap())
        );

        jPanel1.setBackground(new java.awt.Color(255, 255, 255));
        jPanel1.setBorder(javax.swing.BorderFactory.createEtchedBorder());

        btnBaht.setFont(new java.awt.Font("Tahoma", 1, 14)); // NOI18N
        btnBaht.setText("(F6) ส่วนลดบาท");
        btnBaht.setAlignmentY(0.0F);
        btnBaht.setFocusable(false);
        btnBaht.setHorizontalAlignment(javax.swing.SwingConstants.LEFT);
        btnBaht.setMargin(new java.awt.Insets(0, 5, 0, 0));
        btnBaht.setRequestFocusEnabled(false);
        btnBaht.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                btnBahtActionPerformed(evt);
            }
        });

        btnCuponSpecial.setFont(new java.awt.Font("Tahoma", 1, 14)); // NOI18N
        btnCuponSpecial.setText("(F7) ส่วนลดบัตรคูปองพิเศษ");
        btnCuponSpecial.setAlignmentY(0.0F);
        btnCuponSpecial.setFocusable(false);
        btnCuponSpecial.setHorizontalAlignment(javax.swing.SwingConstants.LEFT);
        btnCuponSpecial.setMargin(new java.awt.Insets(0, 5, 0, 0));
        btnCuponSpecial.setRequestFocusEnabled(false);
        btnCuponSpecial.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                btnCuponSpecialActionPerformed(evt);
            }
        });

        txtBahtAmt.setFont(new java.awt.Font("Tahoma", 1, 24)); // NOI18N
        txtBahtAmt.setHorizontalAlignment(javax.swing.JTextField.RIGHT);
        txtBahtAmt.setText("0.00");
        txtBahtAmt.setFocusable(false);
        txtBahtAmt.addKeyListener(new java.awt.event.KeyAdapter() {
            public void keyPressed(java.awt.event.KeyEvent evt) {
                txtBahtAmtKeyPressed(evt);
            }
        });

        txtCuponDiscount.setFont(new java.awt.Font("Tahoma", 1, 24)); // NOI18N
        txtCuponDiscount.setHorizontalAlignment(javax.swing.JTextField.RIGHT);
        txtCuponDiscount.setText("0.00");
        txtCuponDiscount.setFocusable(false);

        javax.swing.GroupLayout jPanel1Layout = new javax.swing.GroupLayout(jPanel1);
        jPanel1.setLayout(jPanel1Layout);
        jPanel1Layout.setHorizontalGroup(
            jPanel1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(jPanel1Layout.createSequentialGroup()
                .addContainerGap()
                .addGroup(jPanel1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addComponent(btnCuponSpecial, javax.swing.GroupLayout.PREFERRED_SIZE, 264, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(btnBaht, javax.swing.GroupLayout.PREFERRED_SIZE, 264, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addGap(28, 28, 28)
                .addGroup(jPanel1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addComponent(txtBahtAmt, javax.swing.GroupLayout.PREFERRED_SIZE, 192, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(txtCuponDiscount, javax.swing.GroupLayout.PREFERRED_SIZE, 192, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addContainerGap(javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
        );
        jPanel1Layout.setVerticalGroup(
            jPanel1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(jPanel1Layout.createSequentialGroup()
                .addContainerGap()
                .addGroup(jPanel1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(btnBaht, javax.swing.GroupLayout.PREFERRED_SIZE, 50, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(txtBahtAmt, javax.swing.GroupLayout.PREFERRED_SIZE, 50, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addGap(18, 18, 18)
                .addGroup(jPanel1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(btnCuponSpecial, javax.swing.GroupLayout.PREFERRED_SIZE, 50, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(txtCuponDiscount, javax.swing.GroupLayout.PREFERRED_SIZE, 50, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addContainerGap(13, Short.MAX_VALUE))
        );

        jPanel3.setBackground(new java.awt.Color(255, 255, 255));
        jPanel3.setBorder(javax.swing.BorderFactory.createEtchedBorder());

        jButton10.setFont(new java.awt.Font("Tahoma", 1, 24)); // NOI18N
        jButton10.setText("CE");
        jButton10.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jButton10ActionPerformed(evt);
            }
        });

        jButton11.setFont(new java.awt.Font("Tahoma", 1, 24)); // NOI18N
        jButton11.setText("/");
        jButton11.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jButton11ActionPerformed(evt);
            }
        });

        jButton12.setFont(new java.awt.Font("Tahoma", 1, 24)); // NOI18N
        jButton12.setText("*");
        jButton12.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jButton12ActionPerformed(evt);
            }
        });

        jButton13.setFont(new java.awt.Font("Tahoma", 1, 24)); // NOI18N
        jButton13.setText("-");
        jButton13.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jButton13ActionPerformed(evt);
            }
        });

        jButton14.setFont(new java.awt.Font("Tahoma", 1, 14)); // NOI18N
        jButton14.setText("Enter");
        jButton14.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jButton14ActionPerformed(evt);
            }
        });

        jButton2.setFont(new java.awt.Font("Tahoma", 1, 24)); // NOI18N
        jButton2.setText("9");
        jButton2.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jButton2ActionPerformed(evt);
            }
        });

        jButton3.setFont(new java.awt.Font("Tahoma", 1, 24)); // NOI18N
        jButton3.setText("8");
        jButton3.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jButton3ActionPerformed(evt);
            }
        });

        jButton1.setFont(new java.awt.Font("Tahoma", 1, 24)); // NOI18N
        jButton1.setText("7");
        jButton1.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jButton1ActionPerformed(evt);
            }
        });

        jButton6.setFont(new java.awt.Font("Tahoma", 1, 24)); // NOI18N
        jButton6.setText("4");
        jButton6.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jButton6ActionPerformed(evt);
            }
        });

        jButton5.setFont(new java.awt.Font("Tahoma", 1, 24)); // NOI18N
        jButton5.setText("5");
        jButton5.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jButton5ActionPerformed(evt);
            }
        });

        jButton4.setFont(new java.awt.Font("Tahoma", 1, 24)); // NOI18N
        jButton4.setText("6");
        jButton4.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jButton4ActionPerformed(evt);
            }
        });

        jButton8.setFont(new java.awt.Font("Tahoma", 1, 24)); // NOI18N
        jButton8.setText("3");
        jButton8.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jButton8ActionPerformed(evt);
            }
        });

        jButton9.setFont(new java.awt.Font("Tahoma", 1, 24)); // NOI18N
        jButton9.setText("2");
        jButton9.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jButton9ActionPerformed(evt);
            }
        });

        jButton7.setFont(new java.awt.Font("Tahoma", 1, 24)); // NOI18N
        jButton7.setText("1");
        jButton7.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jButton7ActionPerformed(evt);
            }
        });

        jButton15.setFont(new java.awt.Font("Tahoma", 1, 24)); // NOI18N
        jButton15.setText("0");
        jButton15.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jButton15ActionPerformed(evt);
            }
        });

        jButton16.setFont(new java.awt.Font("Tahoma", 1, 24)); // NOI18N
        jButton16.setText(".");
        jButton16.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jButton16ActionPerformed(evt);
            }
        });

        jButton17.setFont(new java.awt.Font("Tahoma", 1, 24)); // NOI18N
        jButton17.setText("+");
        jButton17.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jButton17ActionPerformed(evt);
            }
        });

        javax.swing.GroupLayout jPanel3Layout = new javax.swing.GroupLayout(jPanel3);
        jPanel3.setLayout(jPanel3Layout);
        jPanel3Layout.setHorizontalGroup(
            jPanel3Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(jPanel3Layout.createSequentialGroup()
                .addContainerGap()
                .addGroup(jPanel3Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING, false)
                    .addGroup(jPanel3Layout.createSequentialGroup()
                        .addComponent(jButton10, javax.swing.GroupLayout.PREFERRED_SIZE, 75, javax.swing.GroupLayout.PREFERRED_SIZE)
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                        .addComponent(jButton11, javax.swing.GroupLayout.PREFERRED_SIZE, 75, javax.swing.GroupLayout.PREFERRED_SIZE)
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                        .addComponent(jButton12, javax.swing.GroupLayout.PREFERRED_SIZE, 75, javax.swing.GroupLayout.PREFERRED_SIZE))
                    .addGroup(jPanel3Layout.createSequentialGroup()
                        .addComponent(jButton7, javax.swing.GroupLayout.PREFERRED_SIZE, 75, javax.swing.GroupLayout.PREFERRED_SIZE)
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                        .addComponent(jButton9, javax.swing.GroupLayout.PREFERRED_SIZE, 75, javax.swing.GroupLayout.PREFERRED_SIZE)
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                        .addComponent(jButton8, javax.swing.GroupLayout.PREFERRED_SIZE, 75, javax.swing.GroupLayout.PREFERRED_SIZE))
                    .addGroup(javax.swing.GroupLayout.Alignment.TRAILING, jPanel3Layout.createSequentialGroup()
                        .addComponent(jButton15, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                        .addComponent(jButton16, javax.swing.GroupLayout.PREFERRED_SIZE, 75, javax.swing.GroupLayout.PREFERRED_SIZE))
                    .addGroup(jPanel3Layout.createSequentialGroup()
                        .addGroup(jPanel3Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                            .addGroup(jPanel3Layout.createSequentialGroup()
                                .addComponent(jButton6, javax.swing.GroupLayout.PREFERRED_SIZE, 75, javax.swing.GroupLayout.PREFERRED_SIZE)
                                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                                .addComponent(jButton5, javax.swing.GroupLayout.PREFERRED_SIZE, 75, javax.swing.GroupLayout.PREFERRED_SIZE))
                            .addGroup(jPanel3Layout.createSequentialGroup()
                                .addComponent(jButton1, javax.swing.GroupLayout.PREFERRED_SIZE, 75, javax.swing.GroupLayout.PREFERRED_SIZE)
                                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                                .addComponent(jButton3, javax.swing.GroupLayout.PREFERRED_SIZE, 75, javax.swing.GroupLayout.PREFERRED_SIZE)))
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                        .addGroup(jPanel3Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                            .addComponent(jButton4, javax.swing.GroupLayout.PREFERRED_SIZE, 75, javax.swing.GroupLayout.PREFERRED_SIZE)
                            .addComponent(jButton2, javax.swing.GroupLayout.PREFERRED_SIZE, 75, javax.swing.GroupLayout.PREFERRED_SIZE))))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addGroup(jPanel3Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING, false)
                    .addComponent(jButton13, javax.swing.GroupLayout.PREFERRED_SIZE, 75, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(jButton17, javax.swing.GroupLayout.PREFERRED_SIZE, 75, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(jButton14, javax.swing.GroupLayout.PREFERRED_SIZE, 75, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addContainerGap(javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
        );
        jPanel3Layout.setVerticalGroup(
            jPanel3Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(jPanel3Layout.createSequentialGroup()
                .addContainerGap()
                .addGroup(jPanel3Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addGroup(jPanel3Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                        .addComponent(jButton10, javax.swing.GroupLayout.PREFERRED_SIZE, 50, javax.swing.GroupLayout.PREFERRED_SIZE)
                        .addComponent(jButton11, javax.swing.GroupLayout.PREFERRED_SIZE, 50, javax.swing.GroupLayout.PREFERRED_SIZE)
                        .addComponent(jButton12, javax.swing.GroupLayout.PREFERRED_SIZE, 50, javax.swing.GroupLayout.PREFERRED_SIZE))
                    .addComponent(jButton13, javax.swing.GroupLayout.PREFERRED_SIZE, 50, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addGroup(jPanel3Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addGroup(jPanel3Layout.createSequentialGroup()
                        .addGroup(jPanel3Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                            .addComponent(jButton1, javax.swing.GroupLayout.PREFERRED_SIZE, 50, javax.swing.GroupLayout.PREFERRED_SIZE)
                            .addComponent(jButton2, javax.swing.GroupLayout.PREFERRED_SIZE, 50, javax.swing.GroupLayout.PREFERRED_SIZE)
                            .addComponent(jButton3, javax.swing.GroupLayout.PREFERRED_SIZE, 50, javax.swing.GroupLayout.PREFERRED_SIZE))
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                        .addGroup(jPanel3Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                            .addComponent(jButton6, javax.swing.GroupLayout.PREFERRED_SIZE, 50, javax.swing.GroupLayout.PREFERRED_SIZE)
                            .addComponent(jButton4, javax.swing.GroupLayout.PREFERRED_SIZE, 50, javax.swing.GroupLayout.PREFERRED_SIZE)
                            .addComponent(jButton5, javax.swing.GroupLayout.PREFERRED_SIZE, 50, javax.swing.GroupLayout.PREFERRED_SIZE)))
                    .addComponent(jButton17, javax.swing.GroupLayout.PREFERRED_SIZE, 106, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addGroup(jPanel3Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING, false)
                    .addGroup(jPanel3Layout.createSequentialGroup()
                        .addGroup(jPanel3Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                            .addComponent(jButton7, javax.swing.GroupLayout.PREFERRED_SIZE, 50, javax.swing.GroupLayout.PREFERRED_SIZE)
                            .addComponent(jButton8, javax.swing.GroupLayout.PREFERRED_SIZE, 50, javax.swing.GroupLayout.PREFERRED_SIZE)
                            .addComponent(jButton9, javax.swing.GroupLayout.PREFERRED_SIZE, 50, javax.swing.GroupLayout.PREFERRED_SIZE))
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                        .addGroup(jPanel3Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                            .addComponent(jButton16, javax.swing.GroupLayout.PREFERRED_SIZE, 50, javax.swing.GroupLayout.PREFERRED_SIZE)
                            .addComponent(jButton15, javax.swing.GroupLayout.PREFERRED_SIZE, 49, javax.swing.GroupLayout.PREFERRED_SIZE)))
                    .addComponent(jButton14, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
                .addContainerGap(javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
        );

        jPanel5.setBackground(new java.awt.Color(255, 255, 255));
        jPanel5.setBorder(javax.swing.BorderFactory.createEtchedBorder());

        btnSave.setFont(new java.awt.Font("Tahoma", 1, 14)); // NOI18N
        btnSave.setText("บันทึก (Save)");
        btnSave.setFocusable(false);
        btnSave.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                btnSaveActionPerformed(evt);
            }
        });

        btnClose.setBackground(new java.awt.Color(255, 204, 102));
        btnClose.setFont(new java.awt.Font("Tahoma", 1, 14)); // NOI18N
        btnClose.setText("ยกเลิกส่วนลด");
        btnClose.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                btnCloseActionPerformed(evt);
            }
        });

        btnClose1.setBackground(new java.awt.Color(204, 0, 51));
        btnClose1.setFont(new java.awt.Font("Tahoma", 1, 14)); // NOI18N
        btnClose1.setText("ออก (Close)");
        btnClose1.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                btnClose1ActionPerformed(evt);
            }
        });

        javax.swing.GroupLayout jPanel5Layout = new javax.swing.GroupLayout(jPanel5);
        jPanel5.setLayout(jPanel5Layout);
        jPanel5Layout.setHorizontalGroup(
            jPanel5Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(javax.swing.GroupLayout.Alignment.TRAILING, jPanel5Layout.createSequentialGroup()
                .addContainerGap(javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                .addGroup(jPanel5Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addComponent(btnClose1, javax.swing.GroupLayout.PREFERRED_SIZE, 177, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addGroup(jPanel5Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING, false)
                        .addComponent(btnSave, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                        .addComponent(btnClose, javax.swing.GroupLayout.PREFERRED_SIZE, 177, javax.swing.GroupLayout.PREFERRED_SIZE)))
                .addGap(74, 74, 74))
        );
        jPanel5Layout.setVerticalGroup(
            jPanel5Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(jPanel5Layout.createSequentialGroup()
                .addGap(5, 5, 5)
                .addComponent(btnSave, javax.swing.GroupLayout.PREFERRED_SIZE, 40, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(btnClose, javax.swing.GroupLayout.PREFERRED_SIZE, 40, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(btnClose1, javax.swing.GroupLayout.PREFERRED_SIZE, 40, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addContainerGap(javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
        );

        jPanel6.setBackground(new java.awt.Color(255, 255, 153));

        jLabel12.setBackground(new java.awt.Color(255, 255, 153));
        jLabel12.setFont(new java.awt.Font("Tahoma", 1, 14)); // NOI18N
        jLabel12.setHorizontalAlignment(javax.swing.SwingConstants.RIGHT);
        jLabel12.setText("ระบบสมาชิก : ");

        txtMember1.setEditable(false);
        txtMember1.setBackground(new java.awt.Color(0, 102, 204));
        txtMember1.setFont(new java.awt.Font("Tahoma", 1, 14)); // NOI18N
        txtMember1.setForeground(new java.awt.Color(255, 255, 255));
        txtMember1.setHorizontalAlignment(javax.swing.JTextField.CENTER);
        txtMember1.setText(" <ท่านไม่ได้ใช้ระบบสมาชิก> ");
        txtMember1.setBorder(javax.swing.BorderFactory.createLineBorder(new java.awt.Color(0, 0, 0)));
        txtMember1.setFocusable(false);
        txtMember1.setRequestFocusEnabled(false);

        txtMember2.setEditable(false);
        txtMember2.setBackground(new java.awt.Color(0, 102, 204));
        txtMember2.setFont(new java.awt.Font("Tahoma", 1, 14)); // NOI18N
        txtMember2.setForeground(new java.awt.Color(255, 255, 255));
        txtMember2.setHorizontalAlignment(javax.swing.JTextField.CENTER);
        txtMember2.setText(": แต้มสะสม 0 คะแนน");
        txtMember2.setBorder(javax.swing.BorderFactory.createLineBorder(new java.awt.Color(0, 0, 0)));

        javax.swing.GroupLayout jPanel6Layout = new javax.swing.GroupLayout(jPanel6);
        jPanel6.setLayout(jPanel6Layout);
        jPanel6Layout.setHorizontalGroup(
            jPanel6Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(jPanel6Layout.createSequentialGroup()
                .addGap(16, 16, 16)
                .addComponent(jLabel12)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(txtMember1, javax.swing.GroupLayout.PREFERRED_SIZE, 204, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addGap(18, 18, 18)
                .addComponent(txtMember2, javax.swing.GroupLayout.DEFAULT_SIZE, 512, Short.MAX_VALUE)
                .addContainerGap())
        );
        jPanel6Layout.setVerticalGroup(
            jPanel6Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(javax.swing.GroupLayout.Alignment.TRAILING, jPanel6Layout.createSequentialGroup()
                .addContainerGap()
                .addGroup(jPanel6Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(txtMember2, javax.swing.GroupLayout.PREFERRED_SIZE, 36, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(txtMember1, javax.swing.GroupLayout.PREFERRED_SIZE, 36, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(jLabel12, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
                .addContainerGap(javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
        );

        javax.swing.GroupLayout jPanel4Layout = new javax.swing.GroupLayout(jPanel4);
        jPanel4.setLayout(jPanel4Layout);
        jPanel4Layout.setHorizontalGroup(
            jPanel4Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(jPanel4Layout.createSequentialGroup()
                .addContainerGap()
                .addGroup(jPanel4Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addGroup(jPanel4Layout.createSequentialGroup()
                        .addGroup(jPanel4Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.TRAILING)
                            .addComponent(jPanel2, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                            .addComponent(jPanel1, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE))
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                        .addGroup(jPanel4Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING, false)
                            .addComponent(jPanel3, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                            .addComponent(jPanel5, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)))
                    .addComponent(jPanel6, javax.swing.GroupLayout.Alignment.TRAILING, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addContainerGap())
        );
        jPanel4Layout.setVerticalGroup(
            jPanel4Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(jPanel4Layout.createSequentialGroup()
                .addContainerGap()
                .addComponent(jPanel6, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addGroup(jPanel4Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addComponent(jPanel2, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(jPanel3, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addGroup(jPanel4Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING, false)
                    .addComponent(jPanel5, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                    .addComponent(jPanel1, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addContainerGap())
        );

        javax.swing.GroupLayout layout = new javax.swing.GroupLayout(getContentPane());
        getContentPane().setLayout(layout);
        layout.setHorizontalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addComponent(jPanel4, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
        );
        layout.setVerticalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(layout.createSequentialGroup()
                .addComponent(jPanel4, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addGap(0, 11, Short.MAX_VALUE))
        );

        pack();
        setLocationRelativeTo(null);
    }// </editor-fold>//GEN-END:initComponents

    private void btnBahtActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_btnBahtActionPerformed
        if (posConfig.getP_DiscBathChk().equals("Y")) {
            txtBahtAmt.setFocusable(true);
            txtBahtAmt.selectAll();
            txtBahtAmt.requestFocus();
        } else {
            JOptionPane.showMessageDialog(this, "ระบบถูกกำหนดว่า ห้ามให้ส่วนลดรายการนี้ !");
        }
    }//GEN-LAST:event_btnBahtActionPerformed

    private void btnCloseActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_btnCloseActionPerformed
        txtFest.setText("");
        txtFestAmt.setText("0.00");
        txtEmp.setText("");
        txtEmpAmt.setText("0.00");
        txtMem.setText("");
        txtMemAmt.setText("0.00");
        txtTrain.setText("");
        txtTrainAmt.setText("0.00");
        txtCupon.setText("");
        txtCuponAmt.setText("0.00");
        txtBahtAmt.setText("0.00");
        txtCuponDiscount.setText("0.00");
        clearCuponSpecail();
        clearMemberDiscount();
        updateCancelDiscountBalanceDiscClick(tableNo);

    }//GEN-LAST:event_btnCloseActionPerformed

    private void btnSaveActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_btnSaveActionPerformed
        double FestDiscount = Double.parseDouble(txtFestAmt.getText().replace(",", ""));
        double EmpDiscount = Double.parseDouble(txtEmpAmt.getText().replace(",", ""));
        double MemDiscount = Double.parseDouble(txtMemAmt.getText().replace(",", ""));
        double TrainDiscount = Double.parseDouble(txtTrainAmt.getText().replace(",", ""));
        double CuponDiscount = Double.parseDouble(txtCuponAmt.getText().replace(",", ""));
        double BahtDiscount = Double.parseDouble(txtBahtAmt.getText().replace(",", ""));
        double CuponSpecialDiscount = Double.parseDouble(txtCuponDiscount.getText().replace(",", ""));

        double totalDiscAll = FestDiscount + EmpDiscount + MemDiscount + TrainDiscount + CuponDiscount + BahtDiscount + CuponSpecialDiscount;
        if (totalDiscAll > totalAmount && totalAmount > 0) {
            MSG.WAR("จำนวนส่วนลดเกินจำนวนเงินรับชำระ กรุณาตรวจสอบ !");
            return;
        }
        discBean.setStrFestDiscount(txtFest.getText().replace("null", "0"));
        discBean.setStrEmpDiscount(txtEmp.getText().replace("null", "0"));
        discBean.setStrMemDiscount(txtMem.getText().replace("null", "0"));
        discBean.setStrTrainDiscount(txtTrain.getText().replace("null", "0"));
        discBean.setStrCuponDiscount(txtCupon.getText().replace("null", "0"));

        discBean.setFestDiscount(FestDiscount);
        discBean.setEmpDiscount(EmpDiscount);
        discBean.setMemDiscount(MemDiscount);
        discBean.setTrainDiscount(TrainDiscount);
        discBean.setCuponDiscount(CuponDiscount);

        discBean.setBahtDiscount(BahtDiscount);
        discBean.setCuponSpecialDiscount(CuponSpecialDiscount);

        /**
         * * OPEN CONNECTION **
         */
        MySQLConnect mysql = new MySQLConnect();
        mysql.open();

        if (FestDiscount > 0) {
            updateDiscountBalanceDiscClick("FestDiscount", tableNo);
        }
        if (EmpDiscount > 0) {
            updateDiscountBalanceDiscClick("EmpDiscount", tableNo);
        }
        if (TrainDiscount > 0) {
            updateDiscountBalanceDiscClick("TrainDiscount", tableNo);
        }
        if (chkInputAmt()) {
            // update tablefile for discount
            try {
                String sql = "update tablefile set "
                        + "EmpDisc='" + discBean.getStrEmpDiscount() + "', "
                        + "EmpDiscAmt='" + discBean.getEmpDiscount() + "',"
                        + "FastDisc='" + discBean.getStrFestDiscount() + "', "
                        + "FastDiscAmt='" + discBean.getFestDiscount() + "',"
                        + "TrainDisc='" + discBean.getStrTrainDiscount() + "', "
                        + "TrainDiscAmt='" + discBean.getTrainDiscount() + "',"
                        + "MemDisc='" + discBean.getStrMemDiscount() + "', "
                        + "MemDiscAmt='" + discBean.getMemDiscount() + "',"
                        + "SubDisc='" + discBean.getStrCuponDiscount() + "', "
                        + "SubDiscAmt='" + discBean.getCuponDiscount() + "',"
                        + "DiscBath='" + discBean.getBahtDiscount() + "',"
                        + "CuponDiscAmt='" + discBean.getCuponSpecialDiscount() + "', "
                        + "NetTotal= NetTotal-'" + totalDiscAll + "' "
                        + "where TCode='" + tableNo + "'";
                Statement stmt = mysql.getConnection().createStatement();
                stmt.executeUpdate(sql);
                if (discBean.getCuponSpecialDiscount() <= 0) {
                    try {
                        //update detail
                        String sqlUpd = "update balance set "
                                + "R_PrCuDisc='0',"
                                + "R_PrCuCode='',"
                                + "R_PrCuQuan='0', "
                                + "R_PrCuType='',"
                                + "R_QuanCanDisc=R_Quan "
                                + "where r_table='" + tableNo + "'";
                        stmt.executeUpdate(sqlUpd);

                    } catch (SQLException e) {
                        MSG.ERR(e.getMessage());
                        e.printStackTrace();
                    }
                }
                if (discBean.getBahtDiscount() > 0) {
                    try {
                        mysql.open();
                        String sqlgetBL = "select count(r_index) r_index from balance where r_table='" + tableNo + "' and r_void<>'V'";
                        ResultSet rsBL = mysql.getConnection().createStatement().executeQuery(sqlgetBL);
                        if (rsBL.next()) {
                            String sqlUpdate = "update balance set r_discbath ='" + discBean.getBahtDiscount() / rsBL.getInt("r_index") + "'";
                            mysql.getConnection().createStatement().executeUpdate(sqlUpdate);
                        }
                        rsBL.close();
                        mysql.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                stmt.close();
                if (discBean.getMemDiscount() > 0) {
                    updateProSerTableMemVIP(tableNo, discBean.getStrMemDiscount());
                    BalanceControl.updateProSerTable(tableNo, memberBean);
                }
                BalanceControl.updateProSerTable(tableNo, memberBean);
            } catch (SQLException e) {
                MSG.ERR(e.getMessage());
                e.printStackTrace();
            } finally {
                mysql.close();
            }

            dispose();
        }
    }//GEN-LAST:event_btnSaveActionPerformed
    private void updateDiscountBalanceDiscClick(String type, String tableNo) {
        String[] data;
        switch (type) {
            case "EmpDiscount":
                //เช็ค %การให้ส่วนลดจากหน้าจอ
                data = txtEmp.getText().replace(",", "").split("/");
                String discForN = "";
                String discForC = "";
                String discForS = "";
                for (String strs : data) {
                    discForN = data[0];
                    discForC = data[1];
                    discForS = data[2];
                }
                try {
                    //เช็คประเภทสินค้า
                    MySQLConnect c = new MySQLConnect();
                    c.open();
                    String[] typeDisc = posConfig.getP_EmpDisc().split("/");
                    double typeNormalN = 0;
                    double typeNormalC = 0;
                    double typeNormalS = 0;
                    for (String data1 : typeDisc) {
                        typeNormalN = Double.parseDouble(typeDisc[0]);
                        typeNormalC = Double.parseDouble(typeDisc[1]);
                        typeNormalS = Double.parseDouble(typeDisc[2]);
                    }

                    String[] saleTypeDiscCheck = posConfig.getP_DiscChkBySaleType().split("/");
                    String saleTypeE = "";
                    String saleTypeT = "";
                    String saleTypeD = "";
                    String saleTypeP = "";
                    String saleTypeW = "";
                    String sql = "";
                    for (String data2 : saleTypeDiscCheck) {
                        saleTypeE = saleTypeDiscCheck[0];
                        if (saleTypeE.equals("Y")) {
                            if (typeNormalN > 0) {
                                sql = "update balance set "
                                        + "R_PRSubType='-E', R_PRSubCode='EMP', R_PRSubQuan='1', "
                                        + "R_PRSubDisc='" + discForN + "', R_PRSubAmt=(R_Total * " + discForN + ")/100 "
                                        + "where r_table='" + tableNo + "' and R_ETD='E' and R_Normal='N' and R_Discount='Y';";
                                c.getConnection().createStatement().executeUpdate(sql);
                            } else if (typeNormalC > 0) {
                                sql = "update balance set "
                                        + "R_PRSubType='-E', R_PRSubCode='EMP', R_PRSubQuan='1', "
                                        + "R_PRSubDisc='" + discForN + "', R_PRSubAmt=(R_Total * " + discForC + ")/100 "
                                        + "where r_table='" + tableNo + "' and R_ETD='E' and R_Normal='C' and R_Discount='Y';";
                                c.getConnection().createStatement().executeUpdate(sql);
                            } else if (typeNormalS > 0) {
                                sql = "update balance set "
                                        + "R_PRSubType='-E', R_PRSubCode='EMP', R_PRSubQuan='1', "
                                        + "R_PRSubDisc='" + discForN + "', R_PRSubAmt=(R_Total * " + discForS + ")/100 "
                                        + "where r_table='" + tableNo + "' and R_ETD='E' and R_Normal='S' and R_Discount='Y';";
                                c.getConnection().createStatement().executeUpdate(sql);
                            }
                        }
                        saleTypeT = saleTypeDiscCheck[1];
                        if (saleTypeT.equals("Y")) {
                            if (saleTypeE.equals("Y")) {
                                if (typeNormalN > 0) {
                                    sql = "update balance set "
                                            + "R_PRSubType='-E', R_PRSubCode='EMP', R_PRSubQuan='1', "
                                            + "R_PRSubDisc='" + discForN + "', R_PRSubAmt=(R_Total * " + discForN + ")/100 "
                                            + "where r_table='" + tableNo + "' and R_ETD='T' and R_Normal='N' and R_Discount='Y';";
                                    c.getConnection().createStatement().executeUpdate(sql);
                                } else if (typeNormalC > 0) {
                                    sql = "update balance set "
                                            + "R_PRSubType='-E', R_PRSubCode='EMP', R_PRSubQuan='1', "
                                            + "R_PRSubDisc='" + discForC + "', R_PRSubAmt=(R_Total * " + discForC + ")/100 "
                                            + "where r_table='" + tableNo + "' and R_ETD='T' and R_Normal='C' and R_Discount='Y';";
                                    c.getConnection().createStatement().executeUpdate(sql);
                                } else if (typeNormalS > 0) {
                                    sql = "update balance set "
                                            + "R_PRSubType='-E', R_PRSubCode='EMP', R_PRSubQuan='1', "
                                            + "R_PRSubDisc='" + discForS + "', R_PRSubAmt=(R_Total * " + discForS + ")/100 "
                                            + "where r_table='" + tableNo + "' and R_ETD='T' and R_Normal='S' and R_Discount='Y';";
                                    c.getConnection().createStatement().executeUpdate(sql);
                                }
                            }
                        }
                        saleTypeD = saleTypeDiscCheck[2];
                        if (saleTypeD.equals("Y")) {
                            if (saleTypeE.equals("Y")) {
                                if (typeNormalN > 0) {
                                    sql = "update balance set "
                                            + "R_PRSubType='-E', R_PRSubCode='EMP', R_PRSubQuan='1', "
                                            + "R_PRSubDisc='" + discForN + "', R_PRSubAmt=(R_Total * " + discForN + ")/100 "
                                            + "where r_table='" + tableNo + "' and R_ETD='D' and R_Normal='N' and R_Discount='Y';";
                                    c.getConnection().createStatement().executeUpdate(sql);
                                } else if (typeNormalC > 0) {
                                    sql = "update balance set "
                                            + "R_PRSubType='-E', R_PRSubCode='EMP', R_PRSubQuan='1', "
                                            + "R_PRSubDisc='" + discForC + "', R_PRSubAmt=(R_Total * " + discForC + ")/100 "
                                            + "where r_table='" + tableNo + "' and R_ETD='D' and R_Normal='C' and R_Discount='Y';";
                                    c.getConnection().createStatement().executeUpdate(sql);
                                } else if (typeNormalS > 0) {
                                    sql = "update balance set "
                                            + "R_PRSubType='-E', R_PRSubCode='EMP', R_PRSubQuan='1', "
                                            + "R_PRSubDisc='" + discForS + "', R_PRSubAmt=(R_Total * " + discForS + ")/100 "
                                            + "where r_table='" + tableNo + "' and R_ETD='D' and R_Normal='S' and R_Discount='Y';";
                                    c.getConnection().createStatement().executeUpdate(sql);
                                }
                            }
                        }
                        saleTypeP = saleTypeDiscCheck[3];
                        if (saleTypeP.equals("Y")) {
                            if (saleTypeE.equals("Y")) {
                                if (typeNormalN > 0) {
                                    sql = "update balance set "
                                            + "R_PRSubType='-E', R_PRSubCode='EMP', R_PRSubQuan='1', "
                                            + "R_PRSubDisc='" + discForN + "', R_PRSubAmt=(R_Total * " + discForN + ")/100 "
                                            + "where r_table='" + tableNo + "' and R_ETD='P' and R_Normal='N' and R_Discount='Y';";
                                    c.getConnection().createStatement().executeUpdate(sql);
                                } else if (typeNormalC > 0) {
                                    sql = "update balance set "
                                            + "R_PRSubType='-E', R_PRSubCode='EMP', R_PRSubQuan='1', "
                                            + "R_PRSubDisc='" + discForC + "', R_PRSubAmt=(R_Total * " + discForC + ")/100 "
                                            + "where r_table='" + tableNo + "' and R_ETD='P' and R_Normal='C' and R_Discount='Y';";
                                    c.getConnection().createStatement().executeUpdate(sql);
                                } else if (typeNormalS > 0) {
                                    sql = "update balance set "
                                            + "R_PRSubType='-E', R_PRSubCode='EMP', R_PRSubQuan='1', "
                                            + "R_PRSubDisc='" + discForS + "', R_PRSubAmt=(R_Total * " + discForS + ")/100 "
                                            + "where r_table='" + tableNo + "' and R_ETD='P' and R_Normal='S' and R_Discount='Y';";
                                    c.getConnection().createStatement().executeUpdate(sql);
                                }
                            }

                        }
                        saleTypeW = saleTypeDiscCheck[4];
                        if (saleTypeW.equals("Y")) {
                            if (saleTypeE.equals("Y")) {
                                if (typeNormalN > 0) {
                                    sql = "update balance set "
                                            + "R_PRSubType='-E', R_PRSubCode='EMP', R_PRSubQuan='1', "
                                            + "R_PRSubDisc='" + discForN + "', R_PRSubAmt=(R_Total * " + discForN + ")/100 "
                                            + "where r_table='" + tableNo + "' and R_ETD='W' and R_Normal='N' and R_Discount='Y';";
                                    c.getConnection().createStatement().executeUpdate(sql);
                                } else if (typeNormalC > 0) {
                                    sql = "update balance set "
                                            + "R_PRSubType='-E', R_PRSubCode='EMP', R_PRSubQuan='1', "
                                            + "R_PRSubDisc='" + discForC + "', R_PRSubAmt=(R_Total * " + discForC + ")/100 "
                                            + "where r_table='" + tableNo + "' and R_ETD='W' and R_Normal='C' and R_Discount='Y';";
                                    c.getConnection().createStatement().executeUpdate(sql);
                                } else if (typeNormalS > 0) {
                                    sql = "update balance set "
                                            + "R_PRSubType='-E', R_PRSubCode='EMP', R_PRSubQuan='1', "
                                            + "R_PRSubDisc='" + discForS + "', R_PRSubAmt=(R_Total * " + discForS + ")/100 "
                                            + "where r_table='" + tableNo + "' and R_ETD='W' and R_Normal='S' and R_Discount='Y';";
                                    c.getConnection().createStatement().executeUpdate(sql);
                                }
                            }
                        }
                    }

                    c.close();
                } catch (Exception e) {
                    MSG.NOTICE(e.toString());
                }
                break;
        }

    }

    private void updateCancelDiscountBalanceDiscClick(String tableNo) {
        try {
            MySQLConnect c = new MySQLConnect();
            c.open();
            String sql = "";
            sql = "update balance set "
                    + "R_PRSubType='', R_PRSubCode='', R_PRSubQuan='0', "
                    + "R_PRSubDisc='0', R_PRSubAmt='0' "
                    + "where r_table='" + tableNo + "' and R_Discount='Y';";
            c.getConnection().createStatement().executeUpdate(sql);
            c.close();
        } catch (Exception e) {
            MSG.NOTICE(e.toString());
        }

    }
    private void btnCuponActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_btnCuponActionPerformed
// check permission  
        String fest = txtFestAmt.getText();
        String emp = txtEmpAmt.getText();
        String mem = txtMemAmt.getText();
        String train = txtTrainAmt.getText();
        String cup = txtCuponAmt.getText();
        CheckDis5();
        if (!fest.equals("0.00") || !emp.equals("0.00") || !mem.equals("0.00") || !train.equals("0.00")) {
            txtCupon.setText("");
            txtCuponAmt.setText("0.00");
            JOptionPane.showMessageDialog(this, "ท่านได้ให้ส่วนลดไปแล้วไม่สามารถให้ส่วนลดได้อีก");

        }
        if (posConfig.getP_SubDiscGet().equals("Y")) {
//            txtCupon.setFocusable(true);
            txtCupon.requestFocus();
            txtCupon.selectAll();
        } else {
            JOptionPane.showMessageDialog(this, "ส่วนลดนี้ยังไม่ได้กำหนดให้เปิดใช้งาน !");
            txtCupon.setText("");
            txtCuponAmt.setText("0.00");
        }
    }//GEN-LAST:event_btnCuponActionPerformed

    private void btnTrainActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_btnTrainActionPerformed
        // check permission    
        String fest = txtFestAmt.getText();
        String emp = txtEmpAmt.getText();
        String mem = txtMemAmt.getText();
        String train = txtTrainAmt.getText();
        String cup = txtCuponAmt.getText();
        CheckDis4();
        if (!fest.equals("0.00") || !emp.equals("0.00") || !mem.equals("0.00") || !cup.equals("0.00")) {
            txtTrain.setText("");
            txtTrainAmt.setText("0.00");
            JOptionPane.showMessageDialog(this, "ท่านได้ให้ส่วนลดไปแล้วไม่สามารถให้ส่วนลดได้อีก");

        }
        if (posConfig.getP_TrainDiscGet().equals("Y")) {
//            txtTrain.setFocusable(true);
            txtTrain.requestFocus();
            txtTrain.selectAll();
        } else {
            JOptionPane.showMessageDialog(this, "ส่วนลดนี้ยังไม่ได้กำหนดให้เปิดใช้งาน !");
            txtTrain.setText("");
            txtTrainAmt.setText("0.00");
        }
    }//GEN-LAST:event_btnTrainActionPerformed

    private void btnMemActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_btnMemActionPerformed
        // check permission 
        String fest = txtFestAmt.getText();
        String emp = txtEmpAmt.getText();
        String mem = txtMemAmt.getText();
        String train = txtTrainAmt.getText();
        String cup = txtCuponAmt.getText();
//        String Member1 = txtMember1.getText();
        if (posConfig.getP_MemDiscGet().equals("Y")) {
            if (!fest.equals("0.00") || !emp.equals("0.00") || !train.equals("0.00") || !cup.equals("0.00") || !mem.equals("0.00")) {
                txtMem.setText("");
                txtMemAmt.setText("0.00");
                JOptionPane.showMessageDialog(this, "ท่านได้ให้ส่วนลดไปแล้วไม่สามารถให้ส่วนลดได้อีก");
            } else {
//                if (Member1.equals("<ท่านไม่ได้ใช้ระบบสมาชิก>") || Member1.equals("")) {
//                    showMember();
                //JOptionPane.showMessageDialog(this, "กรุณาระบุระบบสมาชิก");
//                } else {
                CheckDis3();
                txtMem.setFocusable(true);
                txtMem.requestFocus();
                txtMem.selectAll();
//                if (posConfig.getP_MemDiscGet().equals("Y")) {
//                    txtMem.setFocusable(true);
//                    txtMem.requestFocus();
//                    txtMem.selectAll();
//                }
//                }
            }
        } else {
            JOptionPane.showMessageDialog(this, "ส่วนลดนี้ยังไม่ได้กำหนดให้เปิดใช้งาน !");
            txtMem.setText("");
            txtMemAmt.setText("0.00");
        }
    }//GEN-LAST:event_btnMemActionPerformed

    private void btnEmpActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_btnEmpActionPerformed
        // check permission 
        String fest = txtFestAmt.getText();
        String emp = txtEmpAmt.getText();
        String mem = txtMemAmt.getText();
        String train = txtTrainAmt.getText();
        String cup = txtCuponAmt.getText();
        CheckDis2();
        if (!fest.equals("0.00") || !mem.equals("0.00") || !train.equals("0.00") || !cup.equals("0.00")) {
            txtEmp.setText("");
            txtEmpAmt.setText("0.00");
            JOptionPane.showMessageDialog(this, "ท่านได้ให้ส่วนลดไปแล้วไม่สามารถให้ส่วนลดได้อีก");

        } else {
            if (posConfig.getP_EmpDiscGet().equals("Y")) {
//            txtEmp.setFocusable(true);
                txtEmp.requestFocus();
                txtEmp.selectAll();
            } else {
                JOptionPane.showMessageDialog(this, "ส่วนลดนี้ยังไม่ได้กำหนดให้เปิดใช้งาน !");
                txtEmp.setText("");
                txtEmpAmt.setText("0.00");
            }
        }
    }//GEN-LAST:event_btnEmpActionPerformed

    private void btnFestActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_btnFestActionPerformed
        // check permission 
        String fest = txtFestAmt.getText();
        String emp = txtEmpAmt.getText();
        String mem = txtMemAmt.getText();
        String train = txtTrainAmt.getText();
        String cup = txtCuponAmt.getText();
        CheckDis1();
        if (!emp.equals("0.00") || !mem.equals("0.00") || !train.equals("0.00") || !cup.equals("0.00")) {
            txtFest.setText("");
            txtFestAmt.setText("0.00");
            JOptionPane.showMessageDialog(this, "ท่านได้ให้ส่วนลดไปแล้วไม่สามารถให้ส่วนลดได้อีก");

        } else {
            if (posConfig.getP_FastDiscGet().equals("Y")) {
//            txtFest.setFocusable(true);
                txtFest.requestFocus();
                txtFest.selectAll();
            } else {
                JOptionPane.showMessageDialog(this, "ส่วนลดนี้ยังไม่ได้กำหนดให้เปิดใช้งาน !");
                txtFest.setText("");
                txtFestAmt.setText("0.00");
            }
        }
    }//GEN-LAST:event_btnFestActionPerformed

    private void txtFestKeyPressed(java.awt.event.KeyEvent evt) {//GEN-FIRST:event_txtFestKeyPressed
        if (evt.getKeyCode() == KeyEvent.VK_ENTER) {
            txtFest.setText(nextDiscountFest());
        }
    }//GEN-LAST:event_txtFestKeyPressed

    private void txtEmpKeyPressed(java.awt.event.KeyEvent evt) {//GEN-FIRST:event_txtEmpKeyPressed
        if (evt.getKeyCode() == KeyEvent.VK_ENTER) {
            txtEmp.setText(nextFest());
        }
    }//GEN-LAST:event_txtEmpKeyPressed

    private void txtMemKeyPressed(java.awt.event.KeyEvent evt) {//GEN-FIRST:event_txtMemKeyPressed
        if (evt.getKeyCode() == KeyEvent.VK_ENTER) {
            txtMem.setText(nextMem());
        }
    }//GEN-LAST:event_txtMemKeyPressed

    private void txtTrainKeyPressed(java.awt.event.KeyEvent evt) {//GEN-FIRST:event_txtTrainKeyPressed
        if (evt.getKeyCode() == KeyEvent.VK_ENTER) {
            txtTrain.setText(nextTrainFest());
        }
    }//GEN-LAST:event_txtTrainKeyPressed

    private void txtCuponKeyPressed(java.awt.event.KeyEvent evt) {//GEN-FIRST:event_txtCuponKeyPressed
        if (evt.getKeyCode() == KeyEvent.VK_ENTER) {
            txtCupon.setText(nextSubFest());
        }
    }//GEN-LAST:event_txtCuponKeyPressed

    private void txtBahtAmtKeyPressed(java.awt.event.KeyEvent evt) {//GEN-FIRST:event_txtBahtAmtKeyPressed
        if (evt.getKeyCode() == KeyEvent.VK_ENTER) {
            chkInputAmt();
        }
    }//GEN-LAST:event_txtBahtAmtKeyPressed

    private void jButton10ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jButton10ActionPerformed

    }//GEN-LAST:event_jButton10ActionPerformed

    private void jButton11ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jButton11ActionPerformed
        input("/");
    }//GEN-LAST:event_jButton11ActionPerformed

    private void jButton12ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jButton12ActionPerformed
        input("*");
    }//GEN-LAST:event_jButton12ActionPerformed

    private void jButton13ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jButton13ActionPerformed
        input("-");
    }//GEN-LAST:event_jButton13ActionPerformed

    private void jButton14ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jButton14ActionPerformed

    }//GEN-LAST:event_jButton14ActionPerformed

    private void jButton2ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jButton2ActionPerformed
        input("9");
    }//GEN-LAST:event_jButton2ActionPerformed

    private void jButton3ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jButton3ActionPerformed
        input("8");
    }//GEN-LAST:event_jButton3ActionPerformed

    private void jButton1ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jButton1ActionPerformed
        input("7");
    }//GEN-LAST:event_jButton1ActionPerformed

    private void jButton6ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jButton6ActionPerformed
        input("4");
    }//GEN-LAST:event_jButton6ActionPerformed

    private void jButton5ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jButton5ActionPerformed
        input("5");
    }//GEN-LAST:event_jButton5ActionPerformed

    private void jButton4ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jButton4ActionPerformed
        input("6");
    }//GEN-LAST:event_jButton4ActionPerformed

    private void jButton8ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jButton8ActionPerformed
        input("3");
    }//GEN-LAST:event_jButton8ActionPerformed

    private void jButton9ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jButton9ActionPerformed
        input("2");
    }//GEN-LAST:event_jButton9ActionPerformed

    private void jButton7ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jButton7ActionPerformed
        input("1");
    }//GEN-LAST:event_jButton7ActionPerformed

    private void jButton15ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jButton15ActionPerformed
        input("0");
    }//GEN-LAST:event_jButton15ActionPerformed

    private void jButton16ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jButton16ActionPerformed
        input(".");
    }//GEN-LAST:event_jButton16ActionPerformed

    private void jButton17ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jButton17ActionPerformed
        input("+");
    }//GEN-LAST:event_jButton17ActionPerformed

    private void txtFestAmtKeyPressed(java.awt.event.KeyEvent evt) {//GEN-FIRST:event_txtFestAmtKeyPressed
        if (evt.getKeyCode() == KeyEvent.VK_ENTER) {
            KeyboardFocusManager.getCurrentKeyboardFocusManager().focusNextComponent();
            ReturnFast(txtFestAmt.getText());
        }
    }//GEN-LAST:event_txtFestAmtKeyPressed

    private void formWindowOpened(java.awt.event.WindowEvent evt) {//GEN-FIRST:event_formWindowOpened
        LoadDist();
    }//GEN-LAST:event_formWindowOpened

    private void btnCuponSpecialActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_btnCuponSpecialActionPerformed
        btnCloseActionPerformed(null);
        CouponDiscount cd = new CouponDiscount(null, true, tableNo, Member1, Member2, this.totalAmount);
        cd.setVisible(true);

        BalanceControl.updateProSerTable(tableNo, memberBean);
        loadDataToText();
    }//GEN-LAST:event_btnCuponSpecialActionPerformed

    private void btnClose1ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_btnClose1ActionPerformed
        double FestDiscount = Double.parseDouble(txtFestAmt.getText().replace(",", ""));
        double EmpDiscount = Double.parseDouble(txtEmpAmt.getText().replace(",", ""));
        double MemDiscount = Double.parseDouble(txtMemAmt.getText().replace(",", ""));
        double TrainDiscount = Double.parseDouble(txtTrainAmt.getText().replace(",", ""));
        double CuponDiscount = Double.parseDouble(txtCuponAmt.getText().replace(",", ""));

        double BahtDiscount = Double.parseDouble(txtBahtAmt.getText().replace(",", ""));
        double CuponSpecialDiscount = Double.parseDouble(txtCuponDiscount.getText().replace(",", ""));

        discBean.setStrFestDiscount(txtFest.getText());
        discBean.setStrEmpDiscount(txtEmp.getText());
        discBean.setStrMemDiscount(txtMem.getText());
        discBean.setStrTrainDiscount(txtTrain.getText());
        discBean.setStrCuponDiscount(txtCupon.getText());

        discBean.setFestDiscount(FestDiscount);
        discBean.setEmpDiscount(EmpDiscount);
        discBean.setMemDiscount(MemDiscount);
        discBean.setTrainDiscount(TrainDiscount);
        discBean.setCuponDiscount(CuponDiscount);

        discBean.setBahtDiscount(BahtDiscount);
        discBean.setCuponSpecialDiscount(CuponSpecialDiscount);

        if (chkInputAmt()) {
            // update tablefile for discount

            /**
             * * OPEN CONNECTION **
             */
            MySQLConnect mysql = new MySQLConnect();
            mysql.open();
            try {
                String sql = "update tablefile set "
                        + "EmpDisc='" + discBean.getStrEmpDiscount() + "', EmpDiscAmt='" + discBean.getEmpDiscount() + "',"
                        + "FastDisc='" + discBean.getStrFestDiscount() + "', FastDiscAmt='" + discBean.getFestDiscount() + "',"
                        + "TrainDisc='" + discBean.getStrTrainDiscount() + "', TrainDiscAmt='" + discBean.getTrainDiscount() + "',"
                        + "MemDisc='" + discBean.getStrMemDiscount() + "', MemDiscAmt='" + discBean.getMemDiscount() + "',"
                        + "SubDisc='" + discBean.getStrCuponDiscount() + "', SubDiscAmt='" + discBean.getCuponDiscount() + "',"
                        + "DiscBath='" + discBean.getBahtDiscount() + "',"
                        + "CuponDiscAmt='" + discBean.getCuponSpecialDiscount() + "' "
                        + "where TCode='" + tableNo + "'";
                Statement stmt = mysql.getConnection().createStatement();
                stmt.executeUpdate(sql);
                stmt.close();

                BalanceControl.updateProSerTable(tableNo, memberBean);
            } catch (SQLException e) {
                MSG.ERR(e.getMessage());
                e.printStackTrace();
            } finally {
                mysql.close();
            }

            dispose();
        }
    }//GEN-LAST:event_btnClose1ActionPerformed

    /**
     * @param args the command line arguments
     */
    public static void main(String args[]) {
        new MySQLConnect();
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
            java.util.logging.Logger.getLogger(DiscountDialog.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        } catch (InstantiationException ex) {
            java.util.logging.Logger.getLogger(DiscountDialog.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        } catch (IllegalAccessException ex) {
            java.util.logging.Logger.getLogger(DiscountDialog.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        } catch (javax.swing.UnsupportedLookAndFeelException ex) {
            java.util.logging.Logger.getLogger(DiscountDialog.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        }
        //</editor-fold>

        /* Create and display the dialog */
        java.awt.EventQueue.invokeLater(new Runnable() {
            public void run() {

            }
        });
    }

    // Variables declaration - do not modify//GEN-BEGIN:variables
    private javax.swing.JButton btnBaht;
    private javax.swing.JButton btnClose;
    private javax.swing.JButton btnClose1;
    private javax.swing.JButton btnCupon;
    private javax.swing.JButton btnCuponSpecial;
    private javax.swing.JButton btnEmp;
    private javax.swing.JButton btnFest;
    private javax.swing.JButton btnMem;
    private javax.swing.JButton btnSave;
    private javax.swing.JButton btnTrain;
    private javax.swing.ButtonGroup buttonGroup1;
    private javax.swing.JButton jButton1;
    private javax.swing.JButton jButton10;
    private javax.swing.JButton jButton11;
    private javax.swing.JButton jButton12;
    private javax.swing.JButton jButton13;
    private javax.swing.JButton jButton14;
    private javax.swing.JButton jButton15;
    private javax.swing.JButton jButton16;
    private javax.swing.JButton jButton17;
    private javax.swing.JButton jButton2;
    private javax.swing.JButton jButton3;
    private javax.swing.JButton jButton4;
    private javax.swing.JButton jButton5;
    private javax.swing.JButton jButton6;
    private javax.swing.JButton jButton7;
    private javax.swing.JButton jButton8;
    private javax.swing.JButton jButton9;
    private javax.swing.JLabel jLabel12;
    private javax.swing.JPanel jPanel1;
    private javax.swing.JPanel jPanel2;
    private javax.swing.JPanel jPanel3;
    private javax.swing.JPanel jPanel4;
    private javax.swing.JPanel jPanel5;
    private javax.swing.JPanel jPanel6;
    private javax.swing.JTextField txtBahtAmt;
    private javax.swing.JTextField txtCupon;
    private javax.swing.JTextField txtCuponAmt;
    private javax.swing.JTextField txtCuponDiscount;
    private javax.swing.JTextField txtEmp;
    private javax.swing.JTextField txtEmpAmt;
    private javax.swing.JTextField txtFest;
    private javax.swing.JTextField txtFestAmt;
    private javax.swing.JTextField txtMem;
    private javax.swing.JTextField txtMemAmt;
    private javax.swing.JTextField txtMember1;
    private javax.swing.JTextField txtMember2;
    private javax.swing.JTextField txtTrain;
    private javax.swing.JTextField txtTrainAmt;
    // End of variables declaration//GEN-END:variables

    public String nextDiscountFest() {
        String[] sp = txtFest.getText().split("/");
        String strFestDisc = "00/00/00";

        int DISC_EE = 0;
        int DISC_TT = 0;
        int DISC_DD = 0;
        if (sp.length == 3) {
            DISC_EE = Integer.parseInt(sp[0]);
            DISC_TT = Integer.parseInt(sp[1]);
            DISC_DD = Integer.parseInt(sp[2]);
        } else if (sp.length == 2) {
            DISC_EE = Integer.parseInt(sp[0]);
            DISC_TT = Integer.parseInt(sp[1]);
            DISC_DD = 0;
        } else {
            DISC_EE = Integer.parseInt(sp[0]);
            DISC_TT = 0;
            DISC_DD = 0;
        }

        strFestDisc = DISC_EE + "/" + DISC_TT + "/" + DISC_DD;

        double TOTAL_EE = 0.00;
        double TOTAL_DD = 0.00;
        double TOTAL_TT = 0.00;

        /**
         * * OPEN CONNECTION **
         */
        MySQLConnect mysql = new MySQLConnect();
        mysql.open();
        try {
            String sql = "select R_Normal, sum(R_Total) as R_Total "
                    + "from balance where R_Table='" + tableNo + "' "
                    + "group by R_Normal;";
            Statement stmt = mysql.getConnection().createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            while (rs.next()) {
                String type = rs.getString("R_Normal");
                double total = rs.getDouble("R_Total");

                if (type.equals("N")) {
                    TOTAL_EE = total;
                } else {
                    TOTAL_DD = total;
                }
            }

            rs.close();
            stmt.close();
        } catch (SQLException e) {
            MSG.ERR(e.getMessage());
            e.printStackTrace();
        } finally {
            mysql.close();
        }

        TOTAL_EE = (TOTAL_EE * DISC_EE) / 100;
        TOTAL_TT = (TOTAL_TT * DISC_TT) / 100;
        TOTAL_DD = (TOTAL_DD * DISC_DD) / 100;

        double totalDiscount = TOTAL_EE + TOTAL_TT + TOTAL_DD;

//        txtFestAmt.setFocusable(true);
        txtFestAmt.requestFocus();
        txtFestAmt.setText("" + totalDiscount);
        txtFestAmt.selectAll();

        return strFestDisc;
    }

    public String nextMem() {
        String[] sp = txtMem.getText().split("/");
        String strMemDisc = "0/0/0";

        int DISC_EE = 0;
        int DISC_TT = 0;
        int DISC_DD = 0;

        if (sp.length == 3) {
            DISC_EE = Integer.parseInt(sp[0]);
            DISC_TT = Integer.parseInt(sp[1]);
            DISC_DD = Integer.parseInt(sp[2]);
        } else if (sp.length == 2) {
            DISC_EE = Integer.parseInt(sp[0]);
            DISC_TT = Integer.parseInt(sp[1]);
            DISC_DD = 0;
        } else {
            DISC_EE = Integer.parseInt(sp[0]);
            DISC_TT = 0;
            DISC_DD = 0;
        }
        strMemDisc = DISC_EE + "/" + DISC_TT + "/" + DISC_DD;

        double TOTAL_EE = 0.00;
        double TOTAL_DD = 0.00;
        double TOTAL_TT = 0.00;

        /**
         * * OPEN CONNECTION **
         */
        MySQLConnect mysql = new MySQLConnect();
        mysql.open();
        try {
            String sql = "select R_Normal, sum(R_Total) as R_Total "
                    + "from balance where R_Table='" + tableNo + "' "
                    + "and r_void<>'V' "
                    + "group by R_Normal;";
            Statement stmt = mysql.getConnection().createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            while (rs.next()) {
                String type = rs.getString("R_Normal");
                double total = rs.getDouble("R_Total");

                if (type.equals("N")) {
                    TOTAL_EE = total;
                } else {
                    TOTAL_DD = total;
                }
            }

            rs.close();
            stmt.close();
        } catch (SQLException e) {
            MSG.ERR(e.getMessage());
            e.printStackTrace();
        } finally {
            mysql.close();
        }

        TOTAL_EE = (TOTAL_EE * DISC_EE) / 100;
        TOTAL_TT = (TOTAL_TT * DISC_TT) / 100;
        TOTAL_DD = (TOTAL_DD * DISC_DD) / 100;

        double totalDiscount = TOTAL_EE + TOTAL_TT + TOTAL_DD;

        txtMemAmt.requestFocus();
        txtMemAmt.setText("" + totalDiscount);
        txtMemAmt.selectAll();

        return strMemDisc;
    }

    private String nextTrainFest() {
        String[] sp = txtTrain.getText().split("/");
        String strTrainAmt = "00/00/00";

        int DISC_EE = 0;
        int DISC_TT = 0;
        int DISC_DD = 0;
        if (sp.length == 3) {
            DISC_EE = Integer.parseInt(sp[0]);
            DISC_TT = Integer.parseInt(sp[1]);
            DISC_DD = Integer.parseInt(sp[2]);
        } else if (sp.length == 2) {
            DISC_EE = Integer.parseInt(sp[0]);
            DISC_TT = Integer.parseInt(sp[1]);
            DISC_DD = 0;
        } else {
            DISC_EE = Integer.parseInt(sp[0]);
            DISC_TT = 0;
            DISC_DD = 0;
        }

        strTrainAmt = DISC_EE + "/" + DISC_TT + "/" + DISC_DD;

        double TOTAL_EE = 0.00;
        double TOTAL_DD = 0.00;
        double TOTAL_TT = 0.00;

        /**
         * * OPEN CONNECTION **
         */
        MySQLConnect mysql = new MySQLConnect();
        mysql.open();
        try {
            String sql = "select R_Normal, sum(R_Total) as R_Total "
                    + "from balance where R_Table='" + tableNo + "' "
                    + "group by R_Normal;";
            Statement stmt = mysql.getConnection().createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            while (rs.next()) {
                String type = rs.getString("R_Normal");
                double total = rs.getDouble("R_Total");

                if (type.equals("N")) {
                    TOTAL_EE = total;
                } else {
                    TOTAL_DD = total;
                }
            }

            rs.close();
            stmt.close();
        } catch (SQLException e) {
            MSG.ERR(e.getMessage());
            e.printStackTrace();
        } finally {
            mysql.close();
        }

        TOTAL_EE = (TOTAL_EE * DISC_EE) / 100;
        TOTAL_TT = (TOTAL_TT * DISC_TT) / 100;
        TOTAL_DD = (TOTAL_DD * DISC_DD) / 100;

        double totalDiscount = TOTAL_EE + TOTAL_TT + TOTAL_DD;

//        txtTrainAmt.setFocusable(true);
        txtTrainAmt.requestFocus();
        txtTrainAmt.setText("" + totalDiscount);
        txtTrainAmt.selectAll();

        return strTrainAmt;
    }

    private String nextSubFest() {
        String[] sp = txtCupon.getText().split("/");
        String strCuponAmt = "00/00/00";

        int DISC_EE = 0;
        int DISC_TT = 0;
        int DISC_DD = 0;
        if (sp.length == 3) {
            DISC_EE = Integer.parseInt(sp[0]);
            DISC_TT = Integer.parseInt(sp[1]);
            DISC_DD = Integer.parseInt(sp[2]);
        } else if (sp.length == 2) {
            DISC_EE = Integer.parseInt(sp[0]);
            DISC_TT = Integer.parseInt(sp[1]);
            DISC_DD = 0;
        } else {
            DISC_EE = Integer.parseInt(sp[0]);
            DISC_TT = 0;
            DISC_DD = 0;
        }

        strCuponAmt = DISC_EE + "/" + DISC_TT + "/" + DISC_DD;

        double TOTAL_EE = 0.00;
        double TOTAL_DD = 0.00;
        double TOTAL_TT = 0.00;

        /**
         * * OPEN CONNECTION **
         */
        MySQLConnect mysql = new MySQLConnect();
        mysql.open();
        try {
            String sql = "select R_Normal, sum(R_Total) as R_Total "
                    + "from balance where R_Table='" + tableNo + "' "
                    + "group by R_Normal;";
            Statement stmt = mysql.getConnection().createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            while (rs.next()) {
                String type = rs.getString("R_Normal");
                double total = rs.getDouble("R_Total");

                if (type.equals("N")) {
                    TOTAL_EE = total;
                } else {
                    TOTAL_DD = total;
                }
            }

            rs.close();
            stmt.close();
        } catch (SQLException e) {
            MSG.ERR(e.getMessage());
            e.printStackTrace();
        } finally {
            mysql.close();
        }

        TOTAL_EE = (TOTAL_EE * DISC_EE) / 100;
        TOTAL_TT = (TOTAL_TT * DISC_TT) / 100;
        TOTAL_DD = (TOTAL_DD * DISC_DD) / 100;

        double totalDiscount = TOTAL_EE + TOTAL_TT + TOTAL_DD;

//        txtCuponAmt.setFocusable(true);
        txtCuponAmt.requestFocus();
        txtCuponAmt.setText("" + totalDiscount);
        txtCuponAmt.selectAll();

        return strCuponAmt;
    }

    private boolean chkInputAmt() {
        double input = Double.parseDouble(txtBahtAmt.getText().replace(",", ""));
        double amount = totalAmount;
        int DiscBathMax = posConfig.getP_DiscBathMax();

        double discAllow = (input * DiscBathMax) / 100;
        discAllow = NumberControl.UP_DOWN_NATURAL_BAHT(discAllow);
        if (input > discAllow) {
            // warning
            JOptionPane.showMessageDialog(this, "ยอดส่วนลดมากกว่า % ที่สามารถให้ส่วนลดได้ !");
            txtBahtAmt.setText("0.00");
            txtBahtAmt.selectAll();
            txtBahtAmt.requestFocus();
            return false;
        } else {
            return true;
        }
    }

    private void input(String string) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    private void CheckDis1() {
        /**
         * * OPEN CONNECTION **
         */
        MySQLConnect mysql = new MySQLConnect();
        mysql.open();

        try {
            String sql = "select * from posconfigsetup";
            Statement stmt = mysql.getConnection().createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            while (rs.next()) {
                String P_FastDisc = rs.getString("P_FastDisc");
                txtFest.setText(P_FastDisc);
                nextFest();
            }

            rs.close();
            stmt.close();
        } catch (SQLException e) {
            MSG.ERR(e.getMessage());
            e.printStackTrace();
        } finally {
            mysql.close();
        }
    }

    private void CheckDis2() {
        /**
         * * OPEN CONNECTION **
         */
        MySQLConnect mysql = new MySQLConnect();
        mysql.open();
        try {
            String sql = "select * from posconfigsetup";
            Statement stmt = mysql.getConnection().createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            while (rs.next()) {
                String P_EmpDisc = rs.getString("P_EmpDisc");
                txtEmp.setText(P_EmpDisc);
                nextEmp();
            }

            rs.close();
            stmt.close();
        } catch (SQLException e) {
            MSG.ERR(e.getMessage());
            e.printStackTrace();
        } finally {
            mysql.close();
        }
    }

    private void CheckDis3() {
        /**
         * * OPEN CONNECTION **
         */
        MySQLConnect mysql = new MySQLConnect();
        mysql.open();
        try {
            String sql = "select * from posconfigsetup";
            Statement stmt = mysql.getConnection().createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            while (rs.next()) {
                String P_MemDisc = rs.getString("P_MemDisc");
                txtMem.setText(P_MemDisc);
                nextMem();
            }

            rs.close();
            stmt.close();
        } catch (SQLException e) {
            MSG.ERR(e.getMessage());
            e.printStackTrace();
        } finally {
            mysql.close();
        }
    }

    private void CheckDis4() {
        /**
         * * OPEN CONNECTION **
         */
        MySQLConnect mysql = new MySQLConnect();
        mysql.open();
        try {
            String sql = "select * from posconfigsetup";
            Statement stmt = mysql.getConnection().createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            while (rs.next()) {
                String P_TrainDisc = rs.getString("P_TrainDisc");
                txtTrain.setText(P_TrainDisc);
                nextTrain();
            }

            rs.close();
            stmt.close();
        } catch (SQLException e) {
            MSG.ERR(e.getMessage());
            e.printStackTrace();
        } finally {
            mysql.close();
        }
    }

    private void CheckDis5() {
        /**
         * * OPEN CONNECTION **
         */
        MySQLConnect mysql = new MySQLConnect();
        mysql.open();
        try {
            String sql = "select * from posconfigsetup";
            Statement stmt = mysql.getConnection().createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            while (rs.next()) {
                String P_SubDisc = rs.getString("P_SubDisc");
                txtCupon.setText(P_SubDisc);
                nextSubDisc();
            }

            rs.close();
            stmt.close();
        } catch (SQLException e) {
            MSG.ERR(e.getMessage());
            e.printStackTrace();
        } finally {
            mysql.close();
        }
    }

    private String nextFest() {
        String[] sp = txtFest.getText().split("/");
        String strFestDisc = "0/0/0";

        int DISC_EE = 0;
        int DISC_TT = 0;
        int DISC_DD = 0;

        if (sp.length == 3) {
            DISC_EE = Integer.parseInt(sp[0]);
            DISC_TT = Integer.parseInt(sp[1]);
            DISC_DD = Integer.parseInt(sp[2]);
        } else if (sp.length == 2) {
            DISC_EE = Integer.parseInt(sp[0]);
            DISC_TT = Integer.parseInt(sp[1]);
            DISC_DD = 0;
        } else {
            DISC_EE = Integer.parseInt(sp[0]);
            DISC_TT = 0;
            DISC_DD = 0;
        }
        strFestDisc = DISC_EE + "/" + DISC_TT + "/" + DISC_DD;

        double TOTAL_EE = 0.00;
        double TOTAL_DD = 0.00;
        double TOTAL_TT = 0.00;
        /**
         * * OPEN CONNECTION **
         */
        MySQLConnect mysql = new MySQLConnect();
        mysql.open();
        try {
            String sql = "select R_Normal, sum(R_Total) as R_Total "
                    + "from balance where R_Table='" + tableNo + "' "
                    + "group by R_Normal;";
            Statement stmt = mysql.getConnection().createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            while (rs.next()) {
                String type = rs.getString("R_Normal");
                double total = rs.getDouble("R_Total");

                if (type.equals("N")) {
                    TOTAL_EE = total;
                } else {
                    TOTAL_DD = total;
                }
            }

            rs.close();
            stmt.close();
        } catch (SQLException e) {
            MSG.ERR(e.getMessage());
            e.printStackTrace();
        } finally {
            mysql.close();
        }

        TOTAL_EE = (TOTAL_EE * DISC_EE) / 100;
        TOTAL_TT = (TOTAL_TT * DISC_TT) / 100;
        TOTAL_DD = (TOTAL_DD * DISC_DD) / 100;

        double totalFastDis = TOTAL_EE + TOTAL_TT + TOTAL_DD;

        txtFestAmt.requestFocus();
        txtFestAmt.setText("" + totalFastDis);
        txtFestAmt.selectAll();

        return strFestDisc;
    }

    private void ReturnFast(String RF) {

        if (RF.equals("") || RF.equals("0")) {
            RF = "0.00";
        }
    }

    private String nextEmp() {
        String[] sp = txtEmp.getText().split("/");
        String strEmpDisc = "0/0/0";

        int DISC_EE = 0;
        int DISC_TT = 0;
        int DISC_DD = 0;

        if (sp.length == 3) {
            DISC_EE = Integer.parseInt(sp[0]);
            DISC_TT = Integer.parseInt(sp[1]);
            DISC_DD = Integer.parseInt(sp[2]);
        } else if (sp.length == 2) {
            DISC_EE = Integer.parseInt(sp[0]);
            DISC_TT = Integer.parseInt(sp[1]);
            DISC_DD = 0;
        } else {
            DISC_EE = Integer.parseInt(sp[0]);
            DISC_TT = 0;
            DISC_DD = 0;
        }
        strEmpDisc = DISC_EE + "/" + DISC_TT + "/" + DISC_DD;

        double TOTAL_EE = 0.00;
        double TOTAL_DD = 0.00;
        double TOTAL_TT = 0.00;
        /**
         * * OPEN CONNECTION **
         */
        MySQLConnect mysql = new MySQLConnect();
        mysql.open();
        try {
            String sql = "select R_Normal, sum(R_Total) as R_Total "
                    + "from balance where R_Table='" + tableNo + "' and r_void<>'V' "
                    + "group by R_Normal;";
            Statement stmt = mysql.getConnection().createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            while (rs.next()) {
                String type = rs.getString("R_Normal");
                double total = rs.getDouble("R_Total");

                if (type.equals("N")) {
                    TOTAL_EE = total;
                } else {
                    TOTAL_DD = total;
                }
            }

            rs.close();
            stmt.close();
        } catch (SQLException e) {
            MSG.ERR(e.getMessage());
            e.printStackTrace();
        } finally {
            mysql.close();
        }

        TOTAL_EE = (TOTAL_EE * DISC_EE) / 100;
        TOTAL_TT = (TOTAL_TT * DISC_TT) / 100;
        TOTAL_DD = (TOTAL_DD * DISC_DD) / 100;

        double totalEmpDis = TOTAL_EE + TOTAL_TT + TOTAL_DD;

        txtEmpAmt.requestFocus();
        txtEmpAmt.setText("" + totalEmpDis);
        txtEmpAmt.selectAll();

        return strEmpDisc;
    }

    private void showMember() {
        if (Value.MemberAlready == true) {
            int confirm = JOptionPane.showConfirmDialog(this, "มีการป้อนรหัสสมาชิกไว้แล้วต้องการเปลี่ยนใหม่หรือไม่...?");
            if (confirm == JOptionPane.YES_OPTION) {
                Value.MemberAlready = false;

                /**
                 * * OPEN CONNECTION **
                 */
                MySQLConnect mysql = new MySQLConnect();
                mysql.open();
                try {
                    String sql = "update tablefile set "
                            + "MemDisc='', "
                            + "MemDiscAmt='0.00', "
                            + "MemCode='', "
                            + "MemCurAmt='0.00', "
                            + "MemName='' "
                            + "where TCode='" + tableNo + "'";
                    Statement stmt = mysql.getConnection().createStatement();
                    stmt.executeUpdate(sql);
                    showMember();

                    stmt.close();
                } catch (SQLException e) {
                    MSG.ERR(e.getMessage());
                    e.printStackTrace();
                } finally {
                    mysql.close();
                }
            }
        } else {
            MemberDialog frm = new MemberDialog(null, true, tableNo);
            frm.setVisible(true);

            if (frm.getMemCode() != null) {
                Value.MemberAlready = true;
                memberBean = MemberBean.getMember(frm.getMemCode());
                if (memberBean == null) {
                    memberBean = new MemberBean();
                }
                //String memDisc = memberBean.getMember_DiscountRate();
                txtMember1.setText(memberBean.getMember_Code());
                txtMember2.setText(memberBean.getMember_NameThai());
                CheckDis3();
                // update member in tablefile
                SimpleDateFormat simp = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
                /**
                 * * OPEN CONNECTION **
                 */
                MySQLConnect mysql = new MySQLConnect();
                mysql.open();
                try {
                    String sql = "update tablefile set "
                            + "MemDisc='" + memberBean.getMember_DiscountRate() + "', "
                            + "MemDiscAmt='0.00', "
                            + "MemCode='" + memberBean.getMember_Code() + "', "
                            + "MemCurAmt='" + memberBean.getMember_TotalScore() + "', "
                            + "MemName='" + ThaiUtil.Unicode2ASCII(memberBean.getMember_NameThai()) + "', "
                            + "MemBegin='" + simp.format(memberBean.getMember_Brithday()) + "', "
                            + "MemEnd='" + simp.format(memberBean.getMember_ExpiredDate()) + "' "
                            + "where TCode='" + tableNo + "'";
                    Statement stmt = mysql.getConnection().createStatement();
                    stmt.executeUpdate(sql);
                } catch (SQLException e) {
                    MSG.ERR(e.getMessage());
                    e.printStackTrace();
                } finally {
                    mysql.close();
                }

                // update old order
                MemberControl mc = new MemberControl();
                mc.updateMemberAllBalance(tableNo, memberBean);

                // update all discount and promotion
                BalanceControl.updateProSerTable(tableNo, memberBean);
            }
        }

    }

    private String nextTrain() {
        String[] sp = txtTrain.getText().split("/");
        String strTrainDisc = "0/0/0";

        int DISC_EE = 0;
        int DISC_TT = 0;
        int DISC_DD = 0;

        if (sp.length == 3) {
            DISC_EE = Integer.parseInt(sp[0]);
            DISC_TT = Integer.parseInt(sp[1]);
            DISC_DD = Integer.parseInt(sp[2]);
        } else if (sp.length == 2) {
            DISC_EE = Integer.parseInt(sp[0]);
            DISC_TT = Integer.parseInt(sp[1]);
            DISC_DD = 0;
        } else {
            DISC_EE = Integer.parseInt(sp[0]);
            DISC_TT = 0;
            DISC_DD = 0;
        }
        strTrainDisc = DISC_EE + "/" + DISC_TT + "/" + DISC_DD;

        double TOTAL_EE = 0.00;
        double TOTAL_DD = 0.00;
        double TOTAL_TT = 0.00;
        /**
         * * OPEN CONNECTION **
         */
        MySQLConnect mysql = new MySQLConnect();
        mysql.open();
        try {
            String sql = "select R_Normal, sum(R_Total) as R_Total "
                    + "from balance where R_Table='" + tableNo + "' "
                    + "group by R_Normal;";
            Statement stmt = mysql.getConnection().createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            while (rs.next()) {
                String type = rs.getString("R_Normal");
                double total = rs.getDouble("R_Total");

                if (type.equals("N")) {
                    TOTAL_EE = total;
                } else {
                    TOTAL_DD = total;
                }
            }

            rs.close();
            stmt.close();
        } catch (SQLException e) {
            MSG.ERR(e.getMessage());
            e.printStackTrace();
        } finally {
            mysql.close();
        }

        TOTAL_EE = (TOTAL_EE * DISC_EE) / 100;
        TOTAL_TT = (TOTAL_TT * DISC_TT) / 100;
        TOTAL_DD = (TOTAL_DD * DISC_DD) / 100;

        double totalFastDis = TOTAL_EE + TOTAL_TT + TOTAL_DD;

        txtTrainAmt.requestFocus();
        txtTrainAmt.setText("" + totalFastDis);
        txtTrainAmt.selectAll();

        return strTrainDisc;
    }

    private String nextSubDisc() {
        String[] sp = txtCupon.getText().split("/");
        String strSubDisc = "0/0/0";

        int DISC_EE = 0;
        int DISC_TT = 0;
        int DISC_DD = 0;

        if (sp.length == 3) {
            DISC_EE = Integer.parseInt(sp[0]);
            DISC_TT = Integer.parseInt(sp[1]);
            DISC_DD = Integer.parseInt(sp[2]);
        } else if (sp.length == 2) {
            DISC_EE = Integer.parseInt(sp[0]);
            DISC_TT = Integer.parseInt(sp[1]);
            DISC_DD = 0;
        } else {
            DISC_EE = Integer.parseInt(sp[0]);
            DISC_TT = 0;
            DISC_DD = 0;
        }
        strSubDisc = DISC_EE + "/" + DISC_TT + "/" + DISC_DD;

        double TOTAL_EE = 0.00;
        double TOTAL_DD = 0.00;
        double TOTAL_TT = 0.00;
        /**
         * * OPEN CONNECTION **
         */
        MySQLConnect mysql = new MySQLConnect();
        mysql.open();
        try {
            String sql = "select R_Normal, sum(R_Total) as R_Total "
                    + "from balance where R_Table='" + tableNo + "' "
                    + "group by R_Normal;";
            Statement stmt = mysql.getConnection().createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            while (rs.next()) {
                String type = rs.getString("R_Normal");
                double total = rs.getDouble("R_Total");

                if (type.equals("N")) {
                    TOTAL_EE = total;
                } else {
                    TOTAL_DD = total;
                }
            }

            rs.close();
            stmt.close();
        } catch (SQLException e) {
            MSG.ERR(e.getMessage());
            e.printStackTrace();
        } finally {
            mysql.close();
        }

        TOTAL_EE = (TOTAL_EE * DISC_EE) / 100;
        TOTAL_TT = (TOTAL_TT * DISC_TT) / 100;
        TOTAL_DD = (TOTAL_DD * DISC_DD) / 100;

        double totalFastDis = TOTAL_EE + TOTAL_TT + TOTAL_DD;

        txtCuponAmt.requestFocus();
        txtCuponAmt.setText("" + totalFastDis);
        txtCuponAmt.selectAll();

        return strSubDisc;
    }

    public void LoadDiscPermis() {
        try {
            MySQLConnect c = new MySQLConnect();
            c.open();
            String sql = "select P_MemDiscGet,P_FastDiscGet,P_EmpDiscGet,P_TrainDiscGet,P_SubDiscGet,P_DiscBathChk from posconfigsetup;";
            ResultSet rs = c.getConnection().createStatement().executeQuery(sql);
            if (rs.next()) {
                String MemDiscGet = rs.getString("P_MemDiscGet");
                String FastDiscGet = rs.getString("P_FastDiscGet");
                String EmpDiscGet = rs.getString("P_EmpDiscGet");
                String TrainDiscGet = rs.getString("P_TrainDiscGet");
                String SubDiscGet = rs.getString("P_SubDiscGet");
                String DiscBathChk = rs.getString("P_DiscBathChk");
                if (MemDiscGet.equals("N")) {
                    btnMem.setEnabled(false);
                    txtMemAmt.setEnabled(false);
                } else {
                    btnMem.setEnabled(true);
                    txtMemAmt.setEnabled(true);
                }
                if (FastDiscGet.equals("N")) {
                    btnFest.setEnabled(false);
                    txtFest.setEnabled(false);
                } else {
                    btnFest.setEnabled(true);
                    txtFest.setEnabled(true);
                }
                if (EmpDiscGet.equals("N")) {
                    btnEmp.setEnabled(false);
                    txtEmpAmt.setEnabled(false);

                } else {
                    btnEmp.setEnabled(true);
                    txtEmpAmt.setEnabled(true);
                }
                if (TrainDiscGet.equals("N")) {
                    btnTrain.setEnabled(false);
                    txtTrainAmt.setEnabled(false);
                } else {
                    btnTrain.setEnabled(true);
                    txtTrainAmt.setEnabled(true);
                }
                if (SubDiscGet.equals("N")) {
                    btnCupon.setEnabled(false);
                    txtCuponAmt.setEnabled(false);
                } else {
                    btnCupon.setEnabled(true);
                    txtCuponAmt.setEnabled(true);
                }
                if (DiscBathChk.equals("N")) {
                    btnBaht.setEnabled(false);
                    txtBahtAmt.setEnabled(false);
                } else {
                    btnBaht.setEnabled(true);
                    txtBahtAmt.setEnabled(true);
                }
                c.close();
                rs.close();
            }
        } catch (Exception e) {
        }

    }

    private void LoadDist() {
        /**
         * * OPEN CONNECTION **
         */
        MySQLConnect mysql = new MySQLConnect();
        mysql.open();
        try {
            String sql = "select * from tablefile where Tcode = '" + tableNo + "'";
            Statement stmt = mysql.getConnection().createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            if (rs.next()) {
                String FastDisc = ThaiUtil.ASCII2Unicode(rs.getString("FastDisc"));
                if (FastDisc.equals("0") || FastDisc.equals("")) {
                    txtFest.setText("");
                } else {
                    txtFest.setText(FastDisc);
                }
                String FastDiscAmt = ThaiUtil.ASCII2Unicode(rs.getString("FastDiscAmt"));
                if (FastDiscAmt.equals("0") || FastDiscAmt.equals("")) {
                    txtFestAmt.setText("0.00");
                } else {
                    txtFestAmt.setText(FastDiscAmt);
                }
                String EmpDisc = ThaiUtil.ASCII2Unicode(rs.getString("EmpDisc"));
                if (EmpDisc.equals("0") || EmpDisc.equals("")) {
                    txtEmp.setText("");
                } else {
                    txtEmp.setText(EmpDisc);
                }
                String EmpDiscAmt = ThaiUtil.ASCII2Unicode(rs.getString("EmpDiscAmt"));
                if (EmpDiscAmt.equals("0") || EmpDiscAmt.equals("")) {
                    txtEmpAmt.setText("0.00");
                } else {
                    txtEmpAmt.setText(EmpDiscAmt);
                }
                String MemDisc = ThaiUtil.ASCII2Unicode(rs.getString("MemDisc"));
                if (MemDisc.equals("0") || MemDisc.equals("")) {
                    txtMem.setText("");
                } else {
                    txtMem.setText(MemDisc);
                }
                String MemDiscAmt = ThaiUtil.ASCII2Unicode(rs.getString("MemDiscAmt"));
                if (MemDiscAmt.equals("0") || MemDiscAmt.equals("")) {
                    txtMemAmt.setText("0.00");
                } else {
                    txtMemAmt.setText(MemDiscAmt);
                }
                String TrainDisc = ThaiUtil.ASCII2Unicode(rs.getString("TrainDisc"));
                if (TrainDisc.equals("0")) {
                    txtTrain.setText("");
                } else {
                    txtTrain.setText(TrainDisc);
                }
                String TrainDiscAmt = ThaiUtil.ASCII2Unicode(rs.getString("TrainDiscAmt"));
                if (TrainDiscAmt.equals("0") || TrainDiscAmt.equals("")) {
                    txtTrainAmt.setText("0.00");
                } else {
                    txtTrainAmt.setText(TrainDiscAmt);
                }
                String SubDisc = ThaiUtil.ASCII2Unicode(rs.getString("SubDisc"));
                if (SubDisc.equals("0") || SubDisc.equals("")) {
                    txtCupon.setText("");
                } else {
                    txtCupon.setText(SubDisc);
                }

                String SubDiscAmt = ThaiUtil.ASCII2Unicode(rs.getString("SubDiscAmt"));
                if (SubDiscAmt.equals("0") || SubDiscAmt.equals("")) {
                    txtCuponAmt.setText("0.00");
                } else {
                    txtCuponAmt.setText(SubDiscAmt);
                }
                String DiscBath = ThaiUtil.ASCII2Unicode(rs.getString("DiscBath"));
                if (DiscBath.equals("0") || DiscBath.equals("")) {
                    txtBahtAmt.setText("0.00");
                } else {
                    txtBahtAmt.setText(DiscBath);
                }
            }
        } catch (SQLException e) {
            MSG.ERR(e.getMessage());
            e.printStackTrace();
        } finally {
            mysql.close();
        }
    }

    private void loadDataToText() {
        TableFileControl tfCon = new TableFileControl();
        TableFileBean tBean = tfCon.getData(tableNo);

        txtFestAmt.setText(NumberFormat.showDouble2(tBean.getFastDiscAmt()));
        txtEmpAmt.setText(NumberFormat.showDouble2(tBean.getEmpDiscAmt()));
        txtMemAmt.setText(NumberFormat.showDouble2(tBean.getMemDiscAmt()));
        txtTrainAmt.setText(NumberFormat.showDouble2(tBean.getTrainDiscAmt()));
//        txtCuponAmt.setText(NumberFormat.showDouble2(tBean.getCuponDiscAmt()));
//        txtBahtAmt.setText(NumberFormat.showDouble2(tBean.getCuponDiscAmt()));
//        txtMemAmt.setText(NumberFormat.showDouble2(tBean.getMemDiscAmt()));
        txtCuponDiscount.setText(NumberFormat.showDouble2(tBean.getCuponDiscAmt()));
    }

    private void clearCuponSpecail() {
        /**
         * * OPEN CONNECTION **
         */
        MySQLConnect mysql = new MySQLConnect();
        mysql.open();
        //clear temp cupon
        try {
            String sql = "delete from tempcupon where r_table='" + tableNo + "'";
            Statement stmt = mysql.getConnection().createStatement();
            stmt.executeUpdate(sql);
        } catch (SQLException e) {
            MSG.ERR(e.getMessage());
            e.printStackTrace();
        } finally {
            mysql.close();
        }

        mysql.open();
        try {
            String sql = "select * from temp_balance "
                    + "where r_table='" + tableNo + "'";
            Statement stmt = mysql.getConnection().createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            if (rs.next()) {
                String sql1 = "delete from balance where r_table='" + tableNo + "'";
                String sql2 = "insert into balance select * from temp_balance "
                        + "where r_table='" + tableNo + "' "
                        + "order by r_index";
                String sql3 = "delete from temp_balance where r_table='" + tableNo + "'";
                stmt.executeUpdate(sql1);
                stmt.executeUpdate(sql2);
                stmt.executeUpdate(sql3);

                BalanceControl.updateProSerTable(tableNo, memberBean);
            }
            rs.close();
            stmt.close();
        } catch (SQLException e) {
            MSG.ERR(e.getMessage());
            e.printStackTrace();
        } finally {
            mysql.close();
        }
        try {
            mysql.open();
            String sqlUpdateBalance = "update balance set r_prcuamt='0',r_discbath='0' where r_table='" + tableNo + "'";
            mysql.getConnection().createStatement().executeUpdate(sqlUpdateBalance);
        } catch (Exception e) {
        }
    }

    public void clearMemberDiscount() {
        /**
         * * OPEN CONNECTION **
         */
        MySQLConnect mysql = new MySQLConnect();
        mysql.open();
        //clear temp cupon
        try {
            String sql = "update tablefile set memdisc='',nettotal= nettotal+memdiscamt,"
                    + " memdiscamt='0',memname='',memcode='' where tcode='" + tableNo + "'";
            Statement stmt = mysql.getConnection().createStatement();
            stmt.executeUpdate(sql);
        } catch (SQLException e) {
            MSG.ERR(e.getMessage());
        } finally {
            mysql.close();
        }
        mysql.open();
        try {
            String sqlUpdate = "update balance "
                    + "set "
                    + "r_prsubtype='',"
                    + "r_prsubcode='',"
                    + "r_prsubquan='0',"
                    + "r_prsubdisc='0',"
                    + "r_prsubamt='0'"
                    + " where r_table='" + tableNo + "'";
            mysql.getConnection().createStatement().executeUpdate(sqlUpdate);
        } catch (SQLException e) {
            MSG.ERR(e.getMessage());
            e.printStackTrace();
        } finally {
            mysql.close();
        }
    }
}