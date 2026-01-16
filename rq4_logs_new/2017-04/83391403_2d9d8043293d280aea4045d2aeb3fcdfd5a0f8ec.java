/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tugas7;

/**
 *
 * @author ASUS
 */
import com.sun.glass.events.KeyEvent;
import java.sql.*;
import javax.swing.*;
import javax.swing.table.DefaultTableModel;
public class FormDataBuku extends javax.swing.JFrame {
private DefaultTableModel model;
private Connection con = koneksi.getConnection();
private Statement stt;
private ResultSet rss;

    /**
     * Creates new form FormDataBuku
     */
    public FormDataBuku() {
        initComponents();
    }
    public void InitTable(){//method untuk menampilkan table 
 model = new DefaultTableModel(); //membuat object table baru
 model.addColumn("Id buku");//menginisiasi atau menentukan colum di object table dengan nama id buku
 model.addColumn("Judul");//menginisiasi atau menentukan colum di object table dengan nama Judul
 model.addColumn("Penulis");//menginisiasi atau menentukan colum di object table dengan nama penulis
  model.addColumn("Harga");//menginisiasi atau menentukan colum di object table dengan nama Harga
 jTable1.setModel(model);////menginisiasi atau menentukan bahwa model jtable1 adalah model object yang telah kita buat di atas

}
public void TampilData(){//method untuk memanggil data
    try {//try jika terjadi error
        String sql = "select * from buku"; //menginisiasi atau menentukan query sql untuk menampilkan semua isi buku
        stt = con.createStatement();//untuk membuat statment mengambil koneksi
        rss = stt.executeQuery(sql);//menjalankan atau mengeksekusi query sql yg telah di buat dengan nama sql
        while(rss.next()){//membuat perulangan untuk menambah data
        Object[] o = new Object[4];//membuat object dari object array yang benama o
        o[0] = rss.getString("id");
        o[1] = rss.getString("judul"); //isi array 2
        o[2] = rss.getString("penulis");//ii array ke 3
        o[3] = rss.getInt("harga");//isi array 4
        model.addRow(o);//fungsi untuk menambahkan row kedalam model jtable
        };
    } catch (SQLException e) {//exception untuk mengatasi error sql
        System.out.println(e.getMessage());//untuk menapilkan error 
    }


};

public void TambahData(String judul,String Penulis,String Harga){//method untuk menambahkan data
    try {//try jika terjadi error
       
    String sql = "INSERT INTO buku VALUES(NULL,'"+judul+"','"+Penulis+"',"+Harga+")"; //menginisiasi atau menentukan query sql untuk menambahkan data
    stt = con.createStatement();//untuk membuat statment mengambil koneksi
    stt.executeUpdate(sql);//  mengeksekusi dan mengupdtae  query sql yg telah di buat dengan nama sql
    } catch (SQLException e) {//exception untuk mengatasi error sql
        System.out.println(e.getMessage());//untuk menapilkan error 
    }


};

public boolean Cekajasih(String judul,String penulis){//method untuk validas
    try {//try jika terjadi error
        String sql = "select * from buku where judul='"+judul+"' and penulis='"+penulis+"';";//menginisiasi atau menentukan query sql untuk memilih data judul dan penulis
        stt = con.createStatement();//untuk membuat statment mengambil koneksi
        rss = stt.executeQuery(sql);//menjalankan atau mengeksekusi query sql yg telah di buat dengan nama sql
        if(rss.next())//jika nilai rss.next benar makan mengembalikan nilai true
        return true;//pemngembalin nilai true
        else//fungsi selainya 
        return false;//pengembalian nilai false
    } catch (SQLException e) {//exception untuk mengatasi error sql
        System.out.println(e.getMessage());//untuk menapilkan error 
        return false;//mengembalikan nilai false
    }
    



};

public void ayokitacari( String by,String cari){//method pencarian
InitTable();//inisiasi table
    try {//try jika terjadi error
        String sql = " select * from buku where "
        +by+" like '%"+cari+"%';";//menginisiasi atau menentukan query sql untuk mecari
        stt = con.createStatement();//untuk membuat statment mengambil koneksi
        rss = stt.executeQuery(sql);//mengeksekus  query sql yg telah di buat dengan nama sql
        while(rss.next()){//perulangan rss dengan fungsi next
            Object[] data = new Object[4];//membuat object dari object array yang benama data
            data[0] = rss.getString("id");//nilai dari data 0 adalah nilai dari id
            data[1] = rss.getString("judul");//nilai dari data 0 adalah nilai dari judul
            data[2] = rss.getString("penulis");//nilai dari data 0 adalah nilai dari penulis
            data[3] = rss.getInt("harga");//nilai dari data 0 adalah nilai dari harga
            model.addRow(data);//fungsi menambhkan data di atas ke dalam model
                    
            
        
     
        }
    } catch (Exception e) {//exception untuk mengatasi error
        System.out.println(e.getMessage());//untuk menapilkan error 
    }

}
public boolean Ubahdong(String id,String judul,String penulis,String harga){//method untuk mengubah data
    try {//try jika terjadi error
        
        String sql = " Update buku set judul='"+judul+"',penulis='"+penulis+"',harga="+
                harga+" where id="+id+";";//menginisiasi atau menentukan query sql untuk mengubah data judul ,penulis dan harga
        stt = con.createStatement();//untuk membuat statment mengambil koneksi
        stt.executeUpdate(sql);//mengeksekus  query sql yg telah di buat dengan nama sql
        return true;//untuk mengembalikan nilai true
    } catch (Exception e) {//exception untuk mengatasi error
        System.out.println(e.getMessage());//untuk menapilkan error 
        return false;//untuk mengembalikan nilai false
    }


}
public boolean hapusindong(String id){//method untuk  menghapus data 
    try {//try untuk mengatasi error
        
        String sql = " delete from buku where id='"+id+"'";//menginisiasi atau menentukan query sql untuk menghapus data dengan merujuk id
         stt = con.createStatement();//untuk membuat statment mengambil koneksi
        stt.executeUpdate(sql);//mengeksekus  query sql yg telah di buat dengan nama sql
        return true;//untuk mengembalikan nilai true
    } catch (Exception e) {//exception untuk mengatasi error
        System.out.println(e.getMessage());//untuk menapilkan error 
        return false;//untuk mengembalikan nilai false
    }


}

    /**
     * This method is called from within the constructor to initialize the form.
     * WARNING: Do NOT modify this code. The content of this method is always
     * regenerated by the Form Editor.
     */
    @SuppressWarnings("unchecked")
    // <editor-fold defaultstate="collapsed" desc="Generated Code">//GEN-BEGIN:initComponents
    private void initComponents() {
        java.awt.GridBagConstraints gridBagConstraints;

        jPanel1 = new javax.swing.JPanel();
        jPanel2 = new javax.swing.JPanel();
        bsimpan = new javax.swing.JButton();
        bubah = new javax.swing.JButton();
        bhapus = new javax.swing.JButton();
        bexit = new javax.swing.JButton();
        jScrollPane1 = new javax.swing.JScrollPane();
        jTable1 = new javax.swing.JTable();
        jPanel3 = new javax.swing.JPanel();
        jTextField1 = new javax.swing.JTextField();
        jLabel1 = new javax.swing.JLabel();
        jComboBox1 = new javax.swing.JComboBox<>();
        jLabel2 = new javax.swing.JLabel();
        jPanel4 = new javax.swing.JPanel();
        jPanel5 = new javax.swing.JPanel();
        jLabel3 = new javax.swing.JLabel();
        txjudul = new javax.swing.JTextField();
        jLabel4 = new javax.swing.JLabel();
        cpenulis = new javax.swing.JComboBox<>();
        jLabel5 = new javax.swing.JLabel();
        txharga = new javax.swing.JTextField();

        setDefaultCloseOperation(javax.swing.WindowConstants.DO_NOTHING_ON_CLOSE);
        addComponentListener(new java.awt.event.ComponentAdapter() {
            public void componentShown(java.awt.event.ComponentEvent evt) {
                formComponentShown(evt);
            }
        });
        addWindowListener(new java.awt.event.WindowAdapter() {
            public void windowClosed(java.awt.event.WindowEvent evt) {
                formWindowClosed(evt);
            }
            public void windowClosing(java.awt.event.WindowEvent evt) {
                formWindowClosing(evt);
            }
        });

        jPanel1.setBackground(new java.awt.Color(255, 51, 51));

        jPanel2.setBackground(new java.awt.Color(0, 0, 0));
        jPanel2.setLayout(new java.awt.GridLayout(1, 0, 10, 0));

        bsimpan.setIcon(new javax.swing.ImageIcon(getClass().getResource("/images/Floppy_disks-512.png"))); // NOI18N
        bsimpan.setText("Simpan");
        bsimpan.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                bsimpanActionPerformed(evt);
            }
        });
        jPanel2.add(bsimpan);

        bubah.setIcon(new javax.swing.ImageIcon(getClass().getResource("/images/document-edit-iconx.png"))); // NOI18N
        bubah.setText("Ubah");
        bubah.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                bubahActionPerformed(evt);
            }
        });
        jPanel2.add(bubah);

        bhapus.setIcon(new javax.swing.ImageIcon(getClass().getResource("/images/Flat_Icon_-_Trash-5z12.png"))); // NOI18N
        bhapus.setText("Hapus");
        bhapus.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                bhapusActionPerformed(evt);
            }
        });
        jPanel2.add(bhapus);

        bexit.setIcon(new javax.swing.ImageIcon(getClass().getResource("/images/close_redl.png"))); // NOI18N
        bexit.setText("Keluar");
        bexit.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                bexitActionPerformed(evt);
            }
        });
        jPanel2.add(bexit);

        jTable1.setModel(new javax.swing.table.DefaultTableModel(
            new Object [][] {
                {null, null, null},
                {null, null, null},
                {null, null, null},
                {null, null, null},
                {null, null, null}
            },
            new String [] {
                "Title 1", "Title 2", "Title 3"
            }
        ));
        jTable1.addMouseListener(new java.awt.event.MouseAdapter() {
            public void mouseClicked(java.awt.event.MouseEvent evt) {
                jTable1MouseClicked(evt);
            }
        });
        jScrollPane1.setViewportView(jTable1);

        jPanel3.setBackground(new java.awt.Color(0, 0, 0));
        jPanel3.setLayout(new java.awt.GridBagLayout());

        jTextField1.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jTextField1ActionPerformed(evt);
            }
        });
        jTextField1.addKeyListener(new java.awt.event.KeyAdapter() {
            public void keyPressed(java.awt.event.KeyEvent evt) {
                jTextField1KeyPressed(evt);
            }
        });
        gridBagConstraints = new java.awt.GridBagConstraints();
        gridBagConstraints.gridx = 1;
        gridBagConstraints.gridy = 0;
        gridBagConstraints.gridheight = 2;
        gridBagConstraints.ipadx = 238;
        gridBagConstraints.anchor = java.awt.GridBagConstraints.NORTHWEST;
        gridBagConstraints.insets = new java.awt.Insets(26, 26, 0, 0);
        jPanel3.add(jTextField1, gridBagConstraints);

        jLabel1.setBackground(new java.awt.Color(255, 255, 255));
        jLabel1.setForeground(new java.awt.Color(255, 255, 255));
        jLabel1.setText("by");
        gridBagConstraints = new java.awt.GridBagConstraints();
        gridBagConstraints.gridx = 2;
        gridBagConstraints.gridy = 0;
        gridBagConstraints.anchor = java.awt.GridBagConstraints.NORTHWEST;
        gridBagConstraints.insets = new java.awt.Insets(29, 18, 0, 0);
        jPanel3.add(jLabel1, gridBagConstraints);

        jComboBox1.setModel(new javax.swing.DefaultComboBoxModel<>(new String[] { "judul", "penulis", "harga" }));
        gridBagConstraints = new java.awt.GridBagConstraints();
        gridBagConstraints.gridx = 3;
        gridBagConstraints.gridy = 0;
        gridBagConstraints.gridheight = 2;
        gridBagConstraints.anchor = java.awt.GridBagConstraints.NORTHWEST;
        gridBagConstraints.insets = new java.awt.Insets(26, 18, 0, 0);
        jPanel3.add(jComboBox1, gridBagConstraints);

        jLabel2.setForeground(new java.awt.Color(255, 255, 255));
        jLabel2.setText("Search");
        gridBagConstraints = new java.awt.GridBagConstraints();
        gridBagConstraints.gridx = 0;
        gridBagConstraints.gridy = 0;
        gridBagConstraints.anchor = java.awt.GridBagConstraints.NORTHWEST;
        gridBagConstraints.insets = new java.awt.Insets(29, 27, 0, 0);
        jPanel3.add(jLabel2, gridBagConstraints);

        jPanel4.setBackground(new java.awt.Color(0, 0, 0));
        jPanel4.setBorder(javax.swing.BorderFactory.createTitledBorder(null, "Form Data Buku", javax.swing.border.TitledBorder.CENTER, javax.swing.border.TitledBorder.TOP, new java.awt.Font("Tahoma", 0, 11), new java.awt.Color(255, 255, 255))); // NOI18N

        jPanel5.setBackground(new java.awt.Color(255, 51, 51));

        jLabel3.setText("Judul");

        jLabel4.setText("Penulis");

        cpenulis.setModel(new javax.swing.DefaultComboBoxModel<>(new String[] { "Tere Liye", "W.S Rendra", "Felix Siauw", "Asma Nadia", "Dewi Lestari" }));

        jLabel5.setText("Harga");

        javax.swing.GroupLayout jPanel5Layout = new javax.swing.GroupLayout(jPanel5);
        jPanel5.setLayout(jPanel5Layout);
        jPanel5Layout.setHorizontalGroup(
            jPanel5Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(jPanel5Layout.createSequentialGroup()
                .addContainerGap()
                .addGroup(jPanel5Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addComponent(jLabel4)
                    .addComponent(jLabel3)
                    .addComponent(jLabel5))
                .addGap(18, 18, 18)
                .addGroup(jPanel5Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.TRAILING, false)
                    .addComponent(cpenulis, javax.swing.GroupLayout.Alignment.LEADING, 0, 197, Short.MAX_VALUE)
                    .addComponent(txharga, javax.swing.GroupLayout.Alignment.LEADING)
                    .addComponent(txjudul))
                .addContainerGap(195, Short.MAX_VALUE))
        );
        jPanel5Layout.setVerticalGroup(
            jPanel5Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(jPanel5Layout.createSequentialGroup()
                .addContainerGap()
                .addGroup(jPanel5Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(jLabel3)
                    .addComponent(txjudul, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
                .addGroup(jPanel5Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(jLabel4)
                    .addComponent(cpenulis, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addGap(18, 18, 18)
                .addGroup(jPanel5Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(jLabel5)
                    .addComponent(txharga, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addContainerGap(36, Short.MAX_VALUE))
        );

        javax.swing.GroupLayout jPanel4Layout = new javax.swing.GroupLayout(jPanel4);
        jPanel4.setLayout(jPanel4Layout);
        jPanel4Layout.setHorizontalGroup(
            jPanel4Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(jPanel4Layout.createSequentialGroup()
                .addContainerGap()
                .addComponent(jPanel5, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                .addContainerGap())
        );
        jPanel4Layout.setVerticalGroup(
            jPanel4Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(jPanel4Layout.createSequentialGroup()
                .addContainerGap()
                .addComponent(jPanel5, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                .addContainerGap())
        );

        javax.swing.GroupLayout jPanel1Layout = new javax.swing.GroupLayout(jPanel1);
        jPanel1.setLayout(jPanel1Layout);
        jPanel1Layout.setHorizontalGroup(
            jPanel1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addComponent(jPanel2, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
            .addComponent(jScrollPane1)
            .addComponent(jPanel3, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
            .addGroup(jPanel1Layout.createSequentialGroup()
                .addGap(24, 24, 24)
                .addComponent(jPanel4, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addContainerGap(javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
        );
        jPanel1Layout.setVerticalGroup(
            jPanel1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(jPanel1Layout.createSequentialGroup()
                .addContainerGap()
                .addComponent(jPanel4, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addGap(44, 44, 44)
                .addComponent(jPanel2, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(jPanel3, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                .addComponent(jScrollPane1, javax.swing.GroupLayout.PREFERRED_SIZE, 236, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addGap(124, 124, 124))
        );

        javax.swing.GroupLayout layout = new javax.swing.GroupLayout(getContentPane());
        getContentPane().setLayout(layout);
        layout.setHorizontalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addComponent(jPanel1, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
        );
        layout.setVerticalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addComponent(jPanel1, javax.swing.GroupLayout.PREFERRED_SIZE, 612, Short.MAX_VALUE)
        );

        pack();
    }// </editor-fold>//GEN-END:initComponents

    private void jTextField1ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jTextField1ActionPerformed
        // TODO add your handling code here:
    }//GEN-LAST:event_jTextField1ActionPerformed

    private void formComponentShown(java.awt.event.ComponentEvent evt) {//GEN-FIRST:event_formComponentShown
        // TODO add your handling code here:
        InitTable();//memanggil method inittable
        TampilData();//memanggil method tampildata
    }//GEN-LAST:event_formComponentShown

    private void bsimpanActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_bsimpanActionPerformed
        // TODO add your handling code here:
        String judul = txjudul.getText(); //fungsi unutk mengambil judul
        String Penulis = cpenulis.getSelectedItem().toString(); //fungsi untuk mengambil penulis
        //Cekajasih(judul);
        String Harga = txharga.getText();//fungsi untuk mengambil harga
        if(Cekajasih(judul, Penulis)){
         JOptionPane.showMessageDialog(null ,"Data yg dimasukan sudah ada");
        }
        else{
        TambahData(judul, Penulis, Harga);//memanggil method tambah data agar data dpt ditambahkan ke database 
        InitTable();//memanggil method imittable
        TampilData();//memanggil method tampildata
        }
    }//GEN-LAST:event_bsimpanActionPerformed

    private void jTextField1KeyPressed(java.awt.event.KeyEvent evt) {//GEN-FIRST:event_jTextField1KeyPressed
        // TODO add your handling code here:
       // if(evt.getKeyCode() == KeyEvent.VK_ENTER){
       InitTable();//untuk meinisiasi table
       if(jTextField1.getText().length()==0){//funsi jika untuk pencarian kosong
       TampilData();//untuk menapilkan data
       }else{
           ayokitacari(jComboBox1.getSelectedItem().toString(), jTextField1.getText());//method pencarian dengan niali dari jcombobox1 dan jtextfield1
       }
        //ayokitacari();
        //}
    }//GEN-LAST:event_jTextField1KeyPressed

    private void bubahActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_bubahActionPerformed
        // TODO add your handling code here:
       int baris=jTable1.getSelectedRow();//menginisiasi baris sebagai fungsi getselected row
       String id = jTable1.getValueAt(baris, 0).toString();// meninisiasi id adalh pengambilan nilai dari baris yg rownya 0
       String judul = txjudul.getText();// meninisiasi judul dengan megambil nilai dari txjudul
       String penulis = cpenulis.getSelectedItem().toString(); //menginisiasi penulis dengan mengambil nilai dari cpenulis
       String harga =  txharga.getText();//menginisiasi harga dengan mengambil nilai dari txharga
       if(Cekajasih(judul, penulis)){//fungsi jika cekajaasih berisi judul dan penulis yg sama maka akan muncul panel 
         JOptionPane.showMessageDialog(null ,"Data sudah ada");//fungsi panel untuk menapilakan data yang anda masukan sudah ada
        }
        else{//jika tidak ada data yang sama akan melanjutkan fungsi dibawah
             if(
               Ubahdong(id, judul, penulis, harga)   ){//funsgi jika untuk merubah data
       JOptionPane.showMessageDialog(null ,"BERHASIL bro");}//panel menapilkan pesan berhasil
       else{
       JOptionPane.showMessageDialog(null ,"GAGAL UBAH Bro");//panel menapilkan pesan gagal
       }    
           }
      InitTable();TampilData();//menginisiasi tabel dan menapilakan data
    }//GEN-LAST:event_bubahActionPerformed

    private void jTable1MouseClicked(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_jTable1MouseClicked
        // TODO add your handling code here:
        int baris=jTable1.getSelectedRow();//fungsi agar table dapat dipilih
        txjudul.setText(jTable1.getValueAt(baris,1).toString());//membuat txjudul bernilai sama dengan baris 1
        cpenulis.setSelectedItem(jTable1.getValueAt(baris, 2).toString());//membuat cpenulsi bernilai sama dengan baris 2
        txharga.setText(jTable1.getValueAt(baris, 3).toString());//membuat txharag bernilai sama dengan baris 3
        
    }//GEN-LAST:event_jTable1MouseClicked

    private void bhapusActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_bhapusActionPerformed
        // TODO add your handling code here:
        int baris=jTable1.getSelectedRow();//fungsi agar table dapat dipilih
         String id = jTable1.getValueAt(baris, 0).toString();//menginisiasi id bernilai baris 0
         if(hapusindong(id)){//fungsi mnghapus
             JOptionPane.showMessageDialog(null ,"BERHASIL HAPUS BOY");}//panel menapilkan pesan berhasil//panel menapilkan pesan berhasil
         else{
       JOptionPane.showMessageDialog(null ,"GAGAL HAPUS BOY");}//panel menapilkan pesan gagal    
      InitTable();TampilData();//menginisiasi tabel dan menapilakan data
    }//GEN-LAST:event_bhapusActionPerformed

    private void formWindowClosed(java.awt.event.WindowEvent evt) {//GEN-FIRST:event_formWindowClosed
        // TODO add your handling code here:
        
    }//GEN-LAST:event_formWindowClosed

    private void bexitActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_bexitActionPerformed
        // TODO add your handling code here:
        int pilihan = JOptionPane.showConfirmDialog(this,"Betulan mau keluar?","Keluar???",JOptionPane.YES_NO_OPTION);//untuk menapilkan pilihan keluar atau tidak
        if (pilihan==0) { //jika pilihan ya maka keluar
            System.exit(0); //fungsi exit
        }
    }//GEN-LAST:event_bexitActionPerformed

    private void formWindowClosing(java.awt.event.WindowEvent evt) {//GEN-FIRST:event_formWindowClosing
        // TODO add your handling code here:
        JOptionPane.showMessageDialog(null ,"Silahkan keluar lewat tombol keluar");//event peringatan untuk hanya keluar lewat tombol keluar
    }//GEN-LAST:event_formWindowClosing

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
            java.util.logging.Logger.getLogger(FormDataBuku.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        } catch (InstantiationException ex) {
            java.util.logging.Logger.getLogger(FormDataBuku.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        } catch (IllegalAccessException ex) {
            java.util.logging.Logger.getLogger(FormDataBuku.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        } catch (javax.swing.UnsupportedLookAndFeelException ex) {
            java.util.logging.Logger.getLogger(FormDataBuku.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        }
        //</editor-fold>
        //</editor-fold>

        /* Create and display the form */
        java.awt.EventQueue.invokeLater(new Runnable() {
            public void run() {
                new FormDataBuku().setVisible(true);
            }
        });
    }

    // Variables declaration - do not modify//GEN-BEGIN:variables
    private javax.swing.JButton bexit;
    private javax.swing.JButton bhapus;
    private javax.swing.JButton bsimpan;
    private javax.swing.JButton bubah;
    private javax.swing.JComboBox<String> cpenulis;
    private javax.swing.JComboBox<String> jComboBox1;
    private javax.swing.JLabel jLabel1;
    private javax.swing.JLabel jLabel2;
    private javax.swing.JLabel jLabel3;
    private javax.swing.JLabel jLabel4;
    private javax.swing.JLabel jLabel5;
    private javax.swing.JPanel jPanel1;
    private javax.swing.JPanel jPanel2;
    private javax.swing.JPanel jPanel3;
    private javax.swing.JPanel jPanel4;
    private javax.swing.JPanel jPanel5;
    private javax.swing.JScrollPane jScrollPane1;
    private javax.swing.JTable jTable1;
    private javax.swing.JTextField jTextField1;
    private javax.swing.JTextField txharga;
    private javax.swing.JTextField txjudul;
    // End of variables declaration//GEN-END:variables
}