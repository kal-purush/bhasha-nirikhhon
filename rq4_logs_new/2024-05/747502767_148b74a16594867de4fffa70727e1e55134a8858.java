package views.list;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.swing.table.DefaultTableModel;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import models.BenhAnModel;
import controllers.BenhAnCtrl;
import views.main.KhamBenhYTa;
import utils.DialogHelper;
import views.main.ThuTien;
import views.main.XepGiuong;

public class DSBenhAnYTa extends javax.swing.JPanel {

    DefaultTableModel tableModel;
    List<BenhAnModel> dsBenhAn = new ArrayList<>();

    public DSBenhAnYTa() {
        try {
            initComponents();

            tableModel = (DefaultTableModel) tblDSBenhAn.getModel();

            hienThiDSBenhAn();
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(DSBenhAnYTa.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private void hienThiDSBenhAn() throws ClassNotFoundException {
        dsBenhAn = BenhAnCtrl.timTatCaBenhAn();
        tableModel.setRowCount(0);

        dsBenhAn.forEach(ba -> {
            tableModel.addRow(new Object[]{ba.getMaBenhAn(), ba.getMaBenhNhan(), ba.getHoTen(),
                ba.getGioiTinh(), ba.getTenPhongKham(), ba.getTenDichVuKham(),
                ba.getNgayKham()});
        });
    }

    @SuppressWarnings("unchecked")
    // <editor-fold defaultstate="collapsed" desc="Generated Code">//GEN-BEGIN:initComponents
    private void initComponents() {

        ChiDinhPanel = new javax.swing.JPanel();
        jLabel19 = new javax.swing.JLabel();
        jPanel3 = new javax.swing.JPanel();
        jLabel2 = new javax.swing.JLabel();
        btnLamMoi = new javax.swing.JButton();
        jPanel4 = new javax.swing.JPanel();
        jLabel3 = new javax.swing.JLabel();
        txtTimKiem = new javax.swing.JTextField();
        jLabel24 = new javax.swing.JLabel();
        jLabel25 = new javax.swing.JLabel();
        jScrollPane1 = new javax.swing.JScrollPane();
        tblDSBenhAn = new javax.swing.JTable();
        tuNgayDateChooser = new com.toedter.calendar.JDateChooser();
        denNgayDateChooser = new com.toedter.calendar.JDateChooser();

        setPreferredSize(new java.awt.Dimension(1100, 503));

        ChiDinhPanel.setBackground(new java.awt.Color(255, 255, 255));
        ChiDinhPanel.setBorder(javax.swing.BorderFactory.createEtchedBorder());
        ChiDinhPanel.setPreferredSize(new java.awt.Dimension(1100, 503));

        jLabel19.setFont(new java.awt.Font("Segoe UI", 0, 14)); // NOI18N
        jLabel19.setText("Tìm kiếm");

        jPanel3.setPreferredSize(new java.awt.Dimension(88, 35));

        jLabel2.setFont(new java.awt.Font("Segoe UI", 1, 12)); // NOI18N
        jLabel2.setText("DANH SÁCH BỆNH ÁN");

        btnLamMoi.setBackground(new java.awt.Color(0, 102, 255));
        btnLamMoi.setFont(new java.awt.Font("Segoe UI", 1, 14)); // NOI18N
        btnLamMoi.setForeground(new java.awt.Color(255, 255, 255));
        btnLamMoi.setText("Làm mới");
        btnLamMoi.setBorder(new javax.swing.border.SoftBevelBorder(javax.swing.border.BevelBorder.RAISED));
        btnLamMoi.setCursor(new java.awt.Cursor(java.awt.Cursor.HAND_CURSOR));
        btnLamMoi.setPreferredSize(new java.awt.Dimension(80, 25));
        btnLamMoi.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                btnLamMoiActionPerformed(evt);
            }
        });

        javax.swing.GroupLayout jPanel3Layout = new javax.swing.GroupLayout(jPanel3);
        jPanel3.setLayout(jPanel3Layout);
        jPanel3Layout.setHorizontalGroup(
            jPanel3Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(jPanel3Layout.createSequentialGroup()
                .addContainerGap()
                .addComponent(jLabel2)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED, 858, Short.MAX_VALUE)
                .addComponent(btnLamMoi, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addGap(30, 30, 30))
        );
        jPanel3Layout.setVerticalGroup(
            jPanel3Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(jPanel3Layout.createSequentialGroup()
                .addGap(8, 8, 8)
                .addGroup(jPanel3Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(jLabel2)
                    .addComponent(btnLamMoi, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addContainerGap(7, Short.MAX_VALUE))
        );

        jPanel4.setPreferredSize(new java.awt.Dimension(88, 35));

        jLabel3.setFont(new java.awt.Font("Segoe UI", 1, 12)); // NOI18N
        jLabel3.setText("TÌM KIẾM BỆNH ÁN");

        javax.swing.GroupLayout jPanel4Layout = new javax.swing.GroupLayout(jPanel4);
        jPanel4.setLayout(jPanel4Layout);
        jPanel4Layout.setHorizontalGroup(
            jPanel4Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(jPanel4Layout.createSequentialGroup()
                .addContainerGap()
                .addComponent(jLabel3)
                .addContainerGap(983, Short.MAX_VALUE))
        );
        jPanel4Layout.setVerticalGroup(
            jPanel4Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(jPanel4Layout.createSequentialGroup()
                .addGap(12, 12, 12)
                .addComponent(jLabel3)
                .addContainerGap(12, Short.MAX_VALUE))
        );

        txtTimKiem.addKeyListener(new java.awt.event.KeyAdapter() {
            public void keyTyped(java.awt.event.KeyEvent evt) {
                txtTimKiemKeyTyped(evt);
            }
        });

        jLabel24.setFont(new java.awt.Font("Segoe UI", 0, 14)); // NOI18N
        jLabel24.setText("Từ ngày");

        jLabel25.setFont(new java.awt.Font("Segoe UI", 0, 14)); // NOI18N
        jLabel25.setText("Đến ngày");

        tblDSBenhAn.setModel(new javax.swing.table.DefaultTableModel(
            new Object [][] {
                {null, null, null, null, null, null, null},
                {null, null, null, null, null, null, null},
                {null, null, null, null, null, null, null},
                {null, null, null, null, null, null, null}
            },
            new String [] {
                "Mã bệnh án", "Mã bệnh nhân", "Họ tên", "Giới tính", "Phòng khám", "Dịch vụ khám", "Ngày khám"
            }
        ) {
            boolean[] canEdit = new boolean [] {
                false, false, false, false, false, false, false
            };

            public boolean isCellEditable(int rowIndex, int columnIndex) {
                return canEdit [columnIndex];
            }
        });
        tblDSBenhAn.setCursor(new java.awt.Cursor(java.awt.Cursor.HAND_CURSOR));
        tblDSBenhAn.addMouseListener(new java.awt.event.MouseAdapter() {
            public void mouseClicked(java.awt.event.MouseEvent evt) {
                tblDSBenhAnMouseClicked(evt);
            }
        });
        jScrollPane1.setViewportView(tblDSBenhAn);

        tuNgayDateChooser.addPropertyChangeListener(new java.beans.PropertyChangeListener() {
            public void propertyChange(java.beans.PropertyChangeEvent evt) {
                tuNgayDateChooserPropertyChange(evt);
            }
        });

        denNgayDateChooser.addPropertyChangeListener(new java.beans.PropertyChangeListener() {
            public void propertyChange(java.beans.PropertyChangeEvent evt) {
                denNgayDateChooserPropertyChange(evt);
            }
        });

        javax.swing.GroupLayout ChiDinhPanelLayout = new javax.swing.GroupLayout(ChiDinhPanel);
        ChiDinhPanel.setLayout(ChiDinhPanelLayout);
        ChiDinhPanelLayout.setHorizontalGroup(
            ChiDinhPanelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(ChiDinhPanelLayout.createSequentialGroup()
                .addGroup(ChiDinhPanelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addComponent(jPanel4, javax.swing.GroupLayout.PREFERRED_SIZE, 1098, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(jPanel3, javax.swing.GroupLayout.PREFERRED_SIZE, 1098, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addGap(0, 0, Short.MAX_VALUE))
            .addComponent(jScrollPane1)
            .addGroup(ChiDinhPanelLayout.createSequentialGroup()
                .addGap(14, 14, 14)
                .addGroup(ChiDinhPanelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING, false)
                    .addComponent(tuNgayDateChooser, javax.swing.GroupLayout.PREFERRED_SIZE, 280, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(jLabel19)
                    .addComponent(txtTimKiem)
                    .addComponent(jLabel24))
                .addGap(164, 164, 164)
                .addGroup(ChiDinhPanelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addComponent(jLabel25)
                    .addComponent(denNgayDateChooser, javax.swing.GroupLayout.PREFERRED_SIZE, 280, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addContainerGap(javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
        );
        ChiDinhPanelLayout.setVerticalGroup(
            ChiDinhPanelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(ChiDinhPanelLayout.createSequentialGroup()
                .addComponent(jPanel4, javax.swing.GroupLayout.PREFERRED_SIZE, 40, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addGap(25, 25, 25)
                .addComponent(jLabel19)
                .addGap(2, 2, 2)
                .addComponent(txtTimKiem, javax.swing.GroupLayout.PREFERRED_SIZE, 25, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addGap(15, 15, 15)
                .addGroup(ChiDinhPanelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(jLabel24)
                    .addComponent(jLabel25))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addGroup(ChiDinhPanelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addGroup(ChiDinhPanelLayout.createSequentialGroup()
                        .addComponent(tuNgayDateChooser, javax.swing.GroupLayout.PREFERRED_SIZE, 25, javax.swing.GroupLayout.PREFERRED_SIZE)
                        .addGap(21, 21, 21)
                        .addComponent(jPanel3, javax.swing.GroupLayout.PREFERRED_SIZE, 40, javax.swing.GroupLayout.PREFERRED_SIZE)
                        .addGap(0, 0, 0)
                        .addComponent(jScrollPane1, javax.swing.GroupLayout.PREFERRED_SIZE, 265, javax.swing.GroupLayout.PREFERRED_SIZE))
                    .addComponent(denNgayDateChooser, javax.swing.GroupLayout.PREFERRED_SIZE, 25, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addContainerGap())
        );

        javax.swing.GroupLayout layout = new javax.swing.GroupLayout(this);
        this.setLayout(layout);
        layout.setHorizontalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addComponent(ChiDinhPanel, javax.swing.GroupLayout.Alignment.TRAILING, javax.swing.GroupLayout.PREFERRED_SIZE, 1102, javax.swing.GroupLayout.PREFERRED_SIZE)
        );
        layout.setVerticalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addComponent(ChiDinhPanel, 505, 505, javax.swing.GroupLayout.PREFERRED_SIZE)
        );
    }// </editor-fold>//GEN-END:initComponents

    private void btnLamMoiActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_btnLamMoiActionPerformed
        try {
            hienThiDSBenhAn();
            txtTimKiem.setText("");
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(DSBenhAnYTa.class.getName()).log(Level.SEVERE, null, ex);
        }
    }//GEN-LAST:event_btnLamMoiActionPerformed

    private void txtTimKiemKeyTyped(java.awt.event.KeyEvent evt) {//GEN-FIRST:event_txtTimKiemKeyTyped
        try {
            String timKiem = txtTimKiem.getText();

            if (timKiem.equals("")) {
                hienThiDSBenhAn();
            } else {
                dsBenhAn = BenhAnCtrl.timBenhAnTheoDK(timKiem);

                tableModel.setRowCount(0);
                dsBenhAn.forEach(ba -> {
                    tableModel.addRow(new Object[]{ba.getMaBenhAn(), ba.getMaBenhNhan(), ba.getHoTen(),
                        ba.getGioiTinh(), ba.getTenPhongKham(), ba.getTenDichVuKham(),
                        ba.getNgayKham()});
                });
            }
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(DSBenhAnYTa.class.getName()).log(Level.SEVERE, null, ex);
        }
    }//GEN-LAST:event_txtTimKiemKeyTyped

    private void denNgayDateChooserPropertyChange(java.beans.PropertyChangeEvent evt) {//GEN-FIRST:event_denNgayDateChooserPropertyChange
        if (tuNgayDateChooser.getDate() != null && denNgayDateChooser.getDate() != null) {
            java.util.Date tuNgayUtil = tuNgayDateChooser.getDate();
            java.util.Date denNgayUtil = denNgayDateChooser.getDate();
            java.sql.Date tuNgay = new java.sql.Date(tuNgayUtil.getTime());
            java.sql.Date denNgay = new java.sql.Date(denNgayUtil.getTime());
            if (tuNgay.equals("")) {
                DialogHelper.showError("Chưa chọn ngày bắt đầu");
            } else if (tuNgayUtil.after(denNgayUtil)) {
                DialogHelper.showError("Từ ngày phải trước đến ngày");
            } else {
                try {
                    dsBenhAn = BenhAnCtrl.timBenhAnTheoThoiGian(tuNgay, denNgay);

                    tableModel.setRowCount(0);
                    dsBenhAn.forEach(ba -> {
                        tableModel.addRow(new Object[]{ba.getMaBenhAn(), ba.getMaBenhNhan(), ba.getHoTen(),
                            ba.getGioiTinh(), ba.getTenPhongKham(), ba.getTenDichVuKham(),
                            ba.getNgayKham()});
                    });
                } catch (ClassNotFoundException ex) {
                    Logger.getLogger(DSBenhAnYTa.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
    }//GEN-LAST:event_denNgayDateChooserPropertyChange

    private void tblDSBenhAnMouseClicked(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_tblDSBenhAnMouseClicked
        int selectedIndex = tblDSBenhAn.getSelectedRow();
        if (selectedIndex >= 0) {
            try {
                BenhAnModel ba = dsBenhAn.get(selectedIndex);

                KhamBenhYTa.Instance.MaBNLabel.setText(ba.getMaBenhNhan());
                KhamBenhYTa.Instance.NameLabel.setText(ba.getHoTen());
                KhamBenhYTa.Instance.SexLabel.setText(ba.getGioiTinh());
                KhamBenhYTa.Instance.RoomLabel.setText(ba.getMaPhongKham() + " " + ba.getTenPhongKham());
                KhamBenhYTa.Instance.MaBALabel.setText(ba.getMaBenhAn());
                KhamBenhYTa.Instance.ServiceLabel.setText(ba.getMaDichVuKham() + " " + ba.getTenDichVuKham());
                SimpleDateFormat inputFormat = new SimpleDateFormat("yyyy-MM-dd");
                SimpleDateFormat outputFormat = new SimpleDateFormat("dd/MM/yyyy");
                Date date = inputFormat.parse(ba.getNgayKham());
                String formattedDate = outputFormat.format(date);
                KhamBenhYTa.Instance.DateLabel.setText(formattedDate);

                ThuTien.Instance.MaBNLabel.setText(ba.getMaBenhNhan());
                ThuTien.Instance.NameLabel.setText(ba.getHoTen());
                ThuTien.Instance.SexLabel.setText(ba.getGioiTinh());
                ThuTien.Instance.MaBALabel.setText(ba.getMaBenhAn());

                //Xep giuong
                XepGiuong.Instance.MaBALabel.setText(ba.getMaBenhAn());
                XepGiuong.Instance.MaBNLabel.setText(ba.getMaBenhNhan());
                XepGiuong.Instance.NameLabel.setText(ba.getHoTen());
            } catch (ParseException ex) {
                Logger.getLogger(DSBenhAnYTa.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }//GEN-LAST:event_tblDSBenhAnMouseClicked

    private void tuNgayDateChooserPropertyChange(java.beans.PropertyChangeEvent evt) {//GEN-FIRST:event_tuNgayDateChooserPropertyChange
        if (tuNgayDateChooser.getDate() != null && denNgayDateChooser.getDate() != null) {
            java.util.Date tuNgayUtil = tuNgayDateChooser.getDate();
            java.util.Date denNgayUtil = denNgayDateChooser.getDate();
            java.sql.Date tuNgay = new java.sql.Date(tuNgayUtil.getTime());
            java.sql.Date denNgay = new java.sql.Date(denNgayUtil.getTime());
            if (tuNgay.equals("")) {
                DialogHelper.showError("Chưa chọn ngày bắt đầu");
            } else if (tuNgayUtil.after(denNgayUtil)) {
                DialogHelper.showError("Từ ngày phải trước đến ngày");
            } else {
                try {
                    dsBenhAn = BenhAnCtrl.timBenhAnTheoThoiGian(tuNgay, denNgay);

                    tableModel.setRowCount(0);
                    dsBenhAn.forEach(ba -> {
                        tableModel.addRow(new Object[]{ba.getMaBenhAn(), ba.getMaBenhNhan(), ba.getHoTen(),
                            ba.getGioiTinh(), ba.getTenPhongKham(), ba.getTenDichVuKham(),
                            ba.getNgayKham()});
                    });
                } catch (ClassNotFoundException ex) {
                    Logger.getLogger(DSBenhAnYTa.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
    }//GEN-LAST:event_tuNgayDateChooserPropertyChange

    // Variables declaration - do not modify//GEN-BEGIN:variables
    private javax.swing.JPanel ChiDinhPanel;
    private javax.swing.JButton btnLamMoi;
    private com.toedter.calendar.JDateChooser denNgayDateChooser;
    private javax.swing.JLabel jLabel19;
    private javax.swing.JLabel jLabel2;
    private javax.swing.JLabel jLabel24;
    private javax.swing.JLabel jLabel25;
    private javax.swing.JLabel jLabel3;
    private javax.swing.JPanel jPanel3;
    private javax.swing.JPanel jPanel4;
    private javax.swing.JScrollPane jScrollPane1;
    private javax.swing.JTable tblDSBenhAn;
    private com.toedter.calendar.JDateChooser tuNgayDateChooser;
    private javax.swing.JTextField txtTimKiem;
    // End of variables declaration//GEN-END:variables
}