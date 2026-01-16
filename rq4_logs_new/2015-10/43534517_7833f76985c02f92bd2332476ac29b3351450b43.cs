namespace NhanSu_TTN
{
    partial class Form1
    {
        /// <summary>
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Windows Form Designer generated code

        /// <summary>
        /// Required method for Designer support - do not modify
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            this.btnClear = new System.Windows.Forms.Button();
            this.groupBox1 = new System.Windows.Forms.GroupBox();
            this.btnRefresh = new System.Windows.Forms.Button();
            this.rbtDiaChi = new System.Windows.Forms.RadioButton();
            this.rbtSDT = new System.Windows.Forms.RadioButton();
            this.rbtChucVu = new System.Windows.Forms.RadioButton();
            this.rbtTenNV = new System.Windows.Forms.RadioButton();
            this.rbtMaNV = new System.Windows.Forms.RadioButton();
            this.btnTimkiem = new System.Windows.Forms.Button();
            this.txtTimKiem = new System.Windows.Forms.TextBox();
            this.label6 = new System.Windows.Forms.Label();
            this.txtLuong = new System.Windows.Forms.TextBox();
            this.label5 = new System.Windows.Forms.Label();
            this.txtChucVu = new System.Windows.Forms.TextBox();
            this.label4 = new System.Windows.Forms.Label();
            this.label3 = new System.Windows.Forms.Label();
            this.label2 = new System.Windows.Forms.Label();
            this.label1 = new System.Windows.Forms.Label();
            this.txtSDT = new System.Windows.Forms.TextBox();
            this.txtTenNV = new System.Windows.Forms.TextBox();
            this.txtDiaChi = new System.Windows.Forms.TextBox();
            this.txtMaNV = new System.Windows.Forms.TextBox();
            this.btnSua = new System.Windows.Forms.Button();
            this.btnLuu = new System.Windows.Forms.Button();
            this.btnXoa = new System.Windows.Forms.Button();
            this.btnThem = new System.Windows.Forms.Button();
            this.dgvNhanVien = new System.Windows.Forms.DataGridView();
            this.clmSTT = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.clmMaNV = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.clmTenNV = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.clmChucVu = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.clmSDT = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.clmLuong = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.clmDiaChi = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.groupBox1.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)(this.dgvNhanVien)).BeginInit();
            this.SuspendLayout();
            // 
            // btnClear
            // 
            this.btnClear.Location = new System.Drawing.Point(23, 148);
            this.btnClear.Name = "btnClear";
            this.btnClear.Size = new System.Drawing.Size(75, 23);
            this.btnClear.TabIndex = 55;
            this.btnClear.Text = "Clear";
            this.btnClear.UseVisualStyleBackColor = true;
            // 
            // groupBox1
            // 
            this.groupBox1.BackColor = System.Drawing.Color.CornflowerBlue;
            this.groupBox1.Controls.Add(this.rbtDiaChi);
            this.groupBox1.Controls.Add(this.rbtSDT);
            this.groupBox1.Controls.Add(this.rbtChucVu);
            this.groupBox1.Controls.Add(this.rbtTenNV);
            this.groupBox1.Controls.Add(this.rbtMaNV);
            this.groupBox1.Controls.Add(this.btnTimkiem);
            this.groupBox1.Controls.Add(this.txtTimKiem);
            this.groupBox1.Location = new System.Drawing.Point(591, 12);
            this.groupBox1.Name = "groupBox1";
            this.groupBox1.Size = new System.Drawing.Size(267, 178);
            this.groupBox1.TabIndex = 54;
            this.groupBox1.TabStop = false;
            this.groupBox1.Text = "Tìm kiếm";
            // 
            // btnRefresh
            // 
            this.btnRefresh.Location = new System.Drawing.Point(23, 180);
            this.btnRefresh.Name = "btnRefresh";
            this.btnRefresh.Size = new System.Drawing.Size(75, 23);
            this.btnRefresh.TabIndex = 35;
            this.btnRefresh.Text = "Refresh ";
            this.btnRefresh.UseVisualStyleBackColor = true;
            // 
            // rbtDiaChi
            // 
            this.rbtDiaChi.AutoSize = true;
            this.rbtDiaChi.Location = new System.Drawing.Point(16, 71);
            this.rbtDiaChi.Name = "rbtDiaChi";
            this.rbtDiaChi.Size = new System.Drawing.Size(59, 17);
            this.rbtDiaChi.TabIndex = 34;
            this.rbtDiaChi.TabStop = true;
            this.rbtDiaChi.Text = "Địa Chỉ";
            this.rbtDiaChi.UseVisualStyleBackColor = true;
            // 
            // rbtSDT
            // 
            this.rbtSDT.AutoSize = true;
            this.rbtSDT.Location = new System.Drawing.Point(16, 140);
            this.rbtSDT.Name = "rbtSDT";
            this.rbtSDT.Size = new System.Drawing.Size(88, 17);
            this.rbtSDT.TabIndex = 33;
            this.rbtSDT.TabStop = true;
            this.rbtSDT.Text = "Số điện thoại";
            this.rbtSDT.UseVisualStyleBackColor = true;
            // 
            // rbtChucVu
            // 
            this.rbtChucVu.AutoSize = true;
            this.rbtChucVu.Location = new System.Drawing.Point(18, 117);
            this.rbtChucVu.Name = "rbtChucVu";
            this.rbtChucVu.Size = new System.Drawing.Size(65, 17);
            this.rbtChucVu.TabIndex = 32;
            this.rbtChucVu.TabStop = true;
            this.rbtChucVu.Text = "Chức vụ";
            this.rbtChucVu.UseVisualStyleBackColor = true;
            // 
            // rbtTenNV
            // 
            this.rbtTenNV.AutoSize = true;
            this.rbtTenNV.Location = new System.Drawing.Point(16, 94);
            this.rbtTenNV.Name = "rbtTenNV";
            this.rbtTenNV.Size = new System.Drawing.Size(94, 17);
            this.rbtTenNV.TabIndex = 31;
            this.rbtTenNV.TabStop = true;
            this.rbtTenNV.Text = "Tên nhân viên";
            this.rbtTenNV.UseVisualStyleBackColor = true;
            // 
            // rbtMaNV
            // 
            this.rbtMaNV.AutoSize = true;
            this.rbtMaNV.Location = new System.Drawing.Point(16, 47);
            this.rbtMaNV.Name = "rbtMaNV";
            this.rbtMaNV.Size = new System.Drawing.Size(90, 17);
            this.rbtMaNV.TabIndex = 30;
            this.rbtMaNV.TabStop = true;
            this.rbtMaNV.Text = "Mã nhân viên";
            this.rbtMaNV.UseVisualStyleBackColor = true;
            // 
            // btnTimkiem
            // 
            this.btnTimkiem.Location = new System.Drawing.Point(257, 213);
            this.btnTimkiem.Name = "btnTimkiem";
            this.btnTimkiem.Size = new System.Drawing.Size(75, 23);
            this.btnTimkiem.TabIndex = 28;
            this.btnTimkiem.Text = "Tìm  kiếm";
            this.btnTimkiem.UseVisualStyleBackColor = true;
            // 
            // txtTimKiem
            // 
            this.txtTimKiem.Location = new System.Drawing.Point(16, 19);
            this.txtTimKiem.Name = "txtTimKiem";
            this.txtTimKiem.Size = new System.Drawing.Size(233, 20);
            this.txtTimKiem.TabIndex = 29;
            // 
            // label6
            // 
            this.label6.AutoSize = true;
            this.label6.Location = new System.Drawing.Point(151, 148);
            this.label6.Name = "label6";
            this.label6.Size = new System.Drawing.Size(37, 13);
            this.label6.TabIndex = 53;
            this.label6.Text = "Lương";
            // 
            // txtLuong
            // 
            this.txtLuong.Location = new System.Drawing.Point(275, 143);
            this.txtLuong.Name = "txtLuong";
            this.txtLuong.Size = new System.Drawing.Size(155, 20);
            this.txtLuong.TabIndex = 52;
            // 
            // label5
            // 
            this.label5.AutoSize = true;
            this.label5.Location = new System.Drawing.Point(151, 86);
            this.label5.Name = "label5";
            this.label5.Size = new System.Drawing.Size(47, 13);
            this.label5.TabIndex = 51;
            this.label5.Text = "Chức vụ";
            // 
            // txtChucVu
            // 
            this.txtChucVu.Location = new System.Drawing.Point(275, 81);
            this.txtChucVu.Name = "txtChucVu";
            this.txtChucVu.Size = new System.Drawing.Size(155, 20);
            this.txtChucVu.TabIndex = 50;
            // 
            // label4
            // 
            this.label4.AutoSize = true;
            this.label4.Location = new System.Drawing.Point(151, 55);
            this.label4.Name = "label4";
            this.label4.Size = new System.Drawing.Size(79, 13);
            this.label4.TabIndex = 49;
            this.label4.Text = "Tên  nhân viên";
            // 
            // label3
            // 
            this.label3.AutoSize = true;
            this.label3.Location = new System.Drawing.Point(151, 179);
            this.label3.Name = "label3";
            this.label3.Size = new System.Drawing.Size(40, 13);
            this.label3.TabIndex = 48;
            this.label3.Text = "Địa chỉ";
            // 
            // label2
            // 
            this.label2.AutoSize = true;
            this.label2.Location = new System.Drawing.Point(151, 117);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(70, 13);
            this.label2.TabIndex = 47;
            this.label2.Text = "Số điện thoại";
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Location = new System.Drawing.Point(151, 24);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(72, 13);
            this.label1.TabIndex = 46;
            this.label1.Text = "Mã nhân viên";
            // 
            // txtSDT
            // 
            this.txtSDT.Location = new System.Drawing.Point(275, 112);
            this.txtSDT.Name = "txtSDT";
            this.txtSDT.Size = new System.Drawing.Size(258, 20);
            this.txtSDT.TabIndex = 45;
            // 
            // txtTenNV
            // 
            this.txtTenNV.Location = new System.Drawing.Point(275, 50);
            this.txtTenNV.Name = "txtTenNV";
            this.txtTenNV.Size = new System.Drawing.Size(258, 20);
            this.txtTenNV.TabIndex = 44;
            // 
            // txtDiaChi
            // 
            this.txtDiaChi.Location = new System.Drawing.Point(275, 174);
            this.txtDiaChi.Name = "txtDiaChi";
            this.txtDiaChi.Size = new System.Drawing.Size(258, 20);
            this.txtDiaChi.TabIndex = 43;
            // 
            // txtMaNV
            // 
            this.txtMaNV.Location = new System.Drawing.Point(275, 19);
            this.txtMaNV.Name = "txtMaNV";
            this.txtMaNV.Size = new System.Drawing.Size(155, 20);
            this.txtMaNV.TabIndex = 42;
            // 
            // btnSua
            // 
            this.btnSua.Location = new System.Drawing.Point(23, 46);
            this.btnSua.Name = "btnSua";
            this.btnSua.Size = new System.Drawing.Size(75, 23);
            this.btnSua.TabIndex = 41;
            this.btnSua.Text = "Sửa";
            this.btnSua.UseVisualStyleBackColor = true;
            // 
            // btnLuu
            // 
            this.btnLuu.Location = new System.Drawing.Point(23, 114);
            this.btnLuu.Name = "btnLuu";
            this.btnLuu.Size = new System.Drawing.Size(75, 23);
            this.btnLuu.TabIndex = 40;
            this.btnLuu.Text = "Lưu";
            this.btnLuu.UseVisualStyleBackColor = true;
            // 
            // btnXoa
            // 
            this.btnXoa.Location = new System.Drawing.Point(23, 80);
            this.btnXoa.Name = "btnXoa";
            this.btnXoa.Size = new System.Drawing.Size(75, 23);
            this.btnXoa.TabIndex = 39;
            this.btnXoa.Text = "Xóa";
            this.btnXoa.UseVisualStyleBackColor = true;
            // 
            // btnThem
            // 
            this.btnThem.Location = new System.Drawing.Point(23, 12);
            this.btnThem.Name = "btnThem";
            this.btnThem.Size = new System.Drawing.Size(75, 23);
            this.btnThem.TabIndex = 38;
            this.btnThem.Text = "Thêm";
            this.btnThem.UseVisualStyleBackColor = true;
            // 
            // dgvNhanVien
            // 
            this.dgvNhanVien.ColumnHeadersHeightSizeMode = System.Windows.Forms.DataGridViewColumnHeadersHeightSizeMode.AutoSize;
            this.dgvNhanVien.Columns.AddRange(new System.Windows.Forms.DataGridViewColumn[] {
            this.clmSTT,
            this.clmMaNV,
            this.clmTenNV,
            this.clmChucVu,
            this.clmSDT,
            this.clmLuong,
            this.clmDiaChi});
            this.dgvNhanVien.Location = new System.Drawing.Point(2, 222);
            this.dgvNhanVien.Margin = new System.Windows.Forms.Padding(4);
            this.dgvNhanVien.Name = "dgvNhanVien";
            this.dgvNhanVien.Size = new System.Drawing.Size(856, 170);
            this.dgvNhanVien.TabIndex = 37;
            // 
            // clmSTT
            // 
            this.clmSTT.HeaderText = "STT";
            this.clmSTT.Name = "clmSTT";
            this.clmSTT.Width = 50;
            // 
            // clmMaNV
            // 
            this.clmMaNV.DataPropertyName = "MaNV";
            this.clmMaNV.HeaderText = "Mã  nhân viên";
            this.clmMaNV.Name = "clmMaNV";
            this.clmMaNV.Width = 120;
            // 
            // clmTenNV
            // 
            this.clmTenNV.DataPropertyName = "TenNV";
            this.clmTenNV.HeaderText = "Tên nhân viên";
            this.clmTenNV.Name = "clmTenNV";
            this.clmTenNV.Width = 220;
            // 
            // clmChucVu
            // 
            this.clmChucVu.DataPropertyName = "ChucVu";
            this.clmChucVu.HeaderText = "Chức vụ";
            this.clmChucVu.Name = "clmChucVu";
            // 
            // clmSDT
            // 
            this.clmSDT.DataPropertyName = "SDT";
            this.clmSDT.HeaderText = "Số điện thoại";
            this.clmSDT.Name = "clmSDT";
            this.clmSDT.Width = 150;
            // 
            // clmLuong
            // 
            this.clmLuong.DataPropertyName = "LUONG";
            this.clmLuong.HeaderText = "Lương";
            this.clmLuong.Name = "clmLuong";
            // 
            // clmDiaChi
            // 
            this.clmDiaChi.DataPropertyName = "DiaChi";
            this.clmDiaChi.HeaderText = "Địa chỉ";
            this.clmDiaChi.Name = "clmDiaChi";
            this.clmDiaChi.Width = 250;
            // 
            // Form1
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(871, 393);
            this.Controls.Add(this.btnRefresh);
            this.Controls.Add(this.btnClear);
            this.Controls.Add(this.groupBox1);
            this.Controls.Add(this.label6);
            this.Controls.Add(this.txtLuong);
            this.Controls.Add(this.label5);
            this.Controls.Add(this.txtChucVu);
            this.Controls.Add(this.label4);
            this.Controls.Add(this.label3);
            this.Controls.Add(this.label2);
            this.Controls.Add(this.label1);
            this.Controls.Add(this.txtSDT);
            this.Controls.Add(this.txtTenNV);
            this.Controls.Add(this.txtDiaChi);
            this.Controls.Add(this.txtMaNV);
            this.Controls.Add(this.btnSua);
            this.Controls.Add(this.btnLuu);
            this.Controls.Add(this.btnXoa);
            this.Controls.Add(this.btnThem);
            this.Controls.Add(this.dgvNhanVien);
            this.Name = "Form1";
            this.Text = "NhanSu";
            this.groupBox1.ResumeLayout(false);
            this.groupBox1.PerformLayout();
            ((System.ComponentModel.ISupportInitialize)(this.dgvNhanVien)).EndInit();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Button btnClear;
        private System.Windows.Forms.GroupBox groupBox1;
        private System.Windows.Forms.Button btnRefresh;
        private System.Windows.Forms.RadioButton rbtDiaChi;
        private System.Windows.Forms.RadioButton rbtSDT;
        private System.Windows.Forms.RadioButton rbtChucVu;
        private System.Windows.Forms.RadioButton rbtTenNV;
        private System.Windows.Forms.RadioButton rbtMaNV;
        private System.Windows.Forms.Button btnTimkiem;
        private System.Windows.Forms.TextBox txtTimKiem;
        private System.Windows.Forms.Label label6;
        private System.Windows.Forms.TextBox txtLuong;
        private System.Windows.Forms.Label label5;
        private System.Windows.Forms.TextBox txtChucVu;
        private System.Windows.Forms.Label label4;
        private System.Windows.Forms.Label label3;
        private System.Windows.Forms.Label label2;
        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.TextBox txtSDT;
        private System.Windows.Forms.TextBox txtTenNV;
        private System.Windows.Forms.TextBox txtDiaChi;
        private System.Windows.Forms.TextBox txtMaNV;
        private System.Windows.Forms.Button btnSua;
        private System.Windows.Forms.Button btnLuu;
        private System.Windows.Forms.Button btnXoa;
        private System.Windows.Forms.Button btnThem;
        private System.Windows.Forms.DataGridView dgvNhanVien;
        private System.Windows.Forms.DataGridViewTextBoxColumn clmSTT;
        private System.Windows.Forms.DataGridViewTextBoxColumn clmMaNV;
        private System.Windows.Forms.DataGridViewTextBoxColumn clmTenNV;
        private System.Windows.Forms.DataGridViewTextBoxColumn clmChucVu;
        private System.Windows.Forms.DataGridViewTextBoxColumn clmSDT;
        private System.Windows.Forms.DataGridViewTextBoxColumn clmLuong;
        private System.Windows.Forms.DataGridViewTextBoxColumn clmDiaChi;
    }
}
