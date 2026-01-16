namespace THBaoMat
{
    partial class Sanpham
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
            this.cboLoai = new System.Windows.Forms.ComboBox();
            this.txtGia = new System.Windows.Forms.TextBox();
            this.label10 = new System.Windows.Forms.Label();
            this.txtSoLuongSP = new System.Windows.Forms.TextBox();
            this.txtTenSP = new System.Windows.Forms.TextBox();
            this.txtMaSP = new System.Windows.Forms.TextBox();
            this.dtpTHBH = new System.Windows.Forms.DateTimePicker();
            this.dtpNSX = new System.Windows.Forms.DateTimePicker();
            this.label5 = new System.Windows.Forms.Label();
            this.label4 = new System.Windows.Forms.Label();
            this.label3 = new System.Windows.Forms.Label();
            this.label2 = new System.Windows.Forms.Label();
            this.label1 = new System.Windows.Forms.Label();
            this.label9 = new System.Windows.Forms.Label();
            this.label8 = new System.Windows.Forms.Label();
            this.dgvSanPham = new System.Windows.Forms.DataGridView();
            this.backgroundWorker1 = new System.ComponentModel.BackgroundWorker();
            this.label6 = new System.Windows.Forms.Label();
            this.txtMoTa = new System.Windows.Forms.TextBox();
            this.groupBox1 = new System.Windows.Forms.GroupBox();
            this.btnXoaSP = new System.Windows.Forms.Button();
            this.btnSuaSP = new System.Windows.Forms.Button();
            this.btnThemSP = new System.Windows.Forms.Button();
            this.grpQLSanPham = new System.Windows.Forms.GroupBox();
            this.cboNCC = new System.Windows.Forms.ComboBox();
            ((System.ComponentModel.ISupportInitialize)(this.dgvSanPham)).BeginInit();
            this.groupBox1.SuspendLayout();
            this.grpQLSanPham.SuspendLayout();
            this.SuspendLayout();
            // 
            // cboLoai
            // 
            this.cboLoai.BackColor = System.Drawing.Color.White;
            this.cboLoai.FormattingEnabled = true;
            this.cboLoai.Location = new System.Drawing.Point(580, 39);
            this.cboLoai.Margin = new System.Windows.Forms.Padding(3, 2, 3, 2);
            this.cboLoai.Name = "cboLoai";
            this.cboLoai.Size = new System.Drawing.Size(183, 31);
            this.cboLoai.TabIndex = 5;
            // 
            // txtGia
            // 
            this.txtGia.Anchor = System.Windows.Forms.AnchorStyles.None;
            this.txtGia.Font = new System.Drawing.Font("Segoe UI", 10F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.txtGia.Location = new System.Drawing.Point(733, 126);
            this.txtGia.Margin = new System.Windows.Forms.Padding(4, 2, 4, 2);
            this.txtGia.Name = "txtGia";
            this.txtGia.Size = new System.Drawing.Size(336, 30);
            this.txtGia.TabIndex = 8;
            // 
            // label10
            // 
            this.label10.AutoSize = true;
            this.label10.Location = new System.Drawing.Point(518, 127);
            this.label10.Name = "label10";
            this.label10.Size = new System.Drawing.Size(35, 23);
            this.label10.TabIndex = 170;
            this.label10.Text = "Giá";
            // 
            // txtSoLuongSP
            // 
            this.txtSoLuongSP.Location = new System.Drawing.Point(140, 118);
            this.txtSoLuongSP.Margin = new System.Windows.Forms.Padding(4, 2, 4, 2);
            this.txtSoLuongSP.Name = "txtSoLuongSP";
            this.txtSoLuongSP.Size = new System.Drawing.Size(284, 30);
            this.txtSoLuongSP.TabIndex = 2;
            // 
            // txtTenSP
            // 
            this.txtTenSP.Location = new System.Drawing.Point(140, 78);
            this.txtTenSP.Margin = new System.Windows.Forms.Padding(4, 2, 4, 2);
            this.txtTenSP.Name = "txtTenSP";
            this.txtTenSP.Size = new System.Drawing.Size(284, 30);
            this.txtTenSP.TabIndex = 1;
            // 
            // txtMaSP
            // 
            this.txtMaSP.Location = new System.Drawing.Point(140, 39);
            this.txtMaSP.Margin = new System.Windows.Forms.Padding(4, 2, 4, 2);
            this.txtMaSP.Name = "txtMaSP";
            this.txtMaSP.Size = new System.Drawing.Size(284, 30);
            this.txtMaSP.TabIndex = 0;
            // 
            // dtpTHBH
            // 
            this.dtpTHBH.Format = System.Windows.Forms.DateTimePickerFormat.Short;
            this.dtpTHBH.Location = new System.Drawing.Point(140, 196);
            this.dtpTHBH.Margin = new System.Windows.Forms.Padding(4, 2, 4, 2);
            this.dtpTHBH.Name = "dtpTHBH";
            this.dtpTHBH.Size = new System.Drawing.Size(284, 30);
            this.dtpTHBH.TabIndex = 4;
            // 
            // dtpNSX
            // 
            this.dtpNSX.Format = System.Windows.Forms.DateTimePickerFormat.Short;
            this.dtpNSX.Location = new System.Drawing.Point(140, 157);
            this.dtpNSX.Margin = new System.Windows.Forms.Padding(4, 2, 4, 2);
            this.dtpNSX.Name = "dtpNSX";
            this.dtpNSX.Size = new System.Drawing.Size(284, 30);
            this.dtpNSX.TabIndex = 3;
            // 
            // label5
            // 
            this.label5.AutoSize = true;
            this.label5.Location = new System.Drawing.Point(15, 193);
            this.label5.Margin = new System.Windows.Forms.Padding(4, 0, 4, 0);
            this.label5.Name = "label5";
            this.label5.Size = new System.Drawing.Size(53, 23);
            this.label5.TabIndex = 164;
            this.label5.Text = "THBH";
            // 
            // label4
            // 
            this.label4.AutoSize = true;
            this.label4.Location = new System.Drawing.Point(15, 154);
            this.label4.Margin = new System.Windows.Forms.Padding(4, 0, 4, 0);
            this.label4.Name = "label4";
            this.label4.Size = new System.Drawing.Size(42, 23);
            this.label4.TabIndex = 163;
            this.label4.Text = "NSX";
            // 
            // label3
            // 
            this.label3.AutoSize = true;
            this.label3.Location = new System.Drawing.Point(15, 116);
            this.label3.Margin = new System.Windows.Forms.Padding(4, 0, 4, 0);
            this.label3.Name = "label3";
            this.label3.Size = new System.Drawing.Size(78, 23);
            this.label3.TabIndex = 162;
            this.label3.Text = "Số lượng";
            // 
            // label2
            // 
            this.label2.AutoSize = true;
            this.label2.Location = new System.Drawing.Point(15, 78);
            this.label2.Margin = new System.Windows.Forms.Padding(4, 0, 4, 0);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(116, 23);
            this.label2.TabIndex = 161;
            this.label2.Text = "Tên sản phẩm";
            this.label2.TextAlign = System.Drawing.ContentAlignment.MiddleLeft;
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Location = new System.Drawing.Point(15, 39);
            this.label1.Margin = new System.Windows.Forms.Padding(4, 0, 4, 0);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(114, 23);
            this.label1.TabIndex = 160;
            this.label1.Text = "Mã sản phẩm";
            this.label1.TextAlign = System.Drawing.ContentAlignment.MiddleLeft;
            // 
            // label9
            // 
            this.label9.AutoSize = true;
            this.label9.Location = new System.Drawing.Point(498, 169);
            this.label9.Margin = new System.Windows.Forms.Padding(4, 0, 4, 0);
            this.label9.Name = "label9";
            this.label9.Size = new System.Drawing.Size(55, 23);
            this.label9.TabIndex = 159;
            this.label9.Text = "Mô tả";
            // 
            // label8
            // 
            this.label8.AutoSize = true;
            this.label8.Location = new System.Drawing.Point(437, 85);
            this.label8.Margin = new System.Windows.Forms.Padding(4, 0, 4, 0);
            this.label8.Name = "label8";
            this.label8.Size = new System.Drawing.Size(117, 23);
            this.label8.TabIndex = 158;
            this.label8.Text = "Nhà cung cấp";
            // 
            // dgvSanPham
            // 
            this.dgvSanPham.ColumnHeadersHeight = 34;
            this.dgvSanPham.Location = new System.Drawing.Point(12, 19);
            this.dgvSanPham.Margin = new System.Windows.Forms.Padding(3, 2, 3, 2);
            this.dgvSanPham.Name = "dgvSanPham";
            this.dgvSanPham.RowHeadersWidth = 62;
            this.dgvSanPham.RowTemplate.Height = 28;
            this.dgvSanPham.Size = new System.Drawing.Size(1489, 238);
            this.dgvSanPham.TabIndex = 0;
            this.dgvSanPham.CellContentClick += new System.Windows.Forms.DataGridViewCellEventHandler(this.dgvSanPham_CellContentClick);
            this.dgvSanPham.CellMouseClick += new System.Windows.Forms.DataGridViewCellMouseEventHandler(this.dgvSanPham_CellMouseClick);
            // 
            // label6
            // 
            this.label6.AutoSize = true;
            this.label6.Location = new System.Drawing.Point(512, 39);
            this.label6.Margin = new System.Windows.Forms.Padding(4, 0, 4, 0);
            this.label6.Name = "label6";
            this.label6.Size = new System.Drawing.Size(41, 23);
            this.label6.TabIndex = 156;
            this.label6.Text = "Loại";
            // 
            // txtMoTa
            // 
            this.txtMoTa.Anchor = System.Windows.Forms.AnchorStyles.None;
            this.txtMoTa.Font = new System.Drawing.Font("Segoe UI", 10F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.txtMoTa.Location = new System.Drawing.Point(733, 167);
            this.txtMoTa.Margin = new System.Windows.Forms.Padding(4, 2, 4, 2);
            this.txtMoTa.Multiline = true;
            this.txtMoTa.Name = "txtMoTa";
            this.txtMoTa.Size = new System.Drawing.Size(336, 67);
            this.txtMoTa.TabIndex = 14;
            // 
            // groupBox1
            // 
            this.groupBox1.Controls.Add(this.dgvSanPham);
            this.groupBox1.Location = new System.Drawing.Point(0, 276);
            this.groupBox1.Margin = new System.Windows.Forms.Padding(3, 2, 3, 2);
            this.groupBox1.Name = "groupBox1";
            this.groupBox1.Padding = new System.Windows.Forms.Padding(3, 2, 3, 2);
            this.groupBox1.Size = new System.Drawing.Size(1507, 446);
            this.groupBox1.TabIndex = 77;
            this.groupBox1.TabStop = false;
            this.groupBox1.Text = "Danh sách sản phẩm";
            // 
            // btnXoaSP
            // 
            this.btnXoaSP.Anchor = System.Windows.Forms.AnchorStyles.None;
            this.btnXoaSP.BackColor = System.Drawing.Color.FromArgb(((int)(((byte)(255)))), ((int)(((byte)(212)))), ((int)(((byte)(0)))));
            this.btnXoaSP.FlatStyle = System.Windows.Forms.FlatStyle.Popup;
            this.btnXoaSP.Font = new System.Drawing.Font("MS Reference Sans Serif", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.btnXoaSP.Location = new System.Drawing.Point(1111, 156);
            this.btnXoaSP.Margin = new System.Windows.Forms.Padding(4, 2, 4, 2);
            this.btnXoaSP.Name = "btnXoaSP";
            this.btnXoaSP.Size = new System.Drawing.Size(157, 49);
            this.btnXoaSP.TabIndex = 17;
            this.btnXoaSP.Text = "Xoá";
            this.btnXoaSP.TextImageRelation = System.Windows.Forms.TextImageRelation.ImageBeforeText;
            this.btnXoaSP.UseVisualStyleBackColor = false;
            this.btnXoaSP.Click += new System.EventHandler(this.btnXoaSP_Click);
            // 
            // btnSuaSP
            // 
            this.btnSuaSP.Anchor = System.Windows.Forms.AnchorStyles.None;
            this.btnSuaSP.BackColor = System.Drawing.Color.FromArgb(((int)(((byte)(255)))), ((int)(((byte)(212)))), ((int)(((byte)(0)))));
            this.btnSuaSP.FlatStyle = System.Windows.Forms.FlatStyle.Popup;
            this.btnSuaSP.Font = new System.Drawing.Font("MS Reference Sans Serif", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.btnSuaSP.Location = new System.Drawing.Point(1111, 98);
            this.btnSuaSP.Margin = new System.Windows.Forms.Padding(4, 2, 4, 2);
            this.btnSuaSP.Name = "btnSuaSP";
            this.btnSuaSP.Size = new System.Drawing.Size(157, 49);
            this.btnSuaSP.TabIndex = 16;
            this.btnSuaSP.Text = "Sửa";
            this.btnSuaSP.TextImageRelation = System.Windows.Forms.TextImageRelation.ImageBeforeText;
            this.btnSuaSP.UseVisualStyleBackColor = false;
            this.btnSuaSP.Click += new System.EventHandler(this.btnSuaSP_Click);
            // 
            // btnThemSP
            // 
            this.btnThemSP.Anchor = System.Windows.Forms.AnchorStyles.None;
            this.btnThemSP.BackColor = System.Drawing.Color.FromArgb(((int)(((byte)(255)))), ((int)(((byte)(212)))), ((int)(((byte)(0)))));
            this.btnThemSP.FlatStyle = System.Windows.Forms.FlatStyle.Popup;
            this.btnThemSP.Font = new System.Drawing.Font("MS Reference Sans Serif", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.btnThemSP.Location = new System.Drawing.Point(1111, 39);
            this.btnThemSP.Margin = new System.Windows.Forms.Padding(4, 2, 4, 2);
            this.btnThemSP.Name = "btnThemSP";
            this.btnThemSP.Size = new System.Drawing.Size(157, 49);
            this.btnThemSP.TabIndex = 15;
            this.btnThemSP.Text = "Thêm";
            this.btnThemSP.TextImageRelation = System.Windows.Forms.TextImageRelation.ImageBeforeText;
            this.btnThemSP.UseVisualStyleBackColor = false;
            this.btnThemSP.Click += new System.EventHandler(this.btnThemSP_Click);
            // 
            // grpQLSanPham
            // 
            this.grpQLSanPham.Controls.Add(this.cboLoai);
            this.grpQLSanPham.Controls.Add(this.txtGia);
            this.grpQLSanPham.Controls.Add(this.label10);
            this.grpQLSanPham.Controls.Add(this.txtSoLuongSP);
            this.grpQLSanPham.Controls.Add(this.txtTenSP);
            this.grpQLSanPham.Controls.Add(this.txtMaSP);
            this.grpQLSanPham.Controls.Add(this.dtpTHBH);
            this.grpQLSanPham.Controls.Add(this.dtpNSX);
            this.grpQLSanPham.Controls.Add(this.label5);
            this.grpQLSanPham.Controls.Add(this.label4);
            this.grpQLSanPham.Controls.Add(this.label3);
            this.grpQLSanPham.Controls.Add(this.label2);
            this.grpQLSanPham.Controls.Add(this.label1);
            this.grpQLSanPham.Controls.Add(this.label9);
            this.grpQLSanPham.Controls.Add(this.label8);
            this.grpQLSanPham.Controls.Add(this.label6);
            this.grpQLSanPham.Controls.Add(this.cboNCC);
            this.grpQLSanPham.Controls.Add(this.txtMoTa);
            this.grpQLSanPham.Controls.Add(this.btnXoaSP);
            this.grpQLSanPham.Controls.Add(this.btnSuaSP);
            this.grpQLSanPham.Controls.Add(this.btnThemSP);
            this.grpQLSanPham.Dock = System.Windows.Forms.DockStyle.Top;
            this.grpQLSanPham.Font = new System.Drawing.Font("Segoe UI", 10F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.grpQLSanPham.Location = new System.Drawing.Point(0, 0);
            this.grpQLSanPham.Margin = new System.Windows.Forms.Padding(4, 2, 4, 2);
            this.grpQLSanPham.Name = "grpQLSanPham";
            this.grpQLSanPham.Padding = new System.Windows.Forms.Padding(4, 2, 4, 2);
            this.grpQLSanPham.Size = new System.Drawing.Size(1507, 285);
            this.grpQLSanPham.TabIndex = 76;
            this.grpQLSanPham.TabStop = false;
            this.grpQLSanPham.Text = "Quản Lý Sản Phẩm";
            // 
            // cboNCC
            // 
            this.cboNCC.Anchor = System.Windows.Forms.AnchorStyles.None;
            this.cboNCC.Font = new System.Drawing.Font("Segoe UI", 10F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.cboNCC.FormattingEnabled = true;
            this.cboNCC.Location = new System.Drawing.Point(733, 84);
            this.cboNCC.Margin = new System.Windows.Forms.Padding(4, 2, 4, 2);
            this.cboNCC.Name = "cboNCC";
            this.cboNCC.Size = new System.Drawing.Size(336, 31);
            this.cboNCC.TabIndex = 7;
            // 
            // Sanpham
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(8F, 16F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(1507, 533);
            this.Controls.Add(this.groupBox1);
            this.Controls.Add(this.grpQLSanPham);
            this.Name = "Sanpham";
            this.Text = "Sanpham";
            ((System.ComponentModel.ISupportInitialize)(this.dgvSanPham)).EndInit();
            this.groupBox1.ResumeLayout(false);
            this.grpQLSanPham.ResumeLayout(false);
            this.grpQLSanPham.PerformLayout();
            this.ResumeLayout(false);

        }

        #endregion
        private System.Windows.Forms.ComboBox cboLoai;
        private System.Windows.Forms.TextBox txtGia;
        private System.Windows.Forms.Label label10;
        private System.Windows.Forms.TextBox txtSoLuongSP;
        private System.Windows.Forms.TextBox txtTenSP;
        private System.Windows.Forms.TextBox txtMaSP;
        private System.Windows.Forms.DateTimePicker dtpTHBH;
        private System.Windows.Forms.DateTimePicker dtpNSX;
        private System.Windows.Forms.Label label5;
        private System.Windows.Forms.Label label4;
        private System.Windows.Forms.Label label3;
        private System.Windows.Forms.Label label2;
        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.Label label9;
        private System.Windows.Forms.Label label8;
        private System.Windows.Forms.DataGridView dgvSanPham;
        private System.ComponentModel.BackgroundWorker backgroundWorker1;
        private System.Windows.Forms.Label label6;
        private System.Windows.Forms.TextBox txtMoTa;
        private System.Windows.Forms.GroupBox groupBox1;
        private System.Windows.Forms.Button btnXoaSP;
        private System.Windows.Forms.Button btnSuaSP;
        private System.Windows.Forms.Button btnThemSP;
        private System.Windows.Forms.GroupBox grpQLSanPham;
        private System.Windows.Forms.ComboBox cboNCC;
    }
}