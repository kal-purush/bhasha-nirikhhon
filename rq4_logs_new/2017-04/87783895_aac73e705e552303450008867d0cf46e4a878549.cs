namespace BHXH.Sys.UI
{
    partial class frmChangePass
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
            this.lblHoTen = new DevExpress.XtraEditors.LabelControl();
            this.labelControl1 = new DevExpress.XtraEditors.LabelControl();
            this.txtUser = new System.Windows.Forms.TextBox();
            this.lblPwd = new DevExpress.XtraEditors.LabelControl();
            this.txtPwd = new System.Windows.Forms.TextBox();
            this.btnGhi = new DevExpress.XtraEditors.SimpleButton();
            this.btnThoat = new DevExpress.XtraEditors.SimpleButton();
            this.txtHoten = new System.Windows.Forms.TextBox();
            this.SuspendLayout();
            // 
            // lblHoTen
            // 
            this.lblHoTen.Location = new System.Drawing.Point(22, 73);
            this.lblHoTen.Name = "lblHoTen";
            this.lblHoTen.Size = new System.Drawing.Size(47, 13);
            this.lblHoTen.TabIndex = 30;
            this.lblHoTen.Text = "Họ và tên";
            // 
            // labelControl1
            // 
            this.labelControl1.Location = new System.Drawing.Point(22, 113);
            this.labelControl1.Name = "labelControl1";
            this.labelControl1.Size = new System.Drawing.Size(75, 13);
            this.labelControl1.TabIndex = 31;
            this.labelControl1.Text = "Tên Đăng Nhập";
            // 
            // txtUser
            // 
            this.txtUser.Location = new System.Drawing.Point(106, 106);
            this.txtUser.Name = "txtUser";
            this.txtUser.Size = new System.Drawing.Size(214, 20);
            this.txtUser.TabIndex = 32;
            this.txtUser.TextChanged += new System.EventHandler(this.txtUser_TextChanged);
            this.txtUser.KeyDown += new System.Windows.Forms.KeyEventHandler(this.txtUser_KeyDown);
            this.txtUser.Leave += new System.EventHandler(this.txtUser_Leave);
            // 
            // lblPwd
            // 
            this.lblPwd.Location = new System.Drawing.Point(22, 148);
            this.lblPwd.Name = "lblPwd";
            this.lblPwd.Size = new System.Drawing.Size(45, 13);
            this.lblPwd.TabIndex = 33;
            this.lblPwd.Text = "Mật Khẩu";
            // 
            // txtPwd
            // 
            this.txtPwd.Location = new System.Drawing.Point(106, 141);
            this.txtPwd.Name = "txtPwd";
            this.txtPwd.Size = new System.Drawing.Size(214, 20);
            this.txtPwd.TabIndex = 34;
            // 
            // btnGhi
            // 
            this.btnGhi.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Bottom | System.Windows.Forms.AnchorStyles.Right)));
            this.btnGhi.Appearance.Font = new System.Drawing.Font("Tahoma", 8.25F, System.Drawing.FontStyle.Bold);
            this.btnGhi.Appearance.Options.UseFont = true;
            this.btnGhi.Location = new System.Drawing.Point(104, 183);
            this.btnGhi.Name = "btnGhi";
            this.btnGhi.Size = new System.Drawing.Size(75, 29);
            this.btnGhi.TabIndex = 35;
            this.btnGhi.Text = "&Ghi";
            this.btnGhi.Click += new System.EventHandler(this.btnGhi_Click);
            // 
            // btnThoat
            // 
            this.btnThoat.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Bottom | System.Windows.Forms.AnchorStyles.Right)));
            this.btnThoat.Appearance.Font = new System.Drawing.Font("Tahoma", 8.25F, System.Drawing.FontStyle.Bold);
            this.btnThoat.Appearance.Options.UseFont = true;
            this.btnThoat.Location = new System.Drawing.Point(243, 183);
            this.btnThoat.Name = "btnThoat";
            this.btnThoat.Size = new System.Drawing.Size(75, 29);
            this.btnThoat.TabIndex = 37;
            this.btnThoat.Text = "&Thoát";
            this.btnThoat.Click += new System.EventHandler(this.btnThoat_Click);
            // 
            // txtHoten
            // 
            this.txtHoten.Location = new System.Drawing.Point(106, 66);
            this.txtHoten.Name = "txtHoten";
            this.txtHoten.Size = new System.Drawing.Size(214, 20);
            this.txtHoten.TabIndex = 38;
            this.txtHoten.TextChanged += new System.EventHandler(this.textBox1_TextChanged);
            // 
            // frmChangePass
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(330, 224);
            this.Controls.Add(this.txtHoten);
            this.Controls.Add(this.btnThoat);
            this.Controls.Add(this.btnGhi);
            this.Controls.Add(this.txtPwd);
            this.Controls.Add(this.lblPwd);
            this.Controls.Add(this.txtUser);
            this.Controls.Add(this.labelControl1);
            this.Controls.Add(this.lblHoTen);
            this.Name = "frmChangePass";
            this.Text = "Thay đổi mật khẩu";
            this.Load += new System.EventHandler(this.frmChangePass_Load);
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion
        internal DevExpress.XtraEditors.LabelControl lblHoTen;
        internal DevExpress.XtraEditors.LabelControl labelControl1;
        private System.Windows.Forms.TextBox txtUser;
        internal DevExpress.XtraEditors.LabelControl lblPwd;
        private System.Windows.Forms.TextBox txtPwd;
        internal DevExpress.XtraEditors.SimpleButton btnGhi;
        internal DevExpress.XtraEditors.SimpleButton btnThoat;
        private System.Windows.Forms.TextBox txtHoten;
    }
}