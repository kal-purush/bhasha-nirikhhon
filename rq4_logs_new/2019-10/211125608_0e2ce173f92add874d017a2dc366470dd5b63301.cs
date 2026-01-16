namespace QLthuvien.GUI
{
    partial class FormMuonSach
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
            System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(FormMuonSach));
            this.gunaPanel1 = new Guna.UI.WinForms.GunaPanel();
            this.btn_MuonSach = new Guna.UI.WinForms.GunaButton();
            this.gunaLabel3 = new Guna.UI.WinForms.GunaLabel();
            this.dtp_NgayMuon = new System.Windows.Forms.DateTimePicker();
            this.cb_Cuonsach = new System.Windows.Forms.ComboBox();
            this.gunaLabel1 = new Guna.UI.WinForms.GunaLabel();
            this.txt_MaSach = new Guna.UI.WinForms.GunaTextBox();
            this.gunaLabel2 = new Guna.UI.WinForms.GunaLabel();
            this.gunaLabel5 = new Guna.UI.WinForms.GunaLabel();
            this.txt_MaDG = new Guna.UI.WinForms.GunaTextBox();
            this.gunaPanel1.SuspendLayout();
            this.SuspendLayout();
            // 
            // gunaPanel1
            // 
            this.gunaPanel1.BackColor = System.Drawing.SystemColors.Info;
            this.gunaPanel1.Controls.Add(this.gunaLabel5);
            this.gunaPanel1.Controls.Add(this.txt_MaDG);
            this.gunaPanel1.Controls.Add(this.btn_MuonSach);
            this.gunaPanel1.Controls.Add(this.gunaLabel3);
            this.gunaPanel1.Controls.Add(this.dtp_NgayMuon);
            this.gunaPanel1.Controls.Add(this.gunaLabel2);
            this.gunaPanel1.Controls.Add(this.txt_MaSach);
            this.gunaPanel1.Controls.Add(this.cb_Cuonsach);
            this.gunaPanel1.Controls.Add(this.gunaLabel1);
            this.gunaPanel1.Location = new System.Drawing.Point(0, 0);
            this.gunaPanel1.Name = "gunaPanel1";
            this.gunaPanel1.Size = new System.Drawing.Size(530, 426);
            this.gunaPanel1.TabIndex = 10;
            // 
            // btn_MuonSach
            // 
            this.btn_MuonSach.AnimationHoverSpeed = 0.07F;
            this.btn_MuonSach.AnimationSpeed = 0.03F;
            this.btn_MuonSach.BaseColor = System.Drawing.Color.FromArgb(((int)(((byte)(100)))), ((int)(((byte)(88)))), ((int)(((byte)(255)))));
            this.btn_MuonSach.BorderColor = System.Drawing.Color.Black;
            this.btn_MuonSach.FocusedColor = System.Drawing.Color.Empty;
            this.btn_MuonSach.Font = new System.Drawing.Font("Segoe UI", 9F);
            this.btn_MuonSach.ForeColor = System.Drawing.Color.White;
            this.btn_MuonSach.Image = ((System.Drawing.Image)(resources.GetObject("btn_MuonSach.Image")));
            this.btn_MuonSach.ImageSize = new System.Drawing.Size(20, 20);
            this.btn_MuonSach.Location = new System.Drawing.Point(187, 311);
            this.btn_MuonSach.Name = "btn_MuonSach";
            this.btn_MuonSach.OnHoverBaseColor = System.Drawing.Color.FromArgb(((int)(((byte)(151)))), ((int)(((byte)(143)))), ((int)(((byte)(255)))));
            this.btn_MuonSach.OnHoverBorderColor = System.Drawing.Color.Black;
            this.btn_MuonSach.OnHoverForeColor = System.Drawing.Color.White;
            this.btn_MuonSach.OnHoverImage = null;
            this.btn_MuonSach.OnPressedColor = System.Drawing.Color.Black;
            this.btn_MuonSach.Size = new System.Drawing.Size(195, 42);
            this.btn_MuonSach.TabIndex = 16;
            this.btn_MuonSach.Text = "Mượn sách";
            // 
            // gunaLabel3
            // 
            this.gunaLabel3.AutoSize = true;
            this.gunaLabel3.Font = new System.Drawing.Font("Segoe UI", 9F);
            this.gunaLabel3.Location = new System.Drawing.Point(75, 224);
            this.gunaLabel3.Name = "gunaLabel3";
            this.gunaLabel3.Size = new System.Drawing.Size(90, 20);
            this.gunaLabel3.TabIndex = 15;
            this.gunaLabel3.Text = "Ngày mượn:";
            // 
            // dtp_NgayMuon
            // 
            this.dtp_NgayMuon.Location = new System.Drawing.Point(187, 222);
            this.dtp_NgayMuon.Name = "dtp_NgayMuon";
            this.dtp_NgayMuon.Size = new System.Drawing.Size(195, 22);
            this.dtp_NgayMuon.TabIndex = 14;
            // 
            // cb_Cuonsach
            // 
            this.cb_Cuonsach.FormattingEnabled = true;
            this.cb_Cuonsach.Location = new System.Drawing.Point(187, 110);
            this.cb_Cuonsach.Name = "cb_Cuonsach";
            this.cb_Cuonsach.Size = new System.Drawing.Size(195, 24);
            this.cb_Cuonsach.TabIndex = 11;
            // 
            // gunaLabel1
            // 
            this.gunaLabel1.AutoSize = true;
            this.gunaLabel1.Font = new System.Drawing.Font("Segoe UI", 9F);
            this.gunaLabel1.Location = new System.Drawing.Point(71, 110);
            this.gunaLabel1.Name = "gunaLabel1";
            this.gunaLabel1.Size = new System.Drawing.Size(79, 20);
            this.gunaLabel1.TabIndex = 10;
            this.gunaLabel1.Text = "Cuốn sách:";
            // 
            // txt_MaSach
            // 
            this.txt_MaSach.BaseColor = System.Drawing.Color.White;
            this.txt_MaSach.BorderColor = System.Drawing.Color.Black;
            this.txt_MaSach.Cursor = System.Windows.Forms.Cursors.IBeam;
            this.txt_MaSach.FocusedBaseColor = System.Drawing.Color.White;
            this.txt_MaSach.FocusedBorderColor = System.Drawing.Color.Blue;
            this.txt_MaSach.FocusedForeColor = System.Drawing.SystemColors.ControlText;
            this.txt_MaSach.Font = new System.Drawing.Font("Segoe UI", 9F);
            this.txt_MaSach.Location = new System.Drawing.Point(187, 154);
            this.txt_MaSach.Name = "txt_MaSach";
            this.txt_MaSach.PasswordChar = '\0';
            this.txt_MaSach.Size = new System.Drawing.Size(195, 32);
            this.txt_MaSach.TabIndex = 12;
            // 
            // gunaLabel2
            // 
            this.gunaLabel2.AutoSize = true;
            this.gunaLabel2.Font = new System.Drawing.Font("Segoe UI", 9F);
            this.gunaLabel2.Location = new System.Drawing.Point(75, 166);
            this.gunaLabel2.Name = "gunaLabel2";
            this.gunaLabel2.Size = new System.Drawing.Size(66, 20);
            this.gunaLabel2.TabIndex = 13;
            this.gunaLabel2.Text = "Mã sách:";
            // 
            // gunaLabel5
            // 
            this.gunaLabel5.AutoSize = true;
            this.gunaLabel5.Font = new System.Drawing.Font("Segoe UI", 9F);
            this.gunaLabel5.Location = new System.Drawing.Point(74, 63);
            this.gunaLabel5.Name = "gunaLabel5";
            this.gunaLabel5.Size = new System.Drawing.Size(87, 20);
            this.gunaLabel5.TabIndex = 20;
            this.gunaLabel5.Text = "Mã độc giả:";
            // 
            // txt_MaDG
            // 
            this.txt_MaDG.BaseColor = System.Drawing.Color.White;
            this.txt_MaDG.BorderColor = System.Drawing.Color.Black;
            this.txt_MaDG.Cursor = System.Windows.Forms.Cursors.IBeam;
            this.txt_MaDG.FocusedBaseColor = System.Drawing.Color.White;
            this.txt_MaDG.FocusedBorderColor = System.Drawing.Color.Blue;
            this.txt_MaDG.FocusedForeColor = System.Drawing.SystemColors.ControlText;
            this.txt_MaDG.Font = new System.Drawing.Font("Segoe UI", 9F);
            this.txt_MaDG.Location = new System.Drawing.Point(187, 51);
            this.txt_MaDG.Name = "txt_MaDG";
            this.txt_MaDG.PasswordChar = '\0';
            this.txt_MaDG.Size = new System.Drawing.Size(195, 32);
            this.txt_MaDG.TabIndex = 19;
            // 
            // FormMuonSach
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(8F, 16F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(533, 426);
            this.Controls.Add(this.gunaPanel1);
            this.Name = "FormMuonSach";
            this.StartPosition = System.Windows.Forms.FormStartPosition.CenterScreen;
            this.Text = "Mượn sách";
            this.Load += new System.EventHandler(this.FormMuonSach_Load);
            this.gunaPanel1.ResumeLayout(false);
            this.gunaPanel1.PerformLayout();
            this.ResumeLayout(false);

        }

        #endregion

        private Guna.UI.WinForms.GunaPanel gunaPanel1;
        private Guna.UI.WinForms.GunaButton btn_MuonSach;
        private Guna.UI.WinForms.GunaLabel gunaLabel3;
        private System.Windows.Forms.DateTimePicker dtp_NgayMuon;
        private System.Windows.Forms.ComboBox cb_Cuonsach;
        private Guna.UI.WinForms.GunaLabel gunaLabel1;
        private Guna.UI.WinForms.GunaLabel gunaLabel5;
        private Guna.UI.WinForms.GunaTextBox txt_MaDG;
        private Guna.UI.WinForms.GunaLabel gunaLabel2;
        private Guna.UI.WinForms.GunaTextBox txt_MaSach;
    }
}