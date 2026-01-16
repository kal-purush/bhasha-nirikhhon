namespace VideoRecordWin
{
    partial class FrmBilgi
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
            this.label1 = new System.Windows.Forms.Label();
            this.label2 = new System.Windows.Forms.Label();
            this.label3 = new System.Windows.Forms.Label();
            this.groupBox1 = new System.Windows.Forms.GroupBox();
            this.lbltarih = new System.Windows.Forms.Label();
            this.lblsaat = new System.Windows.Forms.Label();
            this.lblkalite = new System.Windows.Forms.Label();
            this.lblkare = new System.Windows.Forms.Label();
            this.label5 = new System.Windows.Forms.Label();
            this.groupBox1.SuspendLayout();
            this.SuspendLayout();
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Location = new System.Drawing.Point(29, 32);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(65, 13);
            this.label1.TabIndex = 0;
            this.label1.Text = "Kayıt Tarihi :";
            // 
            // label2
            // 
            this.label2.AutoSize = true;
            this.label2.Location = new System.Drawing.Point(29, 62);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(63, 13);
            this.label2.TabIndex = 1;
            this.label2.Text = "Kayıt Saati :";
            // 
            // label3
            // 
            this.label3.AutoSize = true;
            this.label3.Location = new System.Drawing.Point(18, 93);
            this.label3.Name = "label3";
            this.label3.Size = new System.Drawing.Size(72, 13);
            this.label3.TabIndex = 2;
            this.label3.Text = "Kayıt Kalitesi :";
            // 
            // groupBox1
            // 
            this.groupBox1.Controls.Add(this.lblkare);
            this.groupBox1.Controls.Add(this.label5);
            this.groupBox1.Controls.Add(this.lblkalite);
            this.groupBox1.Controls.Add(this.lblsaat);
            this.groupBox1.Controls.Add(this.lbltarih);
            this.groupBox1.Controls.Add(this.label1);
            this.groupBox1.Controls.Add(this.label3);
            this.groupBox1.Controls.Add(this.label2);
            this.groupBox1.Location = new System.Drawing.Point(12, 12);
            this.groupBox1.Name = "groupBox1";
            this.groupBox1.Size = new System.Drawing.Size(335, 141);
            this.groupBox1.TabIndex = 3;
            this.groupBox1.TabStop = false;
            this.groupBox1.Text = "BİLGİ";
            // 
            // lbltarih
            // 
            this.lbltarih.AutoSize = true;
            this.lbltarih.Location = new System.Drawing.Point(110, 32);
            this.lbltarih.Name = "lbltarih";
            this.lbltarih.Size = new System.Drawing.Size(27, 13);
            this.lbltarih.TabIndex = 3;
            this.lbltarih.Text = "tarih";
            // 
            // lblsaat
            // 
            this.lblsaat.AutoSize = true;
            this.lblsaat.Location = new System.Drawing.Point(110, 62);
            this.lblsaat.Name = "lblsaat";
            this.lblsaat.Size = new System.Drawing.Size(27, 13);
            this.lblsaat.TabIndex = 4;
            this.lblsaat.Text = "saat";
            // 
            // lblkalite
            // 
            this.lblkalite.AutoSize = true;
            this.lblkalite.Location = new System.Drawing.Point(110, 93);
            this.lblkalite.Name = "lblkalite";
            this.lblkalite.Size = new System.Drawing.Size(32, 13);
            this.lblkalite.TabIndex = 5;
            this.lblkalite.Text = "kalite";
            // 
            // lblkare
            // 
            this.lblkare.AutoSize = true;
            this.lblkare.Location = new System.Drawing.Point(279, 32);
            this.lblkare.Name = "lblkare";
            this.lblkare.Size = new System.Drawing.Size(28, 13);
            this.lblkare.TabIndex = 7;
            this.lblkare.Text = "kare";
            // 
            // label5
            // 
            this.label5.AutoSize = true;
            this.label5.Location = new System.Drawing.Point(204, 32);
            this.label5.Name = "label5";
            this.label5.Size = new System.Drawing.Size(65, 13);
            this.label5.TabIndex = 6;
            this.label5.Text = "Kare Sayısı :";
            // 
            // FrmBilgi
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(367, 167);
            this.Controls.Add(this.groupBox1);
            this.MaximizeBox = false;
            this.MinimizeBox = false;
            this.Name = "FrmBilgi";
            this.Text = "VİDEO HAKKINDA";
            this.Load += new System.EventHandler(this.FrmBilgi_Load);
            this.groupBox1.ResumeLayout(false);
            this.groupBox1.PerformLayout();
            this.ResumeLayout(false);

        }

        #endregion

        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.Label label2;
        private System.Windows.Forms.Label label3;
        private System.Windows.Forms.GroupBox groupBox1;
        private System.Windows.Forms.Label lblkalite;
        private System.Windows.Forms.Label lblsaat;
        private System.Windows.Forms.Label lbltarih;
        private System.Windows.Forms.Label lblkare;
        private System.Windows.Forms.Label label5;
    }
}