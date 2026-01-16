namespace Billing
{
    partial class Receipt
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
            this.Logo = new System.Windows.Forms.PictureBox();
            this.InfoGrp = new System.Windows.Forms.GroupBox();
            this.BillGrp = new System.Windows.Forms.GroupBox();
            this.panel1 = new System.Windows.Forms.Panel();
            this.label7 = new System.Windows.Forms.Label();
            this.label8 = new System.Windows.Forms.Label();
            this.label10 = new System.Windows.Forms.Label();
            this.label9 = new System.Windows.Forms.Label();
            this.Bill = new System.Windows.Forms.Panel();
            this.TotalGrp = new System.Windows.Forms.GroupBox();
            this.KHR = new System.Windows.Forms.Label();
            this.USD = new System.Windows.Forms.Label();
            this.label12 = new System.Windows.Forms.Label();
            this.label11 = new System.Windows.Forms.Label();
            this.Info = new System.Windows.Forms.Panel();
            ((System.ComponentModel.ISupportInitialize)(this.Logo)).BeginInit();
            this.InfoGrp.SuspendLayout();
            this.BillGrp.SuspendLayout();
            this.panel1.SuspendLayout();
            this.TotalGrp.SuspendLayout();
            this.SuspendLayout();
            // 
            // Logo
            // 
            this.Logo.BackgroundImageLayout = System.Windows.Forms.ImageLayout.Stretch;
            this.Logo.ImageLocation = "";
            this.Logo.Location = new System.Drawing.Point(13, 14);
            this.Logo.Margin = new System.Windows.Forms.Padding(4, 5, 4, 5);
            this.Logo.Name = "Logo";
            this.Logo.Size = new System.Drawing.Size(458, 71);
            this.Logo.SizeMode = System.Windows.Forms.PictureBoxSizeMode.StretchImage;
            this.Logo.TabIndex = 0;
            this.Logo.TabStop = false;
            // 
            // InfoGrp
            // 
            this.InfoGrp.AutoSize = true;
            this.InfoGrp.Controls.Add(this.Info);
            this.InfoGrp.Font = new System.Drawing.Font("Tahoma", 12F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.InfoGrp.Location = new System.Drawing.Point(12, 93);
            this.InfoGrp.Name = "InfoGrp";
            this.InfoGrp.Size = new System.Drawing.Size(458, 38);
            this.InfoGrp.TabIndex = 2;
            this.InfoGrp.TabStop = false;
            this.InfoGrp.Text = "INFO";
            // 
            // BillGrp
            // 
            this.BillGrp.AutoSize = true;
            this.BillGrp.Controls.Add(this.panel1);
            this.BillGrp.Controls.Add(this.Bill);
            this.BillGrp.Font = new System.Drawing.Font("Tahoma", 12F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.BillGrp.Location = new System.Drawing.Point(12, 188);
            this.BillGrp.Name = "BillGrp";
            this.BillGrp.Size = new System.Drawing.Size(458, 74);
            this.BillGrp.TabIndex = 3;
            this.BillGrp.TabStop = false;
            this.BillGrp.Text = "BILL";
            // 
            // panel1
            // 
            this.panel1.AutoSize = true;
            this.panel1.BackColor = System.Drawing.Color.Transparent;
            this.panel1.Controls.Add(this.label7);
            this.panel1.Controls.Add(this.label8);
            this.panel1.Controls.Add(this.label10);
            this.panel1.Controls.Add(this.label9);
            this.panel1.Dock = System.Windows.Forms.DockStyle.Top;
            this.panel1.Location = new System.Drawing.Point(3, 23);
            this.panel1.Name = "panel1";
            this.panel1.Size = new System.Drawing.Size(452, 19);
            this.panel1.TabIndex = 5;
            // 
            // label7
            // 
            this.label7.AutoSize = true;
            this.label7.Font = new System.Drawing.Font("Tahoma", 12F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label7.Location = new System.Drawing.Point(3, 0);
            this.label7.Name = "label7";
            this.label7.Size = new System.Drawing.Size(101, 19);
            this.label7.TabIndex = 0;
            this.label7.Text = "Description";
            // 
            // label8
            // 
            this.label8.AutoSize = true;
            this.label8.Font = new System.Drawing.Font("Tahoma", 12F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label8.Location = new System.Drawing.Point(220, 0);
            this.label8.Name = "label8";
            this.label8.Size = new System.Drawing.Size(28, 19);
            this.label8.TabIndex = 1;
            this.label8.Text = "Qt";
            // 
            // label10
            // 
            this.label10.AutoSize = true;
            this.label10.Font = new System.Drawing.Font("Tahoma", 12F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label10.Location = new System.Drawing.Point(371, 0);
            this.label10.Name = "label10";
            this.label10.Size = new System.Drawing.Size(73, 19);
            this.label10.TabIndex = 3;
            this.label10.Text = "Amount";
            // 
            // label9
            // 
            this.label9.AutoSize = true;
            this.label9.Font = new System.Drawing.Font("Tahoma", 12F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label9.Location = new System.Drawing.Point(276, 0);
            this.label9.Name = "label9";
            this.label9.Size = new System.Drawing.Size(89, 19);
            this.label9.TabIndex = 2;
            this.label9.Text = "Unit Price";
            // 
            // Bill
            // 
            this.Bill.AutoSize = true;
            this.Bill.BackColor = System.Drawing.Color.Transparent;
            this.Bill.Dock = System.Windows.Forms.DockStyle.Bottom;
            this.Bill.Font = new System.Drawing.Font("Tahoma", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.Bill.Location = new System.Drawing.Point(3, 61);
            this.Bill.Margin = new System.Windows.Forms.Padding(0);
            this.Bill.Name = "Bill";
            this.Bill.Padding = new System.Windows.Forms.Padding(0, 10, 0, 0);
            this.Bill.Size = new System.Drawing.Size(452, 10);
            this.Bill.TabIndex = 4;
            // 
            // TotalGrp
            // 
            this.TotalGrp.Controls.Add(this.KHR);
            this.TotalGrp.Controls.Add(this.USD);
            this.TotalGrp.Controls.Add(this.label12);
            this.TotalGrp.Controls.Add(this.label11);
            this.TotalGrp.Font = new System.Drawing.Font("Tahoma", 12F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.TotalGrp.Location = new System.Drawing.Point(238, 265);
            this.TotalGrp.Margin = new System.Windows.Forms.Padding(0);
            this.TotalGrp.Name = "TotalGrp";
            this.TotalGrp.Size = new System.Drawing.Size(232, 93);
            this.TotalGrp.TabIndex = 4;
            this.TotalGrp.TabStop = false;
            this.TotalGrp.Text = "Total";
            // 
            // KHR
            // 
            this.KHR.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Right)));
            this.KHR.AutoSize = true;
            this.KHR.Font = new System.Drawing.Font("Tahoma", 15.75F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.KHR.Location = new System.Drawing.Point(119, 57);
            this.KHR.Name = "KHR";
            this.KHR.Size = new System.Drawing.Size(83, 25);
            this.KHR.TabIndex = 3;
            this.KHR.Text = "58 240";
            this.KHR.TextAlign = System.Drawing.ContentAlignment.MiddleRight;
            // 
            // USD
            // 
            this.USD.AutoSize = true;
            this.USD.Font = new System.Drawing.Font("Tahoma", 15.75F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.USD.Location = new System.Drawing.Point(119, 23);
            this.USD.Name = "USD";
            this.USD.Size = new System.Drawing.Size(71, 25);
            this.USD.TabIndex = 2;
            this.USD.Text = "14.56";
            // 
            // label12
            // 
            this.label12.AutoSize = true;
            this.label12.Font = new System.Drawing.Font("Tahoma", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label12.Location = new System.Drawing.Point(52, 62);
            this.label12.Name = "label12";
            this.label12.Size = new System.Drawing.Size(39, 19);
            this.label12.TabIndex = 1;
            this.label12.Text = "KHR";
            // 
            // label11
            // 
            this.label11.AutoSize = true;
            this.label11.Font = new System.Drawing.Font("Tahoma", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label11.Location = new System.Drawing.Point(52, 27);
            this.label11.Name = "label11";
            this.label11.Size = new System.Drawing.Size(40, 19);
            this.label11.TabIndex = 0;
            this.label11.Text = "USD";
            // 
            // Info
            // 
            this.Info.AutoSize = true;
            this.Info.Dock = System.Windows.Forms.DockStyle.Bottom;
            this.Info.Font = new System.Drawing.Font("Tahoma", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.Info.Location = new System.Drawing.Point(3, 25);
            this.Info.Name = "Info";
            this.Info.Padding = new System.Windows.Forms.Padding(0, 10, 0, 0);
            this.Info.Size = new System.Drawing.Size(452, 10);
            this.Info.TabIndex = 0;
            // 
            // Receipt
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(9F, 19F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.AutoSize = true;
            this.ClientSize = new System.Drawing.Size(484, 370);
            this.Controls.Add(this.TotalGrp);
            this.Controls.Add(this.BillGrp);
            this.Controls.Add(this.InfoGrp);
            this.Controls.Add(this.Logo);
            this.Font = new System.Drawing.Font("Tahoma", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.Margin = new System.Windows.Forms.Padding(4, 5, 4, 5);
            this.Name = "Receipt";
            this.Text = "Receipt";
            ((System.ComponentModel.ISupportInitialize)(this.Logo)).EndInit();
            this.InfoGrp.ResumeLayout(false);
            this.InfoGrp.PerformLayout();
            this.BillGrp.ResumeLayout(false);
            this.BillGrp.PerformLayout();
            this.panel1.ResumeLayout(false);
            this.panel1.PerformLayout();
            this.TotalGrp.ResumeLayout(false);
            this.TotalGrp.PerformLayout();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.PictureBox Logo;
        private System.Windows.Forms.GroupBox InfoGrp;
        private System.Windows.Forms.GroupBox BillGrp;
        private System.Windows.Forms.Label label10;
        private System.Windows.Forms.Label label9;
        private System.Windows.Forms.Label label8;
        private System.Windows.Forms.Label label7;
        private System.Windows.Forms.GroupBox TotalGrp;
        private System.Windows.Forms.Label KHR;
        private System.Windows.Forms.Label USD;
        private System.Windows.Forms.Label label12;
        private System.Windows.Forms.Label label11;
        private System.Windows.Forms.Panel Bill;
        private System.Windows.Forms.Panel panel1;
        private System.Windows.Forms.Panel Info;
    }
}
