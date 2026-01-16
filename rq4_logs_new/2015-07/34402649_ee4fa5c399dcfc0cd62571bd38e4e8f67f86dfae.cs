namespace SupplyChainManagementUI
{
    partial class DynamischeTabelle
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
            this.Endprodukt = new System.Windows.Forms.Label();
            this.P1 = new System.Windows.Forms.Label();
            this.P2 = new System.Windows.Forms.Label();
            this.P3 = new System.Windows.Forms.Label();
            this.label1 = new System.Windows.Forms.Label();
            this.VertrW_p1 = new System.Windows.Forms.TextBox();
            this.VertrW_p2 = new System.Windows.Forms.TextBox();
            this.VertrW_p3 = new System.Windows.Forms.TextBox();
            this.GeplLagerbesteand = new System.Windows.Forms.Label();
            this.textBox1 = new System.Windows.Forms.TextBox();
            this.textBox2 = new System.Windows.Forms.TextBox();
            this.textBox3 = new System.Windows.Forms.TextBox();
            this.Berechnen = new System.Windows.Forms.Button();
            this.SuspendLayout();
            // 
            // Endprodukt
            // 
            this.Endprodukt.AutoSize = true;
            this.Endprodukt.Location = new System.Drawing.Point(41, 33);
            this.Endprodukt.Name = "Endprodukt";
            this.Endprodukt.Size = new System.Drawing.Size(62, 13);
            this.Endprodukt.TabIndex = 0;
            this.Endprodukt.Text = "Endprodukt";
            // 
            // P1
            // 
            this.P1.AutoSize = true;
            this.P1.Location = new System.Drawing.Point(41, 68);
            this.P1.Name = "P1";
            this.P1.Size = new System.Drawing.Size(20, 13);
            this.P1.TabIndex = 1;
            this.P1.Text = "P1";
            // 
            // P2
            // 
            this.P2.AutoSize = true;
            this.P2.Location = new System.Drawing.Point(41, 97);
            this.P2.Name = "P2";
            this.P2.Size = new System.Drawing.Size(20, 13);
            this.P2.TabIndex = 2;
            this.P2.Text = "P2";
            // 
            // P3
            // 
            this.P3.AutoSize = true;
            this.P3.Location = new System.Drawing.Point(41, 128);
            this.P3.Name = "P3";
            this.P3.Size = new System.Drawing.Size(20, 13);
            this.P3.TabIndex = 3;
            this.P3.Text = "P3";
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Location = new System.Drawing.Point(122, 33);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(85, 13);
            this.label1.TabIndex = 4;
            this.label1.Text = "Vertriebswunsch";
            // 
            // VertrW_p1
            // 
            this.VertrW_p1.Location = new System.Drawing.Point(125, 68);
            this.VertrW_p1.Name = "VertrW_p1";
            this.VertrW_p1.Size = new System.Drawing.Size(100, 20);
            this.VertrW_p1.TabIndex = 5;
            // 
            // VertrW_p2
            // 
            this.VertrW_p2.Location = new System.Drawing.Point(125, 97);
            this.VertrW_p2.Name = "VertrW_p2";
            this.VertrW_p2.Size = new System.Drawing.Size(100, 20);
            this.VertrW_p2.TabIndex = 6;
            // 
            // VertrW_p3
            // 
            this.VertrW_p3.Location = new System.Drawing.Point(125, 128);
            this.VertrW_p3.Name = "VertrW_p3";
            this.VertrW_p3.Size = new System.Drawing.Size(100, 20);
            this.VertrW_p3.TabIndex = 7;
            // 
            // GeplLagerbesteand
            // 
            this.GeplLagerbesteand.AutoSize = true;
            this.GeplLagerbesteand.Location = new System.Drawing.Point(250, 33);
            this.GeplLagerbesteand.Name = "GeplLagerbesteand";
            this.GeplLagerbesteand.Size = new System.Drawing.Size(121, 13);
            this.GeplLagerbesteand.TabIndex = 8;
            this.GeplLagerbesteand.Text = "Geplanter Lagerbestand";
            // 
            // textBox1
            // 
            this.textBox1.Location = new System.Drawing.Point(253, 68);
            this.textBox1.Name = "textBox1";
            this.textBox1.Size = new System.Drawing.Size(100, 20);
            this.textBox1.TabIndex = 9;
            // 
            // textBox2
            // 
            this.textBox2.Location = new System.Drawing.Point(253, 97);
            this.textBox2.Name = "textBox2";
            this.textBox2.Size = new System.Drawing.Size(100, 20);
            this.textBox2.TabIndex = 10;
            // 
            // textBox3
            // 
            this.textBox3.Location = new System.Drawing.Point(253, 127);
            this.textBox3.Name = "textBox3";
            this.textBox3.Size = new System.Drawing.Size(100, 20);
            this.textBox3.TabIndex = 11;
            // 
            // Berechnen
            // 
            this.Berechnen.Location = new System.Drawing.Point(472, 124);
            this.Berechnen.Name = "Berechnen";
            this.Berechnen.Size = new System.Drawing.Size(75, 23);
            this.Berechnen.TabIndex = 12;
            this.Berechnen.Text = "Berechnen";
            this.Berechnen.UseVisualStyleBackColor = true;
            this.Berechnen.Click += new System.EventHandler(this.Berechnen_Click);
            // 
            // DynamischeTabelle
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(577, 192);
            this.Controls.Add(this.Berechnen);
            this.Controls.Add(this.textBox3);
            this.Controls.Add(this.textBox2);
            this.Controls.Add(this.textBox1);
            this.Controls.Add(this.GeplLagerbesteand);
            this.Controls.Add(this.VertrW_p3);
            this.Controls.Add(this.VertrW_p2);
            this.Controls.Add(this.VertrW_p1);
            this.Controls.Add(this.label1);
            this.Controls.Add(this.P3);
            this.Controls.Add(this.P2);
            this.Controls.Add(this.P1);
            this.Controls.Add(this.Endprodukt);
            this.Name = "DynamischeTabelle";
            this.Text = "Planung Endprodukte";
            this.Load += new System.EventHandler(this.Form1_Load);
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Label Endprodukt;
        private System.Windows.Forms.Label P1;
        private System.Windows.Forms.Label P2;
        private System.Windows.Forms.Label P3;
        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.TextBox VertrW_p1;
        private System.Windows.Forms.TextBox VertrW_p2;
        private System.Windows.Forms.TextBox VertrW_p3;
        private System.Windows.Forms.Label GeplLagerbesteand;
        private System.Windows.Forms.TextBox textBox1;
        private System.Windows.Forms.TextBox textBox2;
        private System.Windows.Forms.TextBox textBox3;
        private System.Windows.Forms.Button Berechnen;


    }
}