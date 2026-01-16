namespace LatvanyossagokApplication
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
            this.label_Varosnev = new System.Windows.Forms.Label();
            this.textBox_VarosNev = new System.Windows.Forms.TextBox();
            this.label_Lakossag = new System.Windows.Forms.Label();
            this.numericUpDown_Lakossag = new System.Windows.Forms.NumericUpDown();
            this.button_UjVaros = new System.Windows.Forms.Button();
            this.listBox_FelvettVarosok = new System.Windows.Forms.ListBox();
            this.label_FelvettVarosok = new System.Windows.Forms.Label();
            this.label_LatvanyossagNev = new System.Windows.Forms.Label();
            this.textBox_LatvanyossagNev = new System.Windows.Forms.TextBox();
            this.label_Leiras = new System.Windows.Forms.Label();
            this.textBox_Leiras = new System.Windows.Forms.TextBox();
            this.numericUpDown_Ar = new System.Windows.Forms.NumericUpDown();
            this.label1_Ar = new System.Windows.Forms.Label();
            this.label_Varos = new System.Windows.Forms.Label();
            this.comboBox_ValaszthatoVarosok = new System.Windows.Forms.ComboBox();
            ((System.ComponentModel.ISupportInitialize)(this.numericUpDown_Lakossag)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.numericUpDown_Ar)).BeginInit();
            this.SuspendLayout();
            // 
            // label_Varosnev
            // 
            this.label_Varosnev.AutoSize = true;
            this.label_Varosnev.Location = new System.Drawing.Point(39, 41);
            this.label_Varosnev.Name = "label_Varosnev";
            this.label_Varosnev.Size = new System.Drawing.Size(64, 13);
            this.label_Varosnev.TabIndex = 0;
            this.label_Varosnev.Text = "Város neve:";
            // 
            // textBox_VarosNev
            // 
            this.textBox_VarosNev.Location = new System.Drawing.Point(105, 38);
            this.textBox_VarosNev.Name = "textBox_VarosNev";
            this.textBox_VarosNev.Size = new System.Drawing.Size(148, 20);
            this.textBox_VarosNev.TabIndex = 1;
            this.textBox_VarosNev.Tag = "Varosnev";
            this.textBox_VarosNev.TextChanged += new System.EventHandler(this.textBox_VarosNev_TextChanged);
            // 
            // label_Lakossag
            // 
            this.label_Lakossag.AutoSize = true;
            this.label_Lakossag.Location = new System.Drawing.Point(39, 83);
            this.label_Lakossag.Name = "label_Lakossag";
            this.label_Lakossag.Size = new System.Drawing.Size(56, 13);
            this.label_Lakossag.TabIndex = 2;
            this.label_Lakossag.Tag = "Lakossag";
            this.label_Lakossag.Text = "Lakosság:";
            // 
            // numericUpDown_Lakossag
            // 
            this.numericUpDown_Lakossag.Location = new System.Drawing.Point(105, 80);
            this.numericUpDown_Lakossag.Maximum = new decimal(new int[] {
            2000000,
            0,
            0,
            0});
            this.numericUpDown_Lakossag.Minimum = new decimal(new int[] {
            1,
            0,
            0,
            0});
            this.numericUpDown_Lakossag.Name = "numericUpDown_Lakossag";
            this.numericUpDown_Lakossag.Size = new System.Drawing.Size(148, 20);
            this.numericUpDown_Lakossag.TabIndex = 3;
            this.numericUpDown_Lakossag.Value = new decimal(new int[] {
            1,
            0,
            0,
            0});
            // 
            // button_UjVaros
            // 
            this.button_UjVaros.Location = new System.Drawing.Point(42, 133);
            this.button_UjVaros.Name = "button_UjVaros";
            this.button_UjVaros.Size = new System.Drawing.Size(139, 23);
            this.button_UjVaros.TabIndex = 4;
            this.button_UjVaros.Text = "Város hozzáadása";
            this.button_UjVaros.UseVisualStyleBackColor = true;
            this.button_UjVaros.Click += new System.EventHandler(this.button_UjVaros_Click);
            // 
            // listBox_FelvettVarosok
            // 
            this.listBox_FelvettVarosok.FormattingEnabled = true;
            this.listBox_FelvettVarosok.Location = new System.Drawing.Point(40, 212);
            this.listBox_FelvettVarosok.Name = "listBox_FelvettVarosok";
            this.listBox_FelvettVarosok.Size = new System.Drawing.Size(213, 173);
            this.listBox_FelvettVarosok.TabIndex = 5;
            this.listBox_FelvettVarosok.Tag = "FelvettVarosok";
            // 
            // label_FelvettVarosok
            // 
            this.label_FelvettVarosok.AutoSize = true;
            this.label_FelvettVarosok.Location = new System.Drawing.Point(39, 186);
            this.label_FelvettVarosok.Name = "label_FelvettVarosok";
            this.label_FelvettVarosok.Size = new System.Drawing.Size(84, 13);
            this.label_FelvettVarosok.TabIndex = 6;
            this.label_FelvettVarosok.Text = "Felvett Városok:";
            // 
            // label_LatvanyossagNev
            // 
            this.label_LatvanyossagNev.AutoSize = true;
            this.label_LatvanyossagNev.Location = new System.Drawing.Point(320, 39);
            this.label_LatvanyossagNev.Name = "label_LatvanyossagNev";
            this.label_LatvanyossagNev.Size = new System.Drawing.Size(103, 13);
            this.label_LatvanyossagNev.TabIndex = 7;
            this.label_LatvanyossagNev.Tag = "latvanyossag_neve";
            this.label_LatvanyossagNev.Text = "Látványosság neve:";
            // 
            // textBox_LatvanyossagNev
            // 
            this.textBox_LatvanyossagNev.Location = new System.Drawing.Point(429, 38);
            this.textBox_LatvanyossagNev.Name = "textBox_LatvanyossagNev";
            this.textBox_LatvanyossagNev.Size = new System.Drawing.Size(212, 20);
            this.textBox_LatvanyossagNev.TabIndex = 8;
            // 
            // label_Leiras
            // 
            this.label_Leiras.AutoSize = true;
            this.label_Leiras.Location = new System.Drawing.Point(379, 76);
            this.label_Leiras.Name = "label_Leiras";
            this.label_Leiras.Size = new System.Drawing.Size(40, 13);
            this.label_Leiras.TabIndex = 9;
            this.label_Leiras.Tag = "leiras";
            this.label_Leiras.Text = "Leírás:";
            // 
            // textBox_Leiras
            // 
            this.textBox_Leiras.Location = new System.Drawing.Point(429, 73);
            this.textBox_Leiras.Multiline = true;
            this.textBox_Leiras.Name = "textBox_Leiras";
            this.textBox_Leiras.Size = new System.Drawing.Size(212, 176);
            this.textBox_Leiras.TabIndex = 10;
            // 
            // numericUpDown_Ar
            // 
            this.numericUpDown_Ar.Location = new System.Drawing.Point(429, 266);
            this.numericUpDown_Ar.Maximum = new decimal(new int[] {
            100000000,
            0,
            0,
            0});
            this.numericUpDown_Ar.Name = "numericUpDown_Ar";
            this.numericUpDown_Ar.Size = new System.Drawing.Size(157, 20);
            this.numericUpDown_Ar.TabIndex = 11;
            // 
            // label1_Ar
            // 
            this.label1_Ar.AutoSize = true;
            this.label1_Ar.Location = new System.Drawing.Point(399, 268);
            this.label1_Ar.Name = "label1_Ar";
            this.label1_Ar.Size = new System.Drawing.Size(20, 13);
            this.label1_Ar.TabIndex = 12;
            this.label1_Ar.Tag = "Ar";
            this.label1_Ar.Text = "Ár:";
            // 
            // label_Varos
            // 
            this.label_Varos.AutoSize = true;
            this.label_Varos.Location = new System.Drawing.Point(381, 311);
            this.label_Varos.Name = "label_Varos";
            this.label_Varos.Size = new System.Drawing.Size(87, 13);
            this.label_Varos.TabIndex = 13;
            this.label_Varos.Tag = "varos";
            this.label_Varos.Text = "Melyik városban:";
            // 
            // comboBox_ValaszthatoVarosok
            // 
            this.comboBox_ValaszthatoVarosok.FormattingEnabled = true;
            this.comboBox_ValaszthatoVarosok.Location = new System.Drawing.Point(476, 308);
            this.comboBox_ValaszthatoVarosok.Name = "comboBox_ValaszthatoVarosok";
            this.comboBox_ValaszthatoVarosok.Size = new System.Drawing.Size(154, 21);
            this.comboBox_ValaszthatoVarosok.TabIndex = 14;
            // 
            // Form1
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(688, 450);
            this.Controls.Add(this.comboBox_ValaszthatoVarosok);
            this.Controls.Add(this.label_Varos);
            this.Controls.Add(this.label1_Ar);
            this.Controls.Add(this.numericUpDown_Ar);
            this.Controls.Add(this.textBox_Leiras);
            this.Controls.Add(this.label_Leiras);
            this.Controls.Add(this.textBox_LatvanyossagNev);
            this.Controls.Add(this.label_LatvanyossagNev);
            this.Controls.Add(this.label_FelvettVarosok);
            this.Controls.Add(this.listBox_FelvettVarosok);
            this.Controls.Add(this.button_UjVaros);
            this.Controls.Add(this.numericUpDown_Lakossag);
            this.Controls.Add(this.label_Lakossag);
            this.Controls.Add(this.textBox_VarosNev);
            this.Controls.Add(this.label_Varosnev);
            this.Name = "Form1";
            this.Text = "Form1";
            this.Load += new System.EventHandler(this.Form1_Load);
            ((System.ComponentModel.ISupportInitialize)(this.numericUpDown_Lakossag)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.numericUpDown_Ar)).EndInit();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Label label_Varosnev;
        private System.Windows.Forms.TextBox textBox_VarosNev;
        private System.Windows.Forms.Label label_Lakossag;
        private System.Windows.Forms.NumericUpDown numericUpDown_Lakossag;
        private System.Windows.Forms.Button button_UjVaros;
        private System.Windows.Forms.ListBox listBox_FelvettVarosok;
        private System.Windows.Forms.Label label_FelvettVarosok;
        private System.Windows.Forms.Label label_LatvanyossagNev;
        private System.Windows.Forms.TextBox textBox_LatvanyossagNev;
        private System.Windows.Forms.Label label_Leiras;
        private System.Windows.Forms.TextBox textBox_Leiras;
        private System.Windows.Forms.NumericUpDown numericUpDown_Ar;
        private System.Windows.Forms.Label label1_Ar;
        private System.Windows.Forms.Label label_Varos;
        private System.Windows.Forms.ComboBox comboBox_ValaszthatoVarosok;
    }
}