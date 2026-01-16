using System.Windows.Forms;

namespace KalkulatorImprezy
{
    partial class Form1
    {
        /// <summary>
        ///  Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        ///  Clean up any resources being used.
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
        ///  Required method for Designer support - do not modify
        ///  the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            this.IleOsob = new System.Windows.Forms.NumericUpDown();
            this.Dekoracje = new System.Windows.Forms.CheckBox();
            this.Jedzenie = new System.Windows.Forms.CheckBox();
            this.Koszt = new System.Windows.Forms.Label();
            ((System.ComponentModel.ISupportInitialize)(this.IleOsob)).BeginInit();
            this.SuspendLayout();
            // 
            // IleOsob
            // 
            this.IleOsob.Location = new System.Drawing.Point(12, 12);
            this.IleOsob.Name = "IleOsob";
            this.IleOsob.Size = new System.Drawing.Size(233, 23);
            this.IleOsob.TabIndex = 0;
            this.IleOsob.ValueChanged += new System.EventHandler(this.IleOsob_ValueChanged);
            // 
            // Dekoracje
            // 
            this.Dekoracje.AutoSize = true;
            this.Dekoracje.Location = new System.Drawing.Point(12, 63);
            this.Dekoracje.Name = "Dekoracje";
            this.Dekoracje.Size = new System.Drawing.Size(78, 19);
            this.Dekoracje.TabIndex = 1;
            this.Dekoracje.Text = "Dekoracje";
            this.Dekoracje.UseVisualStyleBackColor = true;
            this.Dekoracje.CheckedChanged += new System.EventHandler(this.Dekoracje_CheckedChanged);
            // 
            // Jedzenie
            // 
            this.Jedzenie.AutoSize = true;
            this.Jedzenie.Location = new System.Drawing.Point(12, 88);
            this.Jedzenie.Name = "Jedzenie";
            this.Jedzenie.Size = new System.Drawing.Size(112, 19);
            this.Jedzenie.TabIndex = 2;
            this.Jedzenie.Text = "Zdrowe jedzenie";
            this.Jedzenie.UseVisualStyleBackColor = true;
            this.Jedzenie.CheckedChanged += new System.EventHandler(this.Jedzenie_CheckedChanged);
            // 
            // Koszt
            // 
            this.Koszt.AutoSize = true;
            this.Koszt.Location = new System.Drawing.Point(12, 169);
            this.Koszt.Name = "Koszt";
            this.Koszt.Size = new System.Drawing.Size(38, 15);
            this.Koszt.TabIndex = 4;
            this.Koszt.Text = "Koszt:";
            // 
            // Form1
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(7F, 15F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(291, 193);
            this.Controls.Add(this.Koszt);
            this.Controls.Add(this.Jedzenie);
            this.Controls.Add(this.Dekoracje);
            this.Controls.Add(this.IleOsob);
            this.Name = "Form1";
            this.Text = "Form1";
            ((System.ComponentModel.ISupportInitialize)(this.IleOsob)).EndInit();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private NumericUpDown IleOsob;
        private CheckBox Dekoracje;
        private CheckBox Jedzenie;
        private Label Koszt;
    }
}