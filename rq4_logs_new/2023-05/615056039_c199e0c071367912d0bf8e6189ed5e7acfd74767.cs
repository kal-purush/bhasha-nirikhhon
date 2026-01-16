
namespace Farmacie
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
            this.Denumire = new System.Windows.Forms.Label();
            this.Prospect = new System.Windows.Forms.Label();
            this.SuspendLayout();
            // 
            // Denumire
            // 
            this.Denumire.AutoSize = true;
            this.Denumire.Location = new System.Drawing.Point(75, 43);
            this.Denumire.Name = "Denumire";
            this.Denumire.Size = new System.Drawing.Size(69, 17);
            this.Denumire.TabIndex = 0;
            this.Denumire.Text = "Denumire";
            // 
            // Prospect
            // 
            this.Prospect.AutoSize = true;
            this.Prospect.Location = new System.Drawing.Point(202, 43);
            this.Prospect.Name = "Prospect";
            this.Prospect.Size = new System.Drawing.Size(64, 17);
            this.Prospect.TabIndex = 1;
            this.Prospect.Text = "Prospect";
            this.Prospect.Click += new System.EventHandler(this.label2_Click);
            // 
            // Form1
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(8F, 16F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(800, 450);
            this.Controls.Add(this.Prospect);
            this.Controls.Add(this.Denumire);
            this.Name = "Form1";
            this.Text = "Farmacie";
            this.Load += new System.EventHandler(this.Form1_Load);
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Label Denumire;
        private System.Windows.Forms.Label Prospect;
    }
}