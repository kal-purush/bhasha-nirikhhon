using System.Windows.Forms;

namespace ZmianaNazwyOkna
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
            this.Zatwierdzenie = new System.Windows.Forms.Button();
            this.Zmiana = new System.Windows.Forms.CheckBox();
            this.NowaNazwa = new System.Windows.Forms.TextBox();
            this.text = new System.Windows.Forms.Label();
            this.TextHistory = new System.Windows.Forms.Label();
            this.TextHistory = new System.Windows.Forms.Label();
            this.SuspendLayout();
            // 
            // Zatwierdzenie
            // 
            this.Zatwierdzenie.Location = new System.Drawing.Point(597, 420);
            this.Zatwierdzenie.Name = "Zatwierdzenie";
            this.Zatwierdzenie.Size = new System.Drawing.Size(75, 23);
            this.Zatwierdzenie.TabIndex = 0;
            this.Zatwierdzenie.Text = "Zatwierdź";
            this.Zatwierdzenie.UseVisualStyleBackColor = true;
            this.Zatwierdzenie.Click += new System.EventHandler(this.Zatwierdzenie_Click);
            // 
            // Zmiana
            // 
            this.Zmiana.AutoSize = true;
            this.Zmiana.Location = new System.Drawing.Point(12, 424);
            this.Zmiana.Name = "Zmiana";
            this.Zmiana.Size = new System.Drawing.Size(72, 19);
            this.Zmiana.TabIndex = 1;
            this.Zmiana.Text = "Zamiana";
            this.Zmiana.UseVisualStyleBackColor = true;
            this.Zmiana.CheckedChanged += new System.EventHandler(this.Zmiana_CheckedChanged);
            // 
            // NowaNazwa
            // 
            this.NowaNazwa.Location = new System.Drawing.Point(12, 12);
            this.NowaNazwa.Name = "NowaNazwa";
            this.NowaNazwa.Size = new System.Drawing.Size(480, 23);
            this.NowaNazwa.TabIndex = 2;
            // 
            // text
            // 
            this.text.AutoSize = true;
            this.text.Location = new System.Drawing.Point(498, 15);
            this.text.Name = "text";
            this.text.Size = new System.Drawing.Size(174, 15);
            this.text.TabIndex = 3;
            this.text.Text = "zmiana nazwy jest zablokowana";
            // 
            // TextHistory
            // 
            this.TextHistory.AutoSize = true;
            this.TextHistory.Location = new System.Drawing.Point(12, 67);
            this.TextHistory.Name = "TextHistory";
            this.TextHistory.Size = new System.Drawing.Size(0, 15);
            this.TextHistory.TabIndex = 5;
            // 
            // label1
            // 
            this.TextHistory.AutoSize = true;
            this.TextHistory.Location = new System.Drawing.Point(12, 52);
            this.TextHistory.Name = "label1";
            this.TextHistory.Size = new System.Drawing.Size(85, 15);
            this.TextHistory.TabIndex = 6;
            this.TextHistory.Text = "Historia tytułu:";
            // 
            // Form1
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(7F, 15F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(701, 450);
            this.Controls.Add(this.TextHistory);
            this.Controls.Add(this.TextHistory);
            this.Controls.Add(this.text);
            this.Controls.Add(this.NowaNazwa);
            this.Controls.Add(this.Zmiana);
            this.Controls.Add(this.Zatwierdzenie);
            this.Name = "Form1";
            this.Text = "Moja aplikacja windowsForm";
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private Button Zatwierdzenie;
        private CheckBox Zmiana;
        private TextBox NowaNazwa;
        private Label text;
        private Label TextHistory;
    }
}