namespace Hallo_World
{
    partial class Form1
    {
        /// <summary>
        /// Erforderliche Designervariable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// Verwendete Ressourcen bereinigen.
        /// </summary>
        /// <param name="disposing">True, wenn verwaltete Ressourcen gelöscht werden sollen; andernfalls False.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Vom Windows Form-Designer generierter Code

        /// <summary>
        /// Erforderliche Methode für die Designerunterstützung.
        /// Der Inhalt der Methode darf nicht mit dem Code-Editor geändert werden.
        /// </summary>
        private void InitializeComponent()
        {
            this.labUeberschrift = new System.Windows.Forms.Label();
            this.labZahlenbereich = new System.Windows.Forms.Label();
            this.txtArraybereich = new System.Windows.Forms.TextBox();
            this.cmdArraybereich = new System.Windows.Forms.Button();
            this.labWertzuweisung = new System.Windows.Forms.Label();
            this.txtWertzuweisung = new System.Windows.Forms.TextBox();
            this.cmdWertzuweisung = new System.Windows.Forms.Button();
            this.labAusgabe = new System.Windows.Forms.Label();
            this.txtGroesstezahl = new System.Windows.Forms.TextBox();
            this.labAusgabeAlles = new System.Windows.Forms.Label();
            this.labFortschritt = new System.Windows.Forms.Label();
            this.labDiezahlen = new System.Windows.Forms.Label();
            this.cmdReset = new System.Windows.Forms.Button();
            this.SuspendLayout();
            // 
            // labUeberschrift
            // 
            this.labUeberschrift.AutoSize = true;
            this.labUeberschrift.BackColor = System.Drawing.Color.Transparent;
            this.labUeberschrift.Font = new System.Drawing.Font("Microsoft Sans Serif", 14.25F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.labUeberschrift.ForeColor = System.Drawing.Color.Black;
            this.labUeberschrift.Location = new System.Drawing.Point(12, 22);
            this.labUeberschrift.Name = "labUeberschrift";
            this.labUeberschrift.Size = new System.Drawing.Size(352, 48);
            this.labUeberschrift.TabIndex = 1;
            this.labUeberschrift.Text = "Berechnung der Größten Zahl, aus \r\neinem von ihnen eingegebenen Bereich\r\n";
            // 
            // labZahlenbereich
            // 
            this.labZahlenbereich.AutoSize = true;
            this.labZahlenbereich.BackColor = System.Drawing.Color.Transparent;
            this.labZahlenbereich.Font = new System.Drawing.Font("Comic Sans MS", 11.25F, System.Drawing.FontStyle.Italic, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.labZahlenbereich.ForeColor = System.Drawing.Color.Black;
            this.labZahlenbereich.Location = new System.Drawing.Point(12, 123);
            this.labZahlenbereich.Name = "labZahlenbereich";
            this.labZahlenbereich.Size = new System.Drawing.Size(301, 22);
            this.labZahlenbereich.TabIndex = 2;
            this.labZahlenbereich.Text = "Wie viele Zahlen groß soll der Bereich sein\r\n";
            // 
            // txtArraybereich
            // 
            this.txtArraybereich.ForeColor = System.Drawing.Color.Black;
            this.txtArraybereich.Location = new System.Drawing.Point(329, 126);
            this.txtArraybereich.Name = "txtArraybereich";
            this.txtArraybereich.Size = new System.Drawing.Size(72, 20);
            this.txtArraybereich.TabIndex = 3;
            // 
            // cmdArraybereich
            // 
            this.cmdArraybereich.Cursor = System.Windows.Forms.Cursors.Hand;
            this.cmdArraybereich.Font = new System.Drawing.Font("Comic Sans MS", 11.25F, ((System.Drawing.FontStyle)((System.Drawing.FontStyle.Bold | System.Drawing.FontStyle.Italic))), System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.cmdArraybereich.ForeColor = System.Drawing.Color.Red;
            this.cmdArraybereich.Location = new System.Drawing.Point(421, 119);
            this.cmdArraybereich.Name = "cmdArraybereich";
            this.cmdArraybereich.Size = new System.Drawing.Size(29, 30);
            this.cmdArraybereich.TabIndex = 4;
            this.cmdArraybereich.Text = "X";
            this.cmdArraybereich.UseVisualStyleBackColor = true;
            this.cmdArraybereich.Click += new System.EventHandler(this.cmdArraybereich_Click);
            // 
            // labWertzuweisung
            // 
            this.labWertzuweisung.AutoSize = true;
            this.labWertzuweisung.BackColor = System.Drawing.Color.Transparent;
            this.labWertzuweisung.Font = new System.Drawing.Font("Comic Sans MS", 11.25F, System.Drawing.FontStyle.Italic, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.labWertzuweisung.ForeColor = System.Drawing.Color.Black;
            this.labWertzuweisung.Location = new System.Drawing.Point(12, 164);
            this.labWertzuweisung.Name = "labWertzuweisung";
            this.labWertzuweisung.Size = new System.Drawing.Size(179, 22);
            this.labWertzuweisung.TabIndex = 5;
            this.labWertzuweisung.Text = "Geben Sie die Zahlen ein";
            // 
            // txtWertzuweisung
            // 
            this.txtWertzuweisung.Enabled = false;
            this.txtWertzuweisung.ForeColor = System.Drawing.Color.Black;
            this.txtWertzuweisung.Location = new System.Drawing.Point(243, 167);
            this.txtWertzuweisung.Name = "txtWertzuweisung";
            this.txtWertzuweisung.Size = new System.Drawing.Size(54, 20);
            this.txtWertzuweisung.TabIndex = 6;
            // 
            // cmdWertzuweisung
            // 
            this.cmdWertzuweisung.BackColor = System.Drawing.Color.Transparent;
            this.cmdWertzuweisung.Cursor = System.Windows.Forms.Cursors.Hand;
            this.cmdWertzuweisung.Enabled = false;
            this.cmdWertzuweisung.Font = new System.Drawing.Font("Comic Sans MS", 11.25F, ((System.Drawing.FontStyle)((System.Drawing.FontStyle.Bold | System.Drawing.FontStyle.Italic))), System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.cmdWertzuweisung.ForeColor = System.Drawing.Color.Red;
            this.cmdWertzuweisung.Location = new System.Drawing.Point(314, 164);
            this.cmdWertzuweisung.Name = "cmdWertzuweisung";
            this.cmdWertzuweisung.Size = new System.Drawing.Size(29, 30);
            this.cmdWertzuweisung.TabIndex = 7;
            this.cmdWertzuweisung.Text = "X";
            this.cmdWertzuweisung.UseVisualStyleBackColor = false;
            this.cmdWertzuweisung.Click += new System.EventHandler(this.cmdWertzuweisung_Click);
            // 
            // labAusgabe
            // 
            this.labAusgabe.AutoSize = true;
            this.labAusgabe.BackColor = System.Drawing.Color.Transparent;
            this.labAusgabe.Font = new System.Drawing.Font("Comic Sans MS", 11.25F, System.Drawing.FontStyle.Italic, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.labAusgabe.ForeColor = System.Drawing.Color.Black;
            this.labAusgabe.Location = new System.Drawing.Point(12, 222);
            this.labAusgabe.Name = "labAusgabe";
            this.labAusgabe.Size = new System.Drawing.Size(155, 22);
            this.labAusgabe.TabIndex = 8;
            this.labAusgabe.Text = "Die größte Zahl ist   :";
            // 
            // txtGroesstezahl
            // 
            this.txtGroesstezahl.Enabled = false;
            this.txtGroesstezahl.ForeColor = System.Drawing.Color.Black;
            this.txtGroesstezahl.Location = new System.Drawing.Point(183, 222);
            this.txtGroesstezahl.Name = "txtGroesstezahl";
            this.txtGroesstezahl.Size = new System.Drawing.Size(72, 20);
            this.txtGroesstezahl.TabIndex = 9;
            // 
            // labAusgabeAlles
            // 
            this.labAusgabeAlles.AutoSize = true;
            this.labAusgabeAlles.BackColor = System.Drawing.Color.Transparent;
            this.labAusgabeAlles.Font = new System.Drawing.Font("Comic Sans MS", 11.25F, System.Drawing.FontStyle.Italic, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.labAusgabeAlles.ForeColor = System.Drawing.Color.Black;
            this.labAusgabeAlles.Location = new System.Drawing.Point(325, 222);
            this.labAusgabeAlles.Name = "labAusgabeAlles";
            this.labAusgabeAlles.Size = new System.Drawing.Size(189, 22);
            this.labAusgabeAlles.TabIndex = 10;
            this.labAusgabeAlles.Text = "Alle eingegebenen Zahlen:\r\n";
            this.labAusgabeAlles.Visible = false;
            // 
            // labFortschritt
            // 
            this.labFortschritt.AutoSize = true;
            this.labFortschritt.BackColor = System.Drawing.Color.Transparent;
            this.labFortschritt.Font = new System.Drawing.Font("Comic Sans MS", 11.25F, System.Drawing.FontStyle.Italic, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.labFortschritt.ForeColor = System.Drawing.Color.Black;
            this.labFortschritt.Location = new System.Drawing.Point(12, 320);
            this.labFortschritt.Name = "labFortschritt";
            this.labFortschritt.Size = new System.Drawing.Size(0, 22);
            this.labFortschritt.TabIndex = 11;
            // 
            // labDiezahlen
            // 
            this.labDiezahlen.AutoSize = true;
            this.labDiezahlen.BackColor = System.Drawing.Color.Transparent;
            this.labDiezahlen.Font = new System.Drawing.Font("Comic Sans MS", 11.25F, System.Drawing.FontStyle.Italic, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.labDiezahlen.ForeColor = System.Drawing.Color.Black;
            this.labDiezahlen.Location = new System.Drawing.Point(543, 222);
            this.labDiezahlen.Name = "labDiezahlen";
            this.labDiezahlen.Size = new System.Drawing.Size(0, 22);
            this.labDiezahlen.TabIndex = 12;
            this.labDiezahlen.Visible = false;
            // 
            // cmdReset
            // 
            this.cmdReset.Cursor = System.Windows.Forms.Cursors.Hand;
            this.cmdReset.Font = new System.Drawing.Font("Open Sans", 9F, ((System.Drawing.FontStyle)((System.Drawing.FontStyle.Bold | System.Drawing.FontStyle.Italic))), System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.cmdReset.Location = new System.Drawing.Point(235, 341);
            this.cmdReset.Name = "cmdReset";
            this.cmdReset.Size = new System.Drawing.Size(108, 21);
            this.cmdReset.TabIndex = 13;
            this.cmdReset.Text = "Neustart";
            this.cmdReset.UseVisualStyleBackColor = true;
            this.cmdReset.Visible = false;
            this.cmdReset.Click += new System.EventHandler(this.cmdReset_Click);
            // 
            // Form1
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.BackColor = System.Drawing.Color.White;
            this.ClientSize = new System.Drawing.Size(708, 374);
            this.Controls.Add(this.cmdReset);
            this.Controls.Add(this.labDiezahlen);
            this.Controls.Add(this.labFortschritt);
            this.Controls.Add(this.labAusgabeAlles);
            this.Controls.Add(this.txtGroesstezahl);
            this.Controls.Add(this.labAusgabe);
            this.Controls.Add(this.cmdWertzuweisung);
            this.Controls.Add(this.txtWertzuweisung);
            this.Controls.Add(this.labWertzuweisung);
            this.Controls.Add(this.cmdArraybereich);
            this.Controls.Add(this.txtArraybereich);
            this.Controls.Add(this.labZahlenbereich);
            this.Controls.Add(this.labUeberschrift);
            this.MaximizeBox = false;
            this.Name = "Form1";
            this.RightToLeftLayout = true;
            this.StartPosition = System.Windows.Forms.FormStartPosition.CenterScreen;
            this.Text = "Der Super Rechner";
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Label labUeberschrift;
        private System.Windows.Forms.Label labZahlenbereich;
        private System.Windows.Forms.TextBox txtArraybereich;
        private System.Windows.Forms.Button cmdArraybereich;
        private System.Windows.Forms.Label labWertzuweisung;
        private System.Windows.Forms.TextBox txtWertzuweisung;
        private System.Windows.Forms.Button cmdWertzuweisung;
        private System.Windows.Forms.Label labAusgabe;
        private System.Windows.Forms.TextBox txtGroesstezahl;
        private System.Windows.Forms.Label labAusgabeAlles;
        private System.Windows.Forms.Label labFortschritt;
        private System.Windows.Forms.Label labDiezahlen;
        private System.Windows.Forms.Button cmdReset;
    }
}
