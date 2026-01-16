namespace Vautour
{
    partial class menu
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
            this.cb_nbJ = new System.Windows.Forms.ComboBox();
            this.label2 = new System.Windows.Forms.Label();
            this.bt_menuJouer = new System.Windows.Forms.Button();
            this.lb_joueur = new System.Windows.Forms.Label();
            this.cb_carteJoueur = new System.Windows.Forms.ComboBox();
            this.lb_IA1 = new System.Windows.Forms.Label();
            this.cb_carteIA1 = new System.Windows.Forms.ComboBox();
            this.cb_diffIA1 = new System.Windows.Forms.ComboBox();
            this.lb_IA2 = new System.Windows.Forms.Label();
            this.cb_carteIA2 = new System.Windows.Forms.ComboBox();
            this.cb_diffIA2 = new System.Windows.Forms.ComboBox();
            this.lb_IA4 = new System.Windows.Forms.Label();
            this.cb_carteIA4 = new System.Windows.Forms.ComboBox();
            this.cb_diffIA4 = new System.Windows.Forms.ComboBox();
            this.lb_IA3 = new System.Windows.Forms.Label();
            this.cb_carteIA3 = new System.Windows.Forms.ComboBox();
            this.cb_diffIA3 = new System.Windows.Forms.ComboBox();
            this.SuspendLayout();
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Font = new System.Drawing.Font("Miramonte", 20F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label1.Location = new System.Drawing.Point(171, 31);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(86, 33);
            this.label1.TabIndex = 0;
            this.label1.Text = "Menu";
            this.label1.TextAlign = System.Drawing.ContentAlignment.TopCenter;
            // 
            // cb_nbJ
            // 
            this.cb_nbJ.BackColor = System.Drawing.SystemColors.Menu;
            this.cb_nbJ.FormattingEnabled = true;
            this.cb_nbJ.Location = new System.Drawing.Point(239, 90);
            this.cb_nbJ.Name = "cb_nbJ";
            this.cb_nbJ.Size = new System.Drawing.Size(121, 21);
            this.cb_nbJ.TabIndex = 1;
            this.cb_nbJ.SelectedIndexChanged += new System.EventHandler(this.cb_nbJ_SelectedIndexChanged);
            // 
            // label2
            // 
            this.label2.AutoSize = true;
            this.label2.Font = new System.Drawing.Font("Miramonte", 8.25F, ((System.Drawing.FontStyle)((System.Drawing.FontStyle.Bold | System.Drawing.FontStyle.Italic))), System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label2.Location = new System.Drawing.Point(72, 93);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(101, 14);
            this.label2.TabIndex = 2;
            this.label2.Text = "Nombre de joueurs";
            // 
            // bt_menuJouer
            // 
            this.bt_menuJouer.Location = new System.Drawing.Point(177, 377);
            this.bt_menuJouer.Name = "bt_menuJouer";
            this.bt_menuJouer.Size = new System.Drawing.Size(75, 23);
            this.bt_menuJouer.TabIndex = 3;
            this.bt_menuJouer.Text = "Jouer";
            this.bt_menuJouer.UseVisualStyleBackColor = true;
            this.bt_menuJouer.Click += new System.EventHandler(this.bt_menuJouer_Click);
            // 
            // lb_joueur
            // 
            this.lb_joueur.AutoSize = true;
            this.lb_joueur.Font = new System.Drawing.Font("Miramonte", 8.25F, ((System.Drawing.FontStyle)((System.Drawing.FontStyle.Bold | System.Drawing.FontStyle.Italic))), System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.lb_joueur.Location = new System.Drawing.Point(40, 170);
            this.lb_joueur.Name = "lb_joueur";
            this.lb_joueur.Size = new System.Drawing.Size(39, 14);
            this.lb_joueur.TabIndex = 4;
            this.lb_joueur.Text = "Joueur";
            // 
            // cb_carteJoueur
            // 
            this.cb_carteJoueur.FormattingEnabled = true;
            this.cb_carteJoueur.Location = new System.Drawing.Point(116, 167);
            this.cb_carteJoueur.Name = "cb_carteJoueur";
            this.cb_carteJoueur.Size = new System.Drawing.Size(121, 21);
            this.cb_carteJoueur.TabIndex = 5;
            // 
            // lb_IA1
            // 
            this.lb_IA1.AutoSize = true;
            this.lb_IA1.Font = new System.Drawing.Font("Miramonte", 8.25F, ((System.Drawing.FontStyle)((System.Drawing.FontStyle.Bold | System.Drawing.FontStyle.Italic))), System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.lb_IA1.Location = new System.Drawing.Point(40, 219);
            this.lb_IA1.Name = "lb_IA1";
            this.lb_IA1.Size = new System.Drawing.Size(26, 14);
            this.lb_IA1.TabIndex = 4;
            this.lb_IA1.Text = "IA1";
            // 
            // cb_carteIA1
            // 
            this.cb_carteIA1.FormattingEnabled = true;
            this.cb_carteIA1.Location = new System.Drawing.Point(116, 216);
            this.cb_carteIA1.Name = "cb_carteIA1";
            this.cb_carteIA1.Size = new System.Drawing.Size(121, 21);
            this.cb_carteIA1.TabIndex = 5;
            // 
            // cb_diffIA1
            // 
            this.cb_diffIA1.FormattingEnabled = true;
            this.cb_diffIA1.Location = new System.Drawing.Point(253, 216);
            this.cb_diffIA1.Name = "cb_diffIA1";
            this.cb_diffIA1.Size = new System.Drawing.Size(121, 21);
            this.cb_diffIA1.TabIndex = 5;
            // 
            // lb_IA2
            // 
            this.lb_IA2.AutoSize = true;
            this.lb_IA2.Font = new System.Drawing.Font("Miramonte", 8.25F, ((System.Drawing.FontStyle)((System.Drawing.FontStyle.Bold | System.Drawing.FontStyle.Italic))), System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.lb_IA2.Location = new System.Drawing.Point(40, 255);
            this.lb_IA2.Name = "lb_IA2";
            this.lb_IA2.Size = new System.Drawing.Size(26, 14);
            this.lb_IA2.TabIndex = 4;
            this.lb_IA2.Text = "IA2";
            // 
            // cb_carteIA2
            // 
            this.cb_carteIA2.FormattingEnabled = true;
            this.cb_carteIA2.Location = new System.Drawing.Point(116, 252);
            this.cb_carteIA2.Name = "cb_carteIA2";
            this.cb_carteIA2.Size = new System.Drawing.Size(121, 21);
            this.cb_carteIA2.TabIndex = 5;
            // 
            // cb_diffIA2
            // 
            this.cb_diffIA2.FormattingEnabled = true;
            this.cb_diffIA2.Location = new System.Drawing.Point(253, 252);
            this.cb_diffIA2.Name = "cb_diffIA2";
            this.cb_diffIA2.Size = new System.Drawing.Size(121, 21);
            this.cb_diffIA2.TabIndex = 5;
            // 
            // lb_IA4
            // 
            this.lb_IA4.AutoSize = true;
            this.lb_IA4.Font = new System.Drawing.Font("Miramonte", 8.25F, ((System.Drawing.FontStyle)((System.Drawing.FontStyle.Bold | System.Drawing.FontStyle.Italic))), System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.lb_IA4.Location = new System.Drawing.Point(40, 336);
            this.lb_IA4.Name = "lb_IA4";
            this.lb_IA4.Size = new System.Drawing.Size(26, 14);
            this.lb_IA4.TabIndex = 4;
            this.lb_IA4.Text = "IA4";
            // 
            // cb_carteIA4
            // 
            this.cb_carteIA4.FormattingEnabled = true;
            this.cb_carteIA4.Location = new System.Drawing.Point(116, 333);
            this.cb_carteIA4.Name = "cb_carteIA4";
            this.cb_carteIA4.Size = new System.Drawing.Size(121, 21);
            this.cb_carteIA4.TabIndex = 5;
            // 
            // cb_diffIA4
            // 
            this.cb_diffIA4.FormattingEnabled = true;
            this.cb_diffIA4.Location = new System.Drawing.Point(253, 333);
            this.cb_diffIA4.Name = "cb_diffIA4";
            this.cb_diffIA4.Size = new System.Drawing.Size(121, 21);
            this.cb_diffIA4.TabIndex = 5;
            // 
            // lb_IA3
            // 
            this.lb_IA3.AutoSize = true;
            this.lb_IA3.Font = new System.Drawing.Font("Miramonte", 8.25F, ((System.Drawing.FontStyle)((System.Drawing.FontStyle.Bold | System.Drawing.FontStyle.Italic))), System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.lb_IA3.Location = new System.Drawing.Point(40, 299);
            this.lb_IA3.Name = "lb_IA3";
            this.lb_IA3.Size = new System.Drawing.Size(26, 14);
            this.lb_IA3.TabIndex = 4;
            this.lb_IA3.Text = "IA3";
            // 
            // cb_carteIA3
            // 
            this.cb_carteIA3.FormattingEnabled = true;
            this.cb_carteIA3.Location = new System.Drawing.Point(116, 296);
            this.cb_carteIA3.Name = "cb_carteIA3";
            this.cb_carteIA3.Size = new System.Drawing.Size(121, 21);
            this.cb_carteIA3.TabIndex = 5;
            // 
            // cb_diffIA3
            // 
            this.cb_diffIA3.FormattingEnabled = true;
            this.cb_diffIA3.Location = new System.Drawing.Point(253, 296);
            this.cb_diffIA3.Name = "cb_diffIA3";
            this.cb_diffIA3.Size = new System.Drawing.Size(121, 21);
            this.cb_diffIA3.TabIndex = 5;
            // 
            // menu
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.BackColor = System.Drawing.Color.SlateBlue;
            this.ClientSize = new System.Drawing.Size(418, 412);
            this.Controls.Add(this.cb_diffIA3);
            this.Controls.Add(this.cb_diffIA4);
            this.Controls.Add(this.cb_diffIA2);
            this.Controls.Add(this.cb_diffIA1);
            this.Controls.Add(this.cb_carteIA3);
            this.Controls.Add(this.cb_carteIA4);
            this.Controls.Add(this.cb_carteIA2);
            this.Controls.Add(this.cb_carteIA1);
            this.Controls.Add(this.cb_carteJoueur);
            this.Controls.Add(this.lb_IA3);
            this.Controls.Add(this.lb_IA4);
            this.Controls.Add(this.lb_IA2);
            this.Controls.Add(this.lb_IA1);
            this.Controls.Add(this.lb_joueur);
            this.Controls.Add(this.bt_menuJouer);
            this.Controls.Add(this.label2);
            this.Controls.Add(this.cb_nbJ);
            this.Controls.Add(this.label1);
            this.Name = "menu";
            this.Text = "Stupid vautour";
            this.TransparencyKey = System.Drawing.Color.White;
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.ComboBox cb_nbJ;
        private System.Windows.Forms.Label label2;
        private System.Windows.Forms.Button bt_menuJouer;
        private System.Windows.Forms.Label lb_joueur;
        private System.Windows.Forms.ComboBox cb_carteJoueur;
        private System.Windows.Forms.Label lb_IA1;
        private System.Windows.Forms.ComboBox cb_carteIA1;
        private System.Windows.Forms.ComboBox cb_diffIA1;
        private System.Windows.Forms.Label lb_IA2;
        private System.Windows.Forms.ComboBox cb_carteIA2;
        private System.Windows.Forms.ComboBox cb_diffIA2;
        private System.Windows.Forms.Label lb_IA4;
        private System.Windows.Forms.ComboBox cb_carteIA4;
        private System.Windows.Forms.ComboBox cb_diffIA4;
        private System.Windows.Forms.Label lb_IA3;
        private System.Windows.Forms.ComboBox cb_carteIA3;
        private System.Windows.Forms.ComboBox cb_diffIA3;
    }
}