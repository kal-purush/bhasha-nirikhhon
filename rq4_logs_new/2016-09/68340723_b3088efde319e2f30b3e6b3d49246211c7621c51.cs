namespace Essaitp2
{
    partial class Form1
    {
        /// <summary>
        /// Variable nécessaire au concepteur.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// Nettoyage des ressources utilisées.
        /// </summary>
        /// <param name="disposing">true si les ressources managées doivent être supprimées ; sinon, false.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Code généré par le Concepteur Windows Form

        /// <summary>
        /// Méthode requise pour la prise en charge du concepteur - ne modifiez pas
        /// le contenu de cette méthode avec l'éditeur de code.
        /// </summary>
        private void InitializeComponent()
        {
            this.components = new System.ComponentModel.Container();
            System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(Form1));
            this.imgJoueur = new System.Windows.Forms.PictureBox();
            this.imgOrdi = new System.Windows.Forms.PictureBox();
            this.lblVictoireNom = new System.Windows.Forms.Label();
            this.label2 = new System.Windows.Forms.Label();
            this.lblVictoirejoueur = new System.Windows.Forms.Label();
            this.lblVictoireordi = new System.Windows.Forms.Label();
            this.label5 = new System.Windows.Forms.Label();
            this.label6 = new System.Windows.Forms.Label();
            this.lblNul = new System.Windows.Forms.Label();
            this.label7 = new System.Windows.Forms.Label();
            this.chkAnimation = new System.Windows.Forms.CheckBox();
            this.lblconfrontation = new System.Windows.Forms.Label();
            this.btnJouer = new System.Windows.Forms.Button();
            this.btnRecommencer = new System.Windows.Forms.Button();
            this.optRoche = new System.Windows.Forms.RadioButton();
            this.optPapier = new System.Windows.Forms.RadioButton();
            this.optAllumette = new System.Windows.Forms.RadioButton();
            this.optCiseaux = new System.Windows.Forms.RadioButton();
            this.timer1 = new System.Windows.Forms.Timer(this.components);
            this.lblGagperdnul = new System.Windows.Forms.Label();
            ((System.ComponentModel.ISupportInitialize)(this.imgJoueur)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.imgOrdi)).BeginInit();
            this.SuspendLayout();
            // 
            // imgJoueur
            // 
            this.imgJoueur.BackColor = System.Drawing.Color.Transparent;
            this.imgJoueur.BorderStyle = System.Windows.Forms.BorderStyle.Fixed3D;
            this.imgJoueur.Image = ((System.Drawing.Image)(resources.GetObject("imgJoueur.Image")));
            this.imgJoueur.Location = new System.Drawing.Point(22, 108);
            this.imgJoueur.Name = "imgJoueur";
            this.imgJoueur.Size = new System.Drawing.Size(220, 200);
            this.imgJoueur.TabIndex = 5;
            this.imgJoueur.TabStop = false;
            // 
            // imgOrdi
            // 
            this.imgOrdi.BackColor = System.Drawing.Color.Transparent;
            this.imgOrdi.BorderStyle = System.Windows.Forms.BorderStyle.Fixed3D;
            this.imgOrdi.Image = ((System.Drawing.Image)(resources.GetObject("imgOrdi.Image")));
            this.imgOrdi.Location = new System.Drawing.Point(389, 108);
            this.imgOrdi.Name = "imgOrdi";
            this.imgOrdi.Size = new System.Drawing.Size(220, 200);
            this.imgOrdi.TabIndex = 6;
            this.imgOrdi.TabStop = false;
            // 
            // lblVictoireNom
            // 
            this.lblVictoireNom.BackColor = System.Drawing.Color.Transparent;
            this.lblVictoireNom.Font = new System.Drawing.Font("Century Gothic", 12F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.lblVictoireNom.ForeColor = System.Drawing.SystemColors.ButtonFace;
            this.lblVictoireNom.Location = new System.Drawing.Point(22, 51);
            this.lblVictoireNom.Name = "lblVictoireNom";
            this.lblVictoireNom.Size = new System.Drawing.Size(220, 23);
            this.lblVictoireNom.TabIndex = 8;
            this.lblVictoireNom.Text = "Victoires joueur";
            this.lblVictoireNom.TextAlign = System.Drawing.ContentAlignment.MiddleCenter;
            // 
            // label2
            // 
            this.label2.BackColor = System.Drawing.Color.Transparent;
            this.label2.Font = new System.Drawing.Font("Century Gothic", 12F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label2.ForeColor = System.Drawing.SystemColors.ButtonFace;
            this.label2.Location = new System.Drawing.Point(425, 51);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(158, 23);
            this.label2.TabIndex = 9;
            this.label2.Text = "Victoires ordinateur";
            // 
            // lblVictoirejoueur
            // 
            this.lblVictoirejoueur.BackColor = System.Drawing.Color.Transparent;
            this.lblVictoirejoueur.Font = new System.Drawing.Font("Century Gothic", 18F, ((System.Drawing.FontStyle)((System.Drawing.FontStyle.Bold | System.Drawing.FontStyle.Italic))), System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.lblVictoirejoueur.ForeColor = System.Drawing.SystemColors.ControlLight;
            this.lblVictoirejoueur.Location = new System.Drawing.Point(112, 74);
            this.lblVictoirejoueur.Name = "lblVictoirejoueur";
            this.lblVictoirejoueur.Size = new System.Drawing.Size(43, 31);
            this.lblVictoirejoueur.TabIndex = 2;
            this.lblVictoirejoueur.Text = "0";
            this.lblVictoirejoueur.TextAlign = System.Drawing.ContentAlignment.MiddleCenter;
            // 
            // lblVictoireordi
            // 
            this.lblVictoireordi.BackColor = System.Drawing.Color.Transparent;
            this.lblVictoireordi.Font = new System.Drawing.Font("Century Gothic", 18F, ((System.Drawing.FontStyle)((System.Drawing.FontStyle.Bold | System.Drawing.FontStyle.Italic))), System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.lblVictoireordi.ForeColor = System.Drawing.SystemColors.ControlLight;
            this.lblVictoireordi.Location = new System.Drawing.Point(478, 75);
            this.lblVictoireordi.Name = "lblVictoireordi";
            this.lblVictoireordi.Size = new System.Drawing.Size(43, 31);
            this.lblVictoireordi.TabIndex = 11;
            this.lblVictoireordi.Text = "0";
            this.lblVictoireordi.TextAlign = System.Drawing.ContentAlignment.MiddleCenter;
            // 
            // label5
            // 
            this.label5.BackColor = System.Drawing.Color.Transparent;
            this.label5.Font = new System.Drawing.Font("Century Gothic", 27.75F, ((System.Drawing.FontStyle)(((System.Drawing.FontStyle.Bold | System.Drawing.FontStyle.Italic) 
                | System.Drawing.FontStyle.Underline))), System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label5.ForeColor = System.Drawing.SystemColors.ControlLight;
            this.label5.Location = new System.Drawing.Point(244, 176);
            this.label5.Name = "label5";
            this.label5.Size = new System.Drawing.Size(145, 60);
            this.label5.TabIndex = 12;
            this.label5.Text = "VS";
            this.label5.TextAlign = System.Drawing.ContentAlignment.MiddleCenter;
            // 
            // label6
            // 
            this.label6.BackColor = System.Drawing.Color.Transparent;
            this.label6.Font = new System.Drawing.Font("Century Gothic", 11.25F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label6.ForeColor = System.Drawing.SystemColors.ControlLight;
            this.label6.Location = new System.Drawing.Point(259, 435);
            this.label6.Name = "label6";
            this.label6.Size = new System.Drawing.Size(115, 47);
            this.label6.TabIndex = 13;
            this.label6.Text = "Confrontations   Nulles";
            this.label6.TextAlign = System.Drawing.ContentAlignment.MiddleCenter;
            // 
            // lblNul
            // 
            this.lblNul.BackColor = System.Drawing.Color.Transparent;
            this.lblNul.Font = new System.Drawing.Font("Century Gothic", 18F, ((System.Drawing.FontStyle)((System.Drawing.FontStyle.Bold | System.Drawing.FontStyle.Italic))), System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.lblNul.ForeColor = System.Drawing.SystemColors.ControlLight;
            this.lblNul.Location = new System.Drawing.Point(265, 404);
            this.lblNul.Name = "lblNul";
            this.lblNul.Size = new System.Drawing.Size(109, 31);
            this.lblNul.TabIndex = 17;
            this.lblNul.Text = "0";
            this.lblNul.TextAlign = System.Drawing.ContentAlignment.MiddleCenter;
            // 
            // label7
            // 
            this.label7.BackColor = System.Drawing.Color.Transparent;
            this.label7.Font = new System.Drawing.Font("Century Gothic", 15.75F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label7.ForeColor = System.Drawing.SystemColors.ControlLight;
            this.label7.Location = new System.Drawing.Point(239, 12);
            this.label7.Name = "label7";
            this.label7.Size = new System.Drawing.Size(162, 62);
            this.label7.TabIndex = 18;
            this.label7.Text = "Confrontations Restantes";
            this.label7.TextAlign = System.Drawing.ContentAlignment.MiddleCenter;
            // 
            // chkAnimation
            // 
            this.chkAnimation.AutoSize = true;
            this.chkAnimation.BackColor = System.Drawing.Color.Transparent;
            this.chkAnimation.Font = new System.Drawing.Font("Century Gothic", 9.75F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.chkAnimation.ForeColor = System.Drawing.SystemColors.ControlLight;
            this.chkAnimation.Location = new System.Drawing.Point(12, 12);
            this.chkAnimation.Name = "chkAnimation";
            this.chkAnimation.Size = new System.Drawing.Size(94, 21);
            this.chkAnimation.TabIndex = 19;
            this.chkAnimation.Text = "Animation";
            this.chkAnimation.UseVisualStyleBackColor = false;
            // 
            // lblconfrontation
            // 
            this.lblconfrontation.BackColor = System.Drawing.Color.Transparent;
            this.lblconfrontation.Font = new System.Drawing.Font("Century Gothic", 20.25F, ((System.Drawing.FontStyle)((System.Drawing.FontStyle.Bold | System.Drawing.FontStyle.Italic))), System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.lblconfrontation.ForeColor = System.Drawing.SystemColors.ControlLight;
            this.lblconfrontation.Location = new System.Drawing.Point(242, 75);
            this.lblconfrontation.Name = "lblconfrontation";
            this.lblconfrontation.Size = new System.Drawing.Size(147, 31);
            this.lblconfrontation.TabIndex = 20;
            this.lblconfrontation.Text = "20";
            this.lblconfrontation.TextAlign = System.Drawing.ContentAlignment.MiddleCenter;
            // 
            // btnJouer
            // 
            this.btnJouer.Font = new System.Drawing.Font("Century Gothic", 20.25F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.btnJouer.Location = new System.Drawing.Point(22, 428);
            this.btnJouer.Name = "btnJouer";
            this.btnJouer.Size = new System.Drawing.Size(220, 52);
            this.btnJouer.TabIndex = 22;
            this.btnJouer.Text = "JOUER";
            this.btnJouer.UseVisualStyleBackColor = true;
            this.btnJouer.Click += new System.EventHandler(this.btnJouer_Click);
            // 
            // btnRecommencer
            // 
            this.btnRecommencer.Font = new System.Drawing.Font("Century Gothic", 20.25F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.btnRecommencer.Location = new System.Drawing.Point(389, 428);
            this.btnRecommencer.Name = "btnRecommencer";
            this.btnRecommencer.Size = new System.Drawing.Size(220, 52);
            this.btnRecommencer.TabIndex = 23;
            this.btnRecommencer.Text = "Recommencer";
            this.btnRecommencer.UseVisualStyleBackColor = true;
            this.btnRecommencer.Click += new System.EventHandler(this.btnRecommencer_Click);
            // 
            // optRoche
            // 
            this.optRoche.Appearance = System.Windows.Forms.Appearance.Button;
            this.optRoche.BackColor = System.Drawing.Color.Silver;
            this.optRoche.Checked = true;
            this.optRoche.FlatAppearance.BorderColor = System.Drawing.Color.FromArgb(((int)(((byte)(64)))), ((int)(((byte)(64)))), ((int)(((byte)(64)))));
            this.optRoche.FlatAppearance.BorderSize = 2;
            this.optRoche.FlatAppearance.CheckedBackColor = System.Drawing.Color.Gray;
            this.optRoche.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.optRoche.Font = new System.Drawing.Font("Century Gothic", 12F, ((System.Drawing.FontStyle)((System.Drawing.FontStyle.Bold | System.Drawing.FontStyle.Italic))), System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.optRoche.ForeColor = System.Drawing.Color.Black;
            this.optRoche.Location = new System.Drawing.Point(22, 329);
            this.optRoche.Name = "optRoche";
            this.optRoche.Size = new System.Drawing.Size(104, 32);
            this.optRoche.TabIndex = 24;
            this.optRoche.TabStop = true;
            this.optRoche.Text = "Roche";
            this.optRoche.TextAlign = System.Drawing.ContentAlignment.MiddleCenter;
            this.optRoche.UseVisualStyleBackColor = false;
            this.optRoche.CheckedChanged += new System.EventHandler(this.optRoche_CheckedChanged);
            // 
            // optPapier
            // 
            this.optPapier.Appearance = System.Windows.Forms.Appearance.Button;
            this.optPapier.BackColor = System.Drawing.Color.Silver;
            this.optPapier.FlatAppearance.BorderColor = System.Drawing.Color.FromArgb(((int)(((byte)(64)))), ((int)(((byte)(64)))), ((int)(((byte)(64)))));
            this.optPapier.FlatAppearance.BorderSize = 2;
            this.optPapier.FlatAppearance.CheckedBackColor = System.Drawing.Color.Gray;
            this.optPapier.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.optPapier.Font = new System.Drawing.Font("Century Gothic", 12F, ((System.Drawing.FontStyle)((System.Drawing.FontStyle.Bold | System.Drawing.FontStyle.Italic))), System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.optPapier.ForeColor = System.Drawing.Color.Black;
            this.optPapier.Location = new System.Drawing.Point(138, 329);
            this.optPapier.Name = "optPapier";
            this.optPapier.Size = new System.Drawing.Size(104, 32);
            this.optPapier.TabIndex = 25;
            this.optPapier.Text = "Papier";
            this.optPapier.TextAlign = System.Drawing.ContentAlignment.MiddleCenter;
            this.optPapier.UseVisualStyleBackColor = false;
            this.optPapier.CheckedChanged += new System.EventHandler(this.optPapier_CheckedChanged);
            // 
            // optAllumette
            // 
            this.optAllumette.Appearance = System.Windows.Forms.Appearance.Button;
            this.optAllumette.BackColor = System.Drawing.Color.Silver;
            this.optAllumette.FlatAppearance.BorderColor = System.Drawing.Color.FromArgb(((int)(((byte)(64)))), ((int)(((byte)(64)))), ((int)(((byte)(64)))));
            this.optAllumette.FlatAppearance.BorderSize = 2;
            this.optAllumette.FlatAppearance.CheckedBackColor = System.Drawing.Color.Gray;
            this.optAllumette.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.optAllumette.Font = new System.Drawing.Font("Century Gothic", 12F, ((System.Drawing.FontStyle)((System.Drawing.FontStyle.Bold | System.Drawing.FontStyle.Italic))), System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.optAllumette.ForeColor = System.Drawing.Color.Black;
            this.optAllumette.Location = new System.Drawing.Point(138, 378);
            this.optAllumette.Name = "optAllumette";
            this.optAllumette.Size = new System.Drawing.Size(104, 32);
            this.optAllumette.TabIndex = 26;
            this.optAllumette.Text = "Allumette";
            this.optAllumette.TextAlign = System.Drawing.ContentAlignment.MiddleCenter;
            this.optAllumette.UseVisualStyleBackColor = false;
            this.optAllumette.CheckedChanged += new System.EventHandler(this.optAllumette_CheckedChanged);
            // 
            // optCiseaux
            // 
            this.optCiseaux.Appearance = System.Windows.Forms.Appearance.Button;
            this.optCiseaux.BackColor = System.Drawing.Color.Silver;
            this.optCiseaux.FlatAppearance.BorderColor = System.Drawing.Color.FromArgb(((int)(((byte)(64)))), ((int)(((byte)(64)))), ((int)(((byte)(64)))));
            this.optCiseaux.FlatAppearance.BorderSize = 2;
            this.optCiseaux.FlatAppearance.CheckedBackColor = System.Drawing.Color.Gray;
            this.optCiseaux.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.optCiseaux.Font = new System.Drawing.Font("Century Gothic", 12F, ((System.Drawing.FontStyle)((System.Drawing.FontStyle.Bold | System.Drawing.FontStyle.Italic))), System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.optCiseaux.ForeColor = System.Drawing.Color.Black;
            this.optCiseaux.Location = new System.Drawing.Point(22, 378);
            this.optCiseaux.Name = "optCiseaux";
            this.optCiseaux.Size = new System.Drawing.Size(104, 32);
            this.optCiseaux.TabIndex = 27;
            this.optCiseaux.Text = "Ciseaux";
            this.optCiseaux.TextAlign = System.Drawing.ContentAlignment.MiddleCenter;
            this.optCiseaux.UseVisualStyleBackColor = false;
            this.optCiseaux.CheckedChanged += new System.EventHandler(this.optCiseaux_CheckedChanged);
            // 
            // timer1
            // 
            this.timer1.Tick += new System.EventHandler(this.timer1_Tick);
            // 
            // lblGagperdnul
            // 
            this.lblGagperdnul.BackColor = System.Drawing.Color.Transparent;
            this.lblGagperdnul.BorderStyle = System.Windows.Forms.BorderStyle.FixedSingle;
            this.lblGagperdnul.FlatStyle = System.Windows.Forms.FlatStyle.Popup;
            this.lblGagperdnul.Font = new System.Drawing.Font("Monotype Corsiva", 26.25F, ((System.Drawing.FontStyle)(((System.Drawing.FontStyle.Bold | System.Drawing.FontStyle.Italic) 
                | System.Drawing.FontStyle.Underline))), System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.lblGagperdnul.ForeColor = System.Drawing.SystemColors.ControlLight;
            this.lblGagperdnul.Location = new System.Drawing.Point(389, 320);
            this.lblGagperdnul.Name = "lblGagperdnul";
            this.lblGagperdnul.Size = new System.Drawing.Size(220, 90);
            this.lblGagperdnul.TabIndex = 29;
            this.lblGagperdnul.Text = "Nouvelle Partie !!!";
            this.lblGagperdnul.TextAlign = System.Drawing.ContentAlignment.MiddleCenter;
            // 
            // Form1
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.BackgroundImage = ((System.Drawing.Image)(resources.GetObject("$this.BackgroundImage")));
            this.BackgroundImageLayout = System.Windows.Forms.ImageLayout.Stretch;
            this.ClientSize = new System.Drawing.Size(634, 511);
            this.Controls.Add(this.lblGagperdnul);
            this.Controls.Add(this.optCiseaux);
            this.Controls.Add(this.optAllumette);
            this.Controls.Add(this.optPapier);
            this.Controls.Add(this.optRoche);
            this.Controls.Add(this.btnRecommencer);
            this.Controls.Add(this.btnJouer);
            this.Controls.Add(this.lblconfrontation);
            this.Controls.Add(this.chkAnimation);
            this.Controls.Add(this.label7);
            this.Controls.Add(this.lblNul);
            this.Controls.Add(this.label6);
            this.Controls.Add(this.label5);
            this.Controls.Add(this.lblVictoireordi);
            this.Controls.Add(this.lblVictoirejoueur);
            this.Controls.Add(this.label2);
            this.Controls.Add(this.lblVictoireNom);
            this.Controls.Add(this.imgOrdi);
            this.Controls.Add(this.imgJoueur);
            this.Name = "Form1";
            this.Text = "Form1";
            this.Load += new System.EventHandler(this.Form1_Load);
            ((System.ComponentModel.ISupportInitialize)(this.imgJoueur)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.imgOrdi)).EndInit();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion
        private System.Windows.Forms.PictureBox imgJoueur;
        private System.Windows.Forms.PictureBox imgOrdi;
        private System.Windows.Forms.Label lblVictoireNom;
        private System.Windows.Forms.Label label2;
        private System.Windows.Forms.Label lblVictoirejoueur;
        private System.Windows.Forms.Label lblVictoireordi;
        private System.Windows.Forms.Label label5;
        private System.Windows.Forms.Label label6;
        private System.Windows.Forms.Label lblNul;
        private System.Windows.Forms.Label label7;
        private System.Windows.Forms.CheckBox chkAnimation;
        private System.Windows.Forms.Label lblconfrontation;
        private System.Windows.Forms.Button btnJouer;
        private System.Windows.Forms.Button btnRecommencer;
        private System.Windows.Forms.RadioButton optRoche;
        private System.Windows.Forms.RadioButton optPapier;
        private System.Windows.Forms.RadioButton optAllumette;
        private System.Windows.Forms.RadioButton optCiseaux;
        private System.Windows.Forms.Timer timer1;
        private System.Windows.Forms.Label lblGagperdnul;
    }
}
