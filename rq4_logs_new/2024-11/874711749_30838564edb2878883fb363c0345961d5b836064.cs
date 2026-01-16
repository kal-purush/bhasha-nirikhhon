namespace TheatreGUI
{
    partial class Accueil
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
            this.label1 = new System.Windows.Forms.Label();
            this.btngestionrepre = new System.Windows.Forms.Button();
            this.btngestionpie = new System.Windows.Forms.Button();
            this.btngestionresa = new System.Windows.Forms.Button();
            this.btngestionfesti = new System.Windows.Forms.Button();
            this.btnQuitter = new System.Windows.Forms.Button();
            this.lblVeuillez = new System.Windows.Forms.Label();
            this.SuspendLayout();
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Font = new System.Drawing.Font("Microsoft Sans Serif", 25F);
            this.label1.Location = new System.Drawing.Point(325, 46);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(128, 39);
            this.label1.TabIndex = 0;
            this.label1.Text = "Accueil";
            // 
            // btngestionrepre
            // 
            this.btngestionrepre.Font = new System.Drawing.Font("Microsoft Sans Serif", 12.25F);
            this.btngestionrepre.Location = new System.Drawing.Point(102, 154);
            this.btngestionrepre.Name = "btngestionrepre";
            this.btngestionrepre.Size = new System.Drawing.Size(187, 60);
            this.btngestionrepre.TabIndex = 1;
            this.btngestionrepre.Text = "Gestion des représentations";
            this.btngestionrepre.UseVisualStyleBackColor = true;
            // 
            // btngestionpie
            // 
            this.btngestionpie.Font = new System.Drawing.Font("Microsoft Sans Serif", 12.25F);
            this.btngestionpie.Location = new System.Drawing.Point(491, 154);
            this.btngestionpie.Name = "btngestionpie";
            this.btngestionpie.Size = new System.Drawing.Size(187, 60);
            this.btngestionpie.TabIndex = 2;
            this.btngestionpie.Text = "Gestion des pièces de théâtre";
            this.btngestionpie.UseVisualStyleBackColor = true;
            this.btngestionpie.Click += new System.EventHandler(this.btngestionpie_Click);
            // 
            // btngestionresa
            // 
            this.btngestionresa.Font = new System.Drawing.Font("Microsoft Sans Serif", 12.25F);
            this.btngestionresa.Location = new System.Drawing.Point(102, 282);
            this.btngestionresa.Name = "btngestionresa";
            this.btngestionresa.Size = new System.Drawing.Size(187, 60);
            this.btngestionresa.TabIndex = 3;
            this.btngestionresa.Text = "Gestion des réservations";
            this.btngestionresa.UseVisualStyleBackColor = true;
            // 
            // btngestionfesti
            // 
            this.btngestionfesti.Font = new System.Drawing.Font("Microsoft Sans Serif", 12.25F);
            this.btngestionfesti.Location = new System.Drawing.Point(491, 282);
            this.btngestionfesti.Name = "btngestionfesti";
            this.btngestionfesti.Size = new System.Drawing.Size(187, 60);
            this.btngestionfesti.TabIndex = 4;
            this.btngestionfesti.Text = "Gestions des festivals";
            this.btngestionfesti.UseVisualStyleBackColor = true;
            // 
            // btnQuitter
            // 
            this.btnQuitter.Location = new System.Drawing.Point(3, 415);
            this.btnQuitter.Name = "btnQuitter";
            this.btnQuitter.Size = new System.Drawing.Size(75, 23);
            this.btnQuitter.TabIndex = 5;
            this.btnQuitter.Text = "Quitter";
            this.btnQuitter.Click += new System.EventHandler(this.btnQuitter_Click);
            // 
            // lblVeuillez
            // 
            this.lblVeuillez.AutoSize = true;
            this.lblVeuillez.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.lblVeuillez.Location = new System.Drawing.Point(269, 98);
            this.lblVeuillez.Name = "lblVeuillez";
            this.lblVeuillez.Size = new System.Drawing.Size(241, 20);
            this.lblVeuillez.TabIndex = 0;
            this.lblVeuillez.Text = "Veuillez sélectionner une gestion";
            // 
            // Accueil
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(800, 450);
            this.Controls.Add(this.lblVeuillez);
            this.Controls.Add(this.btnQuitter);
            this.Controls.Add(this.btngestionfesti);
            this.Controls.Add(this.btngestionresa);
            this.Controls.Add(this.btngestionpie);
            this.Controls.Add(this.btngestionrepre);
            this.Controls.Add(this.label1);
            this.Name = "Accueil";
            this.StartPosition = System.Windows.Forms.FormStartPosition.CenterScreen;
            this.Text = "Accueil";
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.Button btngestionrepre;
        private System.Windows.Forms.Button btngestionpie;
        private System.Windows.Forms.Button btngestionresa;
        private System.Windows.Forms.Button btngestionfesti;
        private System.Windows.Forms.Button btnQuitter;
        private System.Windows.Forms.Label lblVeuillez;
    }
}
