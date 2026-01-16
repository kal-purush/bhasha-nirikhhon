namespace UtilisateurGUI
{
    partial class AjoutReservation
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
            this.lblTitreForm = new System.Windows.Forms.Label();
            this.lblClient = new System.Windows.Forms.Label();
            this.lblNbPlace = new System.Windows.Forms.Label();
            this.lblRepresentation = new System.Windows.Forms.Label();
            this.lblPrenomCLient = new System.Windows.Forms.Label();
            this.lblEmailClient = new System.Windows.Forms.Label();
            this.lblTelClient = new System.Windows.Forms.Label();
            this.cboRepresentation = new System.Windows.Forms.ComboBox();
            this.txtNbPlace = new System.Windows.Forms.TextBox();
            this.txtNomClient = new System.Windows.Forms.TextBox();
            this.txtPrenomClient = new System.Windows.Forms.TextBox();
            this.txtEmailClient = new System.Windows.Forms.TextBox();
            this.txtTelClient = new System.Windows.Forms.TextBox();
            this.lblTarifPersonne = new System.Windows.Forms.Label();
            this.lblTarifTotal = new System.Windows.Forms.Label();
            this.cboPieceDeTheatre = new System.Windows.Forms.ComboBox();
            this.lblPieceDeTheatre = new System.Windows.Forms.Label();
            this.btnAjouter = new System.Windows.Forms.Button();
            this.btnRetour = new System.Windows.Forms.Button();
            this.SuspendLayout();
            // 
            // lblTitreForm
            // 
            this.lblTitreForm.AutoSize = true;
            this.lblTitreForm.Font = new System.Drawing.Font("Microsoft Sans Serif", 20.25F);
            this.lblTitreForm.Location = new System.Drawing.Point(313, 71);
            this.lblTitreForm.Name = "lblTitreForm";
            this.lblTitreForm.Size = new System.Drawing.Size(294, 31);
            this.lblTitreForm.TabIndex = 0;
            this.lblTitreForm.Text = "Ajouter une réservation";
            // 
            // lblClient
            // 
            this.lblClient.AutoSize = true;
            this.lblClient.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F);
            this.lblClient.Location = new System.Drawing.Point(522, 174);
            this.lblClient.Name = "lblClient";
            this.lblClient.Size = new System.Drawing.Size(113, 20);
            this.lblClient.TabIndex = 1;
            this.lblClient.Text = "Nom du client :";
            // 
            // lblNbPlace
            // 
            this.lblNbPlace.AutoSize = true;
            this.lblNbPlace.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F);
            this.lblNbPlace.Location = new System.Drawing.Point(66, 253);
            this.lblNbPlace.Name = "lblNbPlace";
            this.lblNbPlace.Size = new System.Drawing.Size(218, 20);
            this.lblNbPlace.TabIndex = 2;
            this.lblNbPlace.Text = "Nombre de places réservées :";
            // 
            // lblRepresentation
            // 
            this.lblRepresentation.AutoSize = true;
            this.lblRepresentation.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F);
            this.lblRepresentation.Location = new System.Drawing.Point(66, 212);
            this.lblRepresentation.Name = "lblRepresentation";
            this.lblRepresentation.Size = new System.Drawing.Size(127, 20);
            this.lblRepresentation.TabIndex = 3;
            this.lblRepresentation.Text = "Répresentation :";
            // 
            // lblPrenomCLient
            // 
            this.lblPrenomCLient.AutoSize = true;
            this.lblPrenomCLient.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F);
            this.lblPrenomCLient.Location = new System.Drawing.Point(500, 212);
            this.lblPrenomCLient.Name = "lblPrenomCLient";
            this.lblPrenomCLient.Size = new System.Drawing.Size(135, 20);
            this.lblPrenomCLient.TabIndex = 4;
            this.lblPrenomCLient.Text = "Prénom du client :";
            // 
            // lblEmailClient
            // 
            this.lblEmailClient.AutoSize = true;
            this.lblEmailClient.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F);
            this.lblEmailClient.Location = new System.Drawing.Point(516, 253);
            this.lblEmailClient.Name = "lblEmailClient";
            this.lblEmailClient.Size = new System.Drawing.Size(119, 20);
            this.lblEmailClient.TabIndex = 5;
            this.lblEmailClient.Text = "Email du client :";
            // 
            // lblTelClient
            // 
            this.lblTelClient.AutoSize = true;
            this.lblTelClient.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F);
            this.lblTelClient.Location = new System.Drawing.Point(480, 294);
            this.lblTelClient.Name = "lblTelClient";
            this.lblTelClient.Size = new System.Drawing.Size(155, 20);
            this.lblTelClient.TabIndex = 6;
            this.lblTelClient.Text = "Téléphone du client :";
            // 
            // cboRepresentation
            // 
            this.cboRepresentation.FormattingEnabled = true;
            this.cboRepresentation.Location = new System.Drawing.Point(199, 214);
            this.cboRepresentation.Name = "cboRepresentation";
            this.cboRepresentation.Size = new System.Drawing.Size(269, 21);
            this.cboRepresentation.TabIndex = 7;
            // 
            // txtNbPlace
            // 
            this.txtNbPlace.Location = new System.Drawing.Point(290, 255);
            this.txtNbPlace.Name = "txtNbPlace";
            this.txtNbPlace.Size = new System.Drawing.Size(178, 20);
            this.txtNbPlace.TabIndex = 8;
            // 
            // txtNomClient
            // 
            this.txtNomClient.Location = new System.Drawing.Point(641, 174);
            this.txtNomClient.Name = "txtNomClient";
            this.txtNomClient.Size = new System.Drawing.Size(261, 20);
            this.txtNomClient.TabIndex = 9;
            // 
            // txtPrenomClient
            // 
            this.txtPrenomClient.Location = new System.Drawing.Point(641, 212);
            this.txtPrenomClient.Name = "txtPrenomClient";
            this.txtPrenomClient.Size = new System.Drawing.Size(261, 20);
            this.txtPrenomClient.TabIndex = 10;
            // 
            // txtEmailClient
            // 
            this.txtEmailClient.Location = new System.Drawing.Point(641, 255);
            this.txtEmailClient.Name = "txtEmailClient";
            this.txtEmailClient.Size = new System.Drawing.Size(261, 20);
            this.txtEmailClient.TabIndex = 11;
            // 
            // txtTelClient
            // 
            this.txtTelClient.Location = new System.Drawing.Point(641, 296);
            this.txtTelClient.Name = "txtTelClient";
            this.txtTelClient.Size = new System.Drawing.Size(261, 20);
            this.txtTelClient.TabIndex = 12;
            // 
            // lblTarifPersonne
            // 
            this.lblTarifPersonne.AutoSize = true;
            this.lblTarifPersonne.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F);
            this.lblTarifPersonne.Location = new System.Drawing.Point(66, 296);
            this.lblTarifPersonne.Name = "lblTarifPersonne";
            this.lblTarifPersonne.Size = new System.Drawing.Size(146, 20);
            this.lblTarifPersonne.TabIndex = 13;
            this.lblTarifPersonne.Text = "Tarif par personne :";
            // 
            // lblTarifTotal
            // 
            this.lblTarifTotal.AutoSize = true;
            this.lblTarifTotal.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F);
            this.lblTarifTotal.Location = new System.Drawing.Point(66, 336);
            this.lblTarifTotal.Name = "lblTarifTotal";
            this.lblTarifTotal.Size = new System.Drawing.Size(83, 20);
            this.lblTarifTotal.TabIndex = 14;
            this.lblTarifTotal.Text = "Tarif total :";
            // 
            // cboPieceDeTheatre
            // 
            this.cboPieceDeTheatre.FormattingEnabled = true;
            this.cboPieceDeTheatre.Location = new System.Drawing.Point(199, 176);
            this.cboPieceDeTheatre.Name = "cboPieceDeTheatre";
            this.cboPieceDeTheatre.Size = new System.Drawing.Size(269, 21);
            this.cboPieceDeTheatre.TabIndex = 16;
            // 
            // lblPieceDeTheatre
            // 
            this.lblPieceDeTheatre.AutoSize = true;
            this.lblPieceDeTheatre.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F);
            this.lblPieceDeTheatre.Location = new System.Drawing.Point(66, 174);
            this.lblPieceDeTheatre.Name = "lblPieceDeTheatre";
            this.lblPieceDeTheatre.Size = new System.Drawing.Size(133, 20);
            this.lblPieceDeTheatre.TabIndex = 15;
            this.lblPieceDeTheatre.Text = "Pièce de théâtre :";
            // 
            // btnAjouter
            // 
            this.btnAjouter.Font = new System.Drawing.Font("Microsoft Sans Serif", 15.25F);
            this.btnAjouter.Location = new System.Drawing.Point(728, 468);
            this.btnAjouter.Name = "btnAjouter";
            this.btnAjouter.Size = new System.Drawing.Size(173, 37);
            this.btnAjouter.TabIndex = 17;
            this.btnAjouter.Text = "Ajouter";
            this.btnAjouter.UseVisualStyleBackColor = true;
            // 
            // btnRetour
            // 
            this.btnRetour.Font = new System.Drawing.Font("Microsoft Sans Serif", 15.25F);
            this.btnRetour.Location = new System.Drawing.Point(34, 468);
            this.btnRetour.Name = "btnRetour";
            this.btnRetour.Size = new System.Drawing.Size(165, 37);
            this.btnRetour.TabIndex = 18;
            this.btnRetour.Text = "Retour";
            this.btnRetour.UseVisualStyleBackColor = true;
            // 
            // AjoutReservation
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(933, 517);
            this.Controls.Add(this.btnRetour);
            this.Controls.Add(this.btnAjouter);
            this.Controls.Add(this.cboPieceDeTheatre);
            this.Controls.Add(this.lblPieceDeTheatre);
            this.Controls.Add(this.lblTarifTotal);
            this.Controls.Add(this.lblTarifPersonne);
            this.Controls.Add(this.txtTelClient);
            this.Controls.Add(this.txtEmailClient);
            this.Controls.Add(this.txtPrenomClient);
            this.Controls.Add(this.txtNomClient);
            this.Controls.Add(this.txtNbPlace);
            this.Controls.Add(this.cboRepresentation);
            this.Controls.Add(this.lblTelClient);
            this.Controls.Add(this.lblEmailClient);
            this.Controls.Add(this.lblPrenomCLient);
            this.Controls.Add(this.lblRepresentation);
            this.Controls.Add(this.lblNbPlace);
            this.Controls.Add(this.lblClient);
            this.Controls.Add(this.lblTitreForm);
            this.Name = "AjoutReservation";
            this.Text = "AjoutReservation";
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Label lblTitreForm;
        private System.Windows.Forms.Label lblClient;
        private System.Windows.Forms.Label lblNbPlace;
        private System.Windows.Forms.Label lblRepresentation;
        private System.Windows.Forms.Label lblPrenomCLient;
        private System.Windows.Forms.Label lblEmailClient;
        private System.Windows.Forms.Label lblTelClient;
        private System.Windows.Forms.ComboBox cboRepresentation;
        private System.Windows.Forms.TextBox txtNbPlace;
        private System.Windows.Forms.TextBox txtNomClient;
        private System.Windows.Forms.TextBox txtPrenomClient;
        private System.Windows.Forms.TextBox txtEmailClient;
        private System.Windows.Forms.TextBox txtTelClient;
        private System.Windows.Forms.Label lblTarifPersonne;
        private System.Windows.Forms.Label lblTarifTotal;
        private System.Windows.Forms.ComboBox cboPieceDeTheatre;
        private System.Windows.Forms.Label lblPieceDeTheatre;
        private System.Windows.Forms.Button btnAjouter;
        private System.Windows.Forms.Button btnRetour;
    }
}