namespace Sistema_Control_Acceso_Empleados
{
    partial class FrmAutorizacionManual
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
            System.Windows.Forms.Label lblCorreoUser;
            this.txtCorreoUser = new System.Windows.Forms.TextBox();
            this.lblMotivo = new System.Windows.Forms.Label();
            this.txtMotivo = new System.Windows.Forms.TextBox();
            this.btnAutorizar = new System.Windows.Forms.Button();
            this.lblResultado = new System.Windows.Forms.Label();
            this.label1 = new System.Windows.Forms.Label();
            lblCorreoUser = new System.Windows.Forms.Label();
            this.SuspendLayout();
            // 
            // lblCorreoUser
            // 
            lblCorreoUser.AutoSize = true;
            lblCorreoUser.Location = new System.Drawing.Point(177, 133);
            lblCorreoUser.Name = "lblCorreoUser";
            lblCorreoUser.Size = new System.Drawing.Size(145, 20);
            lblCorreoUser.TabIndex = 0;
            lblCorreoUser.Text = "Correo del Usuario:";
            // 
            // txtCorreoUser
            // 
            this.txtCorreoUser.Location = new System.Drawing.Point(458, 130);
            this.txtCorreoUser.Name = "txtCorreoUser";
            this.txtCorreoUser.Size = new System.Drawing.Size(228, 26);
            this.txtCorreoUser.TabIndex = 1;
            // 
            // lblMotivo
            // 
            this.lblMotivo.AutoSize = true;
            this.lblMotivo.Location = new System.Drawing.Point(181, 190);
            this.lblMotivo.Name = "lblMotivo";
            this.lblMotivo.Size = new System.Drawing.Size(171, 20);
            this.lblMotivo.TabIndex = 2;
            this.lblMotivo.Text = "Motivo de autorización:";
            // 
            // txtMotivo
            // 
            this.txtMotivo.Location = new System.Drawing.Point(458, 190);
            this.txtMotivo.Multiline = true;
            this.txtMotivo.Name = "txtMotivo";
            this.txtMotivo.Size = new System.Drawing.Size(228, 26);
            this.txtMotivo.TabIndex = 3;
            // 
            // btnAutorizar
            // 
            this.btnAutorizar.Location = new System.Drawing.Point(116, 309);
            this.btnAutorizar.Name = "btnAutorizar";
            this.btnAutorizar.Size = new System.Drawing.Size(206, 83);
            this.btnAutorizar.TabIndex = 4;
            this.btnAutorizar.Text = "Autorizar";
            this.btnAutorizar.UseVisualStyleBackColor = true;
            this.btnAutorizar.Click += new System.EventHandler(this.btnAutorizar_Click);
            // 
            // lblResultado
            // 
            this.lblResultado.AutoSize = true;
            this.lblResultado.Location = new System.Drawing.Point(420, 326);
            this.lblResultado.Name = "lblResultado";
            this.lblResultado.Size = new System.Drawing.Size(25, 20);
            this.lblResultado.TabIndex = 5;
            this.lblResultado.Text = "\" \"";
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Font = new System.Drawing.Font("Segoe UI Black", 16F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label1.Location = new System.Drawing.Point(289, 28);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(348, 45);
            this.label1.TabIndex = 6;
            this.label1.Text = "Autorización Manual";
            // 
            // FrmAutorizacionManual
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(9F, 20F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(800, 450);
            this.Controls.Add(this.label1);
            this.Controls.Add(this.lblResultado);
            this.Controls.Add(this.btnAutorizar);
            this.Controls.Add(this.txtMotivo);
            this.Controls.Add(this.lblMotivo);
            this.Controls.Add(this.txtCorreoUser);
            this.Controls.Add(lblCorreoUser);
            this.Name = "FrmAutorizacionManual";
            this.Text = "FrmAutorizacionManual";
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.TextBox txtCorreoUser;
        private System.Windows.Forms.Label lblMotivo;
        private System.Windows.Forms.TextBox txtMotivo;
        private System.Windows.Forms.Button btnAutorizar;
        private System.Windows.Forms.Label lblResultado;
        private System.Windows.Forms.Label label1;
    }
}