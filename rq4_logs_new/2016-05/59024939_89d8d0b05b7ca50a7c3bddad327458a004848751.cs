namespace ProjecteFinal
{
    partial class FrmManual
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
            this.txtNFactura = new System.Windows.Forms.TextBox();
            this.dtpDada = new System.Windows.Forms.DateTimePicker();
            this.label2 = new System.Windows.Forms.Label();
            this.tbnFacturar = new System.Windows.Forms.Button();
            this.btnCancelar = new System.Windows.Forms.Button();
            this.SuspendLayout();
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Location = new System.Drawing.Point(22, 53);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(80, 13);
            this.label1.TabIndex = 0;
            this.label1.Text = "Número factura";
            // 
            // txtNFactura
            // 
            this.txtNFactura.Location = new System.Drawing.Point(133, 46);
            this.txtNFactura.Name = "txtNFactura";
            this.txtNFactura.Size = new System.Drawing.Size(100, 20);
            this.txtNFactura.TabIndex = 1;
            // 
            // dtpDada
            // 
            this.dtpDada.Location = new System.Drawing.Point(72, 91);
            this.dtpDada.Name = "dtpDada";
            this.dtpDada.Size = new System.Drawing.Size(200, 20);
            this.dtpDada.TabIndex = 2;
            this.dtpDada.Value = new System.DateTime(2016, 5, 18, 0, 0, 0, 0);
            // 
            // label2
            // 
            this.label2.AutoSize = true;
            this.label2.Location = new System.Drawing.Point(22, 98);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(30, 13);
            this.label2.TabIndex = 3;
            this.label2.Text = "Data";
            // 
            // tbnFacturar
            // 
            this.tbnFacturar.Location = new System.Drawing.Point(54, 132);
            this.tbnFacturar.Name = "tbnFacturar";
            this.tbnFacturar.Size = new System.Drawing.Size(75, 23);
            this.tbnFacturar.TabIndex = 4;
            this.tbnFacturar.Text = "Facturar";
            this.tbnFacturar.UseVisualStyleBackColor = true;
            this.tbnFacturar.Click += new System.EventHandler(this.tbnFacturar_Click);
            // 
            // btnCancelar
            // 
            this.btnCancelar.Location = new System.Drawing.Point(158, 132);
            this.btnCancelar.Name = "btnCancelar";
            this.btnCancelar.Size = new System.Drawing.Size(75, 23);
            this.btnCancelar.TabIndex = 5;
            this.btnCancelar.Text = "Cancel·lar";
            this.btnCancelar.UseVisualStyleBackColor = true;
            this.btnCancelar.Click += new System.EventHandler(this.btnCancelar_Click);
            // 
            // FrmManual
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(326, 178);
            this.Controls.Add(this.btnCancelar);
            this.Controls.Add(this.tbnFacturar);
            this.Controls.Add(this.label2);
            this.Controls.Add(this.dtpDada);
            this.Controls.Add(this.txtNFactura);
            this.Controls.Add(this.label1);
            this.Name = "FrmManual";
            this.Text = "FrmManual";
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.TextBox txtNFactura;
        private System.Windows.Forms.DateTimePicker dtpDada;
        private System.Windows.Forms.Label label2;
        private System.Windows.Forms.Button tbnFacturar;
        private System.Windows.Forms.Button btnCancelar;
    }
}