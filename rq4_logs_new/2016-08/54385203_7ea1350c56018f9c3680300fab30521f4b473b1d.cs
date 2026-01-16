namespace StockVentas
{
    partial class frmArticulosDetalle
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
            this.txtIdArticulo = new System.Windows.Forms.TextBox();
            this.groupBox2 = new System.Windows.Forms.GroupBox();
            this.btnSalir = new System.Windows.Forms.Button();
            this.btnGrabar = new System.Windows.Forms.Button();
            this.btnImagen = new System.Windows.Forms.Button();
            this.txtFoto = new System.Windows.Forms.TextBox();
            this.txtDescripcion = new System.Windows.Forms.TextBox();
            this.label2 = new System.Windows.Forms.Label();
            this.label6 = new System.Windows.Forms.Label();
            this.label11 = new System.Windows.Forms.Label();
            this.label1 = new System.Windows.Forms.Label();
            this.chkActivo = new System.Windows.Forms.CheckBox();
            this.txtDescripcionWeb = new System.Windows.Forms.TextBox();
            this.label3 = new System.Windows.Forms.Label();
            this.txtCosto = new System.Windows.Forms.TextBox();
            this.label4 = new System.Windows.Forms.Label();
            this.txtPublico = new System.Windows.Forms.TextBox();
            this.label5 = new System.Windows.Forms.Label();
            this.txtMayor = new System.Windows.Forms.TextBox();
            this.label7 = new System.Windows.Forms.Label();
            this.txtFotoColor = new System.Windows.Forms.TextBox();
            this.label8 = new System.Windows.Forms.Label();
            this.btnImagenColor = new System.Windows.Forms.Button();
            this.txtFotoBack = new System.Windows.Forms.TextBox();
            this.label9 = new System.Windows.Forms.Label();
            this.btnImagenBck = new System.Windows.Forms.Button();
            this.grpCampos = new System.Windows.Forms.GroupBox();
            this.label12 = new System.Windows.Forms.Label();
            this.cmbAlicuota = new System.Windows.Forms.ComboBox();
            this.cmbGenero = new System.Windows.Forms.ComboBox();
            this.cmbProveedor = new System.Windows.Forms.ComboBox();
            this.label10 = new System.Windows.Forms.Label();
            this.label13 = new System.Windows.Forms.Label();
            this.groupBox2.SuspendLayout();
            this.grpCampos.SuspendLayout();
            this.SuspendLayout();
            // 
            // txtIdArticulo
            // 
            this.txtIdArticulo.BackColor = System.Drawing.SystemColors.ActiveCaptionText;
            this.txtIdArticulo.Location = new System.Drawing.Point(125, 19);
            this.txtIdArticulo.Name = "txtIdArticulo";
            this.txtIdArticulo.ReadOnly = true;
            this.txtIdArticulo.Size = new System.Drawing.Size(353, 20);
            this.txtIdArticulo.TabIndex = 0;
            // 
            // groupBox2
            // 
            this.groupBox2.Controls.Add(this.btnSalir);
            this.groupBox2.Controls.Add(this.btnGrabar);
            this.groupBox2.Location = new System.Drawing.Point(520, 7);
            this.groupBox2.Name = "groupBox2";
            this.groupBox2.Size = new System.Drawing.Size(166, 385);
            this.groupBox2.TabIndex = 1;
            this.groupBox2.TabStop = false;
            // 
            // btnSalir
            // 
            this.btnSalir.Location = new System.Drawing.Point(18, 54);
            this.btnSalir.Name = "btnSalir";
            this.btnSalir.Size = new System.Drawing.Size(135, 26);
            this.btnSalir.TabIndex = 1;
            this.btnSalir.Text = "Salir";
            this.btnSalir.UseVisualStyleBackColor = true;
            this.btnSalir.Click += new System.EventHandler(this.btnSalir_Click);
            // 
            // btnGrabar
            // 
            this.btnGrabar.Location = new System.Drawing.Point(18, 17);
            this.btnGrabar.Name = "btnGrabar";
            this.btnGrabar.Size = new System.Drawing.Size(135, 26);
            this.btnGrabar.TabIndex = 0;
            this.btnGrabar.Text = "Grabar";
            this.btnGrabar.UseVisualStyleBackColor = true;
            this.btnGrabar.Click += new System.EventHandler(this.btnGrabar_Click);
            // 
            // btnImagen
            // 
            this.btnImagen.Font = new System.Drawing.Font("Microsoft Sans Serif", 8.25F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.btnImagen.Location = new System.Drawing.Point(450, 257);
            this.btnImagen.Name = "btnImagen";
            this.btnImagen.Size = new System.Drawing.Size(28, 20);
            this.btnImagen.TabIndex = 10;
            this.btnImagen.TabStop = false;
            this.btnImagen.Text = "...";
            this.btnImagen.UseVisualStyleBackColor = true;
            this.btnImagen.Click += new System.EventHandler(this.btnImagen_Click);
            // 
            // txtFoto
            // 
            this.txtFoto.Location = new System.Drawing.Point(125, 260);
            this.txtFoto.Name = "txtFoto";
            this.txtFoto.Size = new System.Drawing.Size(353, 20);
            this.txtFoto.TabIndex = 8;
            // 
            // txtDescripcion
            // 
            this.txtDescripcion.CharacterCasing = System.Windows.Forms.CharacterCasing.Upper;
            this.txtDescripcion.Location = new System.Drawing.Point(125, 72);
            this.txtDescripcion.Name = "txtDescripcion";
            this.txtDescripcion.Size = new System.Drawing.Size(353, 20);
            this.txtDescripcion.TabIndex = 2;
            // 
            // label2
            // 
            this.label2.AutoSize = true;
            this.label2.Font = new System.Drawing.Font("Microsoft Sans Serif", 9F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label2.ForeColor = System.Drawing.SystemColors.Highlight;
            this.label2.Location = new System.Drawing.Point(16, 22);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(60, 15);
            this.label2.TabIndex = 26;
            this.label2.Text = "Id Artículo";
            // 
            // label6
            // 
            this.label6.AutoSize = true;
            this.label6.Font = new System.Drawing.Font("Microsoft Sans Serif", 9F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label6.ForeColor = System.Drawing.SystemColors.Highlight;
            this.label6.Location = new System.Drawing.Point(16, 75);
            this.label6.Name = "label6";
            this.label6.Size = new System.Drawing.Size(72, 15);
            this.label6.TabIndex = 31;
            this.label6.Text = "Descripción";
            // 
            // label11
            // 
            this.label11.AutoSize = true;
            this.label11.Font = new System.Drawing.Font("Microsoft Sans Serif", 9F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label11.ForeColor = System.Drawing.SystemColors.Highlight;
            this.label11.Location = new System.Drawing.Point(16, 262);
            this.label11.Name = "label11";
            this.label11.Size = new System.Drawing.Size(31, 15);
            this.label11.TabIndex = 36;
            this.label11.Text = "Foto";
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Font = new System.Drawing.Font("Microsoft Sans Serif", 9F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label1.ForeColor = System.Drawing.SystemColors.Highlight;
            this.label1.Location = new System.Drawing.Point(16, 342);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(38, 15);
            this.label1.TabIndex = 36;
            this.label1.Text = "Activo";
            // 
            // chkActivo
            // 
            this.chkActivo.AutoSize = true;
            this.chkActivo.Location = new System.Drawing.Point(125, 344);
            this.chkActivo.Name = "chkActivo";
            this.chkActivo.Size = new System.Drawing.Size(15, 14);
            this.chkActivo.TabIndex = 37;
            this.chkActivo.UseVisualStyleBackColor = true;
            // 
            // txtDescripcionWeb
            // 
            this.txtDescripcionWeb.CharacterCasing = System.Windows.Forms.CharacterCasing.Upper;
            this.txtDescripcionWeb.Location = new System.Drawing.Point(125, 98);
            this.txtDescripcionWeb.Name = "txtDescripcionWeb";
            this.txtDescripcionWeb.Size = new System.Drawing.Size(353, 20);
            this.txtDescripcionWeb.TabIndex = 3;
            // 
            // label3
            // 
            this.label3.AutoSize = true;
            this.label3.Font = new System.Drawing.Font("Microsoft Sans Serif", 9F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label3.ForeColor = System.Drawing.SystemColors.Highlight;
            this.label3.Location = new System.Drawing.Point(16, 101);
            this.label3.Name = "label3";
            this.label3.Size = new System.Drawing.Size(98, 15);
            this.label3.TabIndex = 31;
            this.label3.Text = "Descripción web";
            // 
            // txtCosto
            // 
            this.txtCosto.Location = new System.Drawing.Point(125, 124);
            this.txtCosto.Name = "txtCosto";
            this.txtCosto.Size = new System.Drawing.Size(353, 20);
            this.txtCosto.TabIndex = 4;
            // 
            // label4
            // 
            this.label4.AutoSize = true;
            this.label4.Font = new System.Drawing.Font("Microsoft Sans Serif", 9F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label4.ForeColor = System.Drawing.SystemColors.Highlight;
            this.label4.Location = new System.Drawing.Point(16, 127);
            this.label4.Name = "label4";
            this.label4.Size = new System.Drawing.Size(91, 15);
            this.label4.TabIndex = 31;
            this.label4.Text = "Precio de costo";
            // 
            // txtPublico
            // 
            this.txtPublico.Location = new System.Drawing.Point(125, 150);
            this.txtPublico.Name = "txtPublico";
            this.txtPublico.Size = new System.Drawing.Size(353, 20);
            this.txtPublico.TabIndex = 5;
            // 
            // label5
            // 
            this.label5.AutoSize = true;
            this.label5.Font = new System.Drawing.Font("Microsoft Sans Serif", 9F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label5.ForeColor = System.Drawing.SystemColors.Highlight;
            this.label5.Location = new System.Drawing.Point(16, 153);
            this.label5.Name = "label5";
            this.label5.Size = new System.Drawing.Size(98, 15);
            this.label5.TabIndex = 31;
            this.label5.Text = "Precio al público";
            // 
            // txtMayor
            // 
            this.txtMayor.Location = new System.Drawing.Point(125, 176);
            this.txtMayor.Name = "txtMayor";
            this.txtMayor.Size = new System.Drawing.Size(353, 20);
            this.txtMayor.TabIndex = 6;
            // 
            // label7
            // 
            this.label7.AutoSize = true;
            this.label7.Font = new System.Drawing.Font("Microsoft Sans Serif", 9F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label7.ForeColor = System.Drawing.SystemColors.Highlight;
            this.label7.Location = new System.Drawing.Point(16, 179);
            this.label7.Name = "label7";
            this.label7.Size = new System.Drawing.Size(98, 15);
            this.label7.TabIndex = 31;
            this.label7.Text = "Precio mayorista";
            // 
            // txtFotoColor
            // 
            this.txtFotoColor.Location = new System.Drawing.Point(125, 314);
            this.txtFotoColor.Name = "txtFotoColor";
            this.txtFotoColor.Size = new System.Drawing.Size(353, 20);
            this.txtFotoColor.TabIndex = 10;
            // 
            // label8
            // 
            this.label8.AutoSize = true;
            this.label8.Font = new System.Drawing.Font("Microsoft Sans Serif", 9F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label8.ForeColor = System.Drawing.SystemColors.Highlight;
            this.label8.Location = new System.Drawing.Point(16, 317);
            this.label8.Name = "label8";
            this.label8.Size = new System.Drawing.Size(61, 15);
            this.label8.TabIndex = 36;
            this.label8.Text = "Foto color";
            // 
            // btnImagenColor
            // 
            this.btnImagenColor.Font = new System.Drawing.Font("Microsoft Sans Serif", 8.25F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.btnImagenColor.Location = new System.Drawing.Point(450, 312);
            this.btnImagenColor.Name = "btnImagenColor";
            this.btnImagenColor.Size = new System.Drawing.Size(28, 20);
            this.btnImagenColor.TabIndex = 12;
            this.btnImagenColor.TabStop = false;
            this.btnImagenColor.Text = "...";
            this.btnImagenColor.UseVisualStyleBackColor = true;
            this.btnImagenColor.Click += new System.EventHandler(this.btnImagenColor_Click);
            // 
            // txtFotoBack
            // 
            this.txtFotoBack.Location = new System.Drawing.Point(125, 286);
            this.txtFotoBack.Name = "txtFotoBack";
            this.txtFotoBack.Size = new System.Drawing.Size(353, 20);
            this.txtFotoBack.TabIndex = 9;
            // 
            // label9
            // 
            this.label9.AutoSize = true;
            this.label9.Font = new System.Drawing.Font("Microsoft Sans Serif", 9F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label9.ForeColor = System.Drawing.SystemColors.Highlight;
            this.label9.Location = new System.Drawing.Point(16, 289);
            this.label9.Name = "label9";
            this.label9.Size = new System.Drawing.Size(61, 15);
            this.label9.TabIndex = 36;
            this.label9.Text = "Foto atrás";
            // 
            // btnImagenBck
            // 
            this.btnImagenBck.Font = new System.Drawing.Font("Microsoft Sans Serif", 8.25F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.btnImagenBck.Location = new System.Drawing.Point(450, 283);
            this.btnImagenBck.Name = "btnImagenBck";
            this.btnImagenBck.Size = new System.Drawing.Size(28, 20);
            this.btnImagenBck.TabIndex = 11;
            this.btnImagenBck.TabStop = false;
            this.btnImagenBck.Text = "...";
            this.btnImagenBck.UseVisualStyleBackColor = true;
            this.btnImagenBck.Click += new System.EventHandler(this.btnImagenBck_Click);
            // 
            // grpCampos
            // 
            this.grpCampos.Controls.Add(this.label12);
            this.grpCampos.Controls.Add(this.cmbAlicuota);
            this.grpCampos.Controls.Add(this.cmbGenero);
            this.grpCampos.Controls.Add(this.cmbProveedor);
            this.grpCampos.Controls.Add(this.btnImagen);
            this.grpCampos.Controls.Add(this.label2);
            this.grpCampos.Controls.Add(this.btnImagenColor);
            this.grpCampos.Controls.Add(this.txtIdArticulo);
            this.grpCampos.Controls.Add(this.btnImagenBck);
            this.grpCampos.Controls.Add(this.txtFoto);
            this.grpCampos.Controls.Add(this.txtFotoColor);
            this.grpCampos.Controls.Add(this.chkActivo);
            this.grpCampos.Controls.Add(this.txtFotoBack);
            this.grpCampos.Controls.Add(this.label1);
            this.grpCampos.Controls.Add(this.txtDescripcion);
            this.grpCampos.Controls.Add(this.label8);
            this.grpCampos.Controls.Add(this.txtDescripcionWeb);
            this.grpCampos.Controls.Add(this.label9);
            this.grpCampos.Controls.Add(this.txtCosto);
            this.grpCampos.Controls.Add(this.label10);
            this.grpCampos.Controls.Add(this.label11);
            this.grpCampos.Controls.Add(this.txtPublico);
            this.grpCampos.Controls.Add(this.label13);
            this.grpCampos.Controls.Add(this.label7);
            this.grpCampos.Controls.Add(this.txtMayor);
            this.grpCampos.Controls.Add(this.label5);
            this.grpCampos.Controls.Add(this.label6);
            this.grpCampos.Controls.Add(this.label4);
            this.grpCampos.Controls.Add(this.label3);
            this.grpCampos.Location = new System.Drawing.Point(9, 7);
            this.grpCampos.Name = "grpCampos";
            this.grpCampos.Size = new System.Drawing.Size(499, 385);
            this.grpCampos.TabIndex = 0;
            this.grpCampos.TabStop = false;
            // 
            // label12
            // 
            this.label12.AutoSize = true;
            this.label12.Font = new System.Drawing.Font("Microsoft Sans Serif", 9F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label12.ForeColor = System.Drawing.SystemColors.Highlight;
            this.label12.Location = new System.Drawing.Point(16, 47);
            this.label12.Name = "label12";
            this.label12.Size = new System.Drawing.Size(48, 15);
            this.label12.TabIndex = 39;
            this.label12.Text = "Genero";
            // 
            // cmbAlicuota
            // 
            this.cmbAlicuota.FormattingEnabled = true;
            this.cmbAlicuota.Location = new System.Drawing.Point(125, 203);
            this.cmbAlicuota.Name = "cmbAlicuota";
            this.cmbAlicuota.Size = new System.Drawing.Size(353, 21);
            this.cmbAlicuota.TabIndex = 1;
            // 
            // cmbGenero
            // 
            this.cmbGenero.FormattingEnabled = true;
            this.cmbGenero.Location = new System.Drawing.Point(125, 45);
            this.cmbGenero.Name = "cmbGenero";
            this.cmbGenero.Size = new System.Drawing.Size(353, 21);
            this.cmbGenero.TabIndex = 1;
            // 
            // cmbProveedor
            // 
            this.cmbProveedor.FormattingEnabled = true;
            this.cmbProveedor.Location = new System.Drawing.Point(125, 232);
            this.cmbProveedor.Name = "cmbProveedor";
            this.cmbProveedor.Size = new System.Drawing.Size(353, 21);
            this.cmbProveedor.TabIndex = 7;
            // 
            // label10
            // 
            this.label10.AutoSize = true;
            this.label10.Font = new System.Drawing.Font("Microsoft Sans Serif", 9F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label10.ForeColor = System.Drawing.SystemColors.Highlight;
            this.label10.Location = new System.Drawing.Point(16, 237);
            this.label10.Name = "label10";
            this.label10.Size = new System.Drawing.Size(63, 15);
            this.label10.TabIndex = 36;
            this.label10.Text = "Proveedor";
            // 
            // label13
            // 
            this.label13.AutoSize = true;
            this.label13.Font = new System.Drawing.Font("Microsoft Sans Serif", 9F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label13.ForeColor = System.Drawing.SystemColors.Highlight;
            this.label13.Location = new System.Drawing.Point(16, 207);
            this.label13.Name = "label13";
            this.label13.Size = new System.Drawing.Size(68, 15);
            this.label13.TabIndex = 31;
            this.label13.Text = "Alícuota iva";
            // 
            // frmArticulosDetalle
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(695, 404);
            this.Controls.Add(this.grpCampos);
            this.Controls.Add(this.groupBox2);
            this.Name = "frmArticulosDetalle";
            this.Text = "Artículos";
            this.FormClosing += new System.Windows.Forms.FormClosingEventHandler(this.frmArticulosDetalle_FormClosing);
            this.Load += new System.EventHandler(this.frmArticulosDetalle_Load);
            this.groupBox2.ResumeLayout(false);
            this.grpCampos.ResumeLayout(false);
            this.grpCampos.PerformLayout();
            this.ResumeLayout(false);

        }

        #endregion

        private System.Windows.Forms.TextBox txtIdArticulo;
        private System.Windows.Forms.GroupBox groupBox2;
        private System.Windows.Forms.Button btnSalir;
        private System.Windows.Forms.Button btnGrabar;
        private System.Windows.Forms.TextBox txtFoto;
        private System.Windows.Forms.TextBox txtDescripcion;
        private System.Windows.Forms.Button btnImagen;
        private System.Windows.Forms.Label label2;
        private System.Windows.Forms.Label label6;
        private System.Windows.Forms.Label label11;
        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.CheckBox chkActivo;
        private System.Windows.Forms.TextBox txtDescripcionWeb;
        private System.Windows.Forms.Label label3;
        private System.Windows.Forms.TextBox txtCosto;
        private System.Windows.Forms.Label label4;
        private System.Windows.Forms.TextBox txtPublico;
        private System.Windows.Forms.Label label5;
        private System.Windows.Forms.TextBox txtMayor;
        private System.Windows.Forms.Label label7;
        private System.Windows.Forms.TextBox txtFotoColor;
        private System.Windows.Forms.Label label8;
        private System.Windows.Forms.Button btnImagenColor;
        private System.Windows.Forms.TextBox txtFotoBack;
        private System.Windows.Forms.Label label9;
        private System.Windows.Forms.Button btnImagenBck;
        private System.Windows.Forms.GroupBox grpCampos;
        private System.Windows.Forms.ComboBox cmbProveedor;
        private System.Windows.Forms.Label label10;
        private System.Windows.Forms.Label label12;
        private System.Windows.Forms.ComboBox cmbGenero;
        private System.Windows.Forms.ComboBox cmbAlicuota;
        private System.Windows.Forms.Label label13;
    }
}