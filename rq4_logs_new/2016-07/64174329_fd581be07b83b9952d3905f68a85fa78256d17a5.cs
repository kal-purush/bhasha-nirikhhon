namespace WindowsFormsApplication1
{
    partial class Form_registro_pedido
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
            System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(Form_registro_pedido));
            this.Lbl_pedido_cliente = new System.Windows.Forms.Label();
            this.Btn_registro_pedido = new System.Windows.Forms.Button();
            this.Btn_modificar_pedido = new System.Windows.Forms.Button();
            this.dgv_pedido_cliente = new System.Windows.Forms.DataGridView();
            this.Btn_cancelar_pedido = new System.Windows.Forms.Button();
            this.cbo_nombre = new System.Windows.Forms.ComboBox();
            this.Lbl_nombre = new System.Windows.Forms.Label();
            this.Lbl_marca = new System.Windows.Forms.Label();
            this.txt_marca = new System.Windows.Forms.TextBox();
            this.Lbl_cantidad = new System.Windows.Forms.Label();
            this.txt_cantidad = new System.Windows.Forms.TextBox();
            this.Lbl_fecha = new System.Windows.Forms.Label();
            this.txt_fecha = new System.Windows.Forms.TextBox();
            this.Lbl_cliente = new System.Windows.Forms.Label();
            this.cbo_cliente = new System.Windows.Forms.ComboBox();
            this.gbp_menu = new System.Windows.Forms.GroupBox();
            this.Btn_guardar_pedido = new System.Windows.Forms.Button();
            this.Lbl_guardar = new System.Windows.Forms.Label();
            this.Lbl_cancelar = new System.Windows.Forms.Label();
            ((System.ComponentModel.ISupportInitialize)(this.dgv_pedido_cliente)).BeginInit();
            this.gbp_menu.SuspendLayout();
            this.SuspendLayout();
            // 
            // Lbl_pedido_cliente
            // 
            this.Lbl_pedido_cliente.AutoSize = true;
            this.Lbl_pedido_cliente.Font = new System.Drawing.Font("Century Gothic", 20.25F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.Lbl_pedido_cliente.Location = new System.Drawing.Point(266, 24);
            this.Lbl_pedido_cliente.Name = "Lbl_pedido_cliente";
            this.Lbl_pedido_cliente.Size = new System.Drawing.Size(354, 32);
            this.Lbl_pedido_cliente.TabIndex = 0;
            this.Lbl_pedido_cliente.Text = "REGISTRO PEDIDO CLIENTE";
            this.Lbl_pedido_cliente.Click += new System.EventHandler(this.label1_Click);
            // 
            // Btn_registro_pedido
            // 
            this.Btn_registro_pedido.Font = new System.Drawing.Font("Century Gothic", 11.25F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.Btn_registro_pedido.Location = new System.Drawing.Point(205, 21);
            this.Btn_registro_pedido.Name = "Btn_registro_pedido";
            this.Btn_registro_pedido.Size = new System.Drawing.Size(91, 30);
            this.Btn_registro_pedido.TabIndex = 2;
            this.Btn_registro_pedido.Text = "Registro";
            this.Btn_registro_pedido.UseVisualStyleBackColor = true;
            this.Btn_registro_pedido.Click += new System.EventHandler(this.Btn_registro_pedido_Click);
            // 
            // Btn_modificar_pedido
            // 
            this.Btn_modificar_pedido.Font = new System.Drawing.Font("Century Gothic", 11.25F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.Btn_modificar_pedido.Location = new System.Drawing.Point(472, 21);
            this.Btn_modificar_pedido.Name = "Btn_modificar_pedido";
            this.Btn_modificar_pedido.Size = new System.Drawing.Size(91, 30);
            this.Btn_modificar_pedido.TabIndex = 1;
            this.Btn_modificar_pedido.Text = "Modificar";
            this.Btn_modificar_pedido.UseVisualStyleBackColor = true;
            this.Btn_modificar_pedido.Click += new System.EventHandler(this.Btn_modificar_pedido_Click);
            // 
            // dgv_pedido_cliente
            // 
            this.dgv_pedido_cliente.ColumnHeadersHeightSizeMode = System.Windows.Forms.DataGridViewColumnHeadersHeightSizeMode.AutoSize;
            this.dgv_pedido_cliente.Location = new System.Drawing.Point(57, 339);
            this.dgv_pedido_cliente.Name = "dgv_pedido_cliente";
            this.dgv_pedido_cliente.Size = new System.Drawing.Size(800, 150);
            this.dgv_pedido_cliente.TabIndex = 4;
            // 
            // Btn_cancelar_pedido
            // 
            this.Btn_cancelar_pedido.BackgroundImage = ((System.Drawing.Image)(resources.GetObject("Btn_cancelar_pedido.BackgroundImage")));
            this.Btn_cancelar_pedido.BackgroundImageLayout = System.Windows.Forms.ImageLayout.Stretch;
            this.Btn_cancelar_pedido.Cursor = System.Windows.Forms.Cursors.Arrow;
            this.Btn_cancelar_pedido.Font = new System.Drawing.Font("Century Gothic", 11.25F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.Btn_cancelar_pedido.Location = new System.Drawing.Point(792, 238);
            this.Btn_cancelar_pedido.Name = "Btn_cancelar_pedido";
            this.Btn_cancelar_pedido.Size = new System.Drawing.Size(65, 65);
            this.Btn_cancelar_pedido.TabIndex = 5;
            this.Btn_cancelar_pedido.UseVisualStyleBackColor = true;
            // 
            // cbo_nombre
            // 
            this.cbo_nombre.FormattingEnabled = true;
            this.cbo_nombre.Location = new System.Drawing.Point(203, 133);
            this.cbo_nombre.Name = "cbo_nombre";
            this.cbo_nombre.Size = new System.Drawing.Size(150, 21);
            this.cbo_nombre.TabIndex = 7;
            // 
            // Lbl_nombre
            // 
            this.Lbl_nombre.AutoSize = true;
            this.Lbl_nombre.Font = new System.Drawing.Font("Century Gothic", 11.25F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.Lbl_nombre.Location = new System.Drawing.Point(53, 134);
            this.Lbl_nombre.Name = "Lbl_nombre";
            this.Lbl_nombre.Size = new System.Drawing.Size(144, 20);
            this.Lbl_nombre.TabIndex = 8;
            this.Lbl_nombre.Text = "Nombre Producto:";
            // 
            // Lbl_marca
            // 
            this.Lbl_marca.AutoSize = true;
            this.Lbl_marca.Font = new System.Drawing.Font("Century Gothic", 11.25F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.Lbl_marca.Location = new System.Drawing.Point(53, 168);
            this.Lbl_marca.Name = "Lbl_marca";
            this.Lbl_marca.Size = new System.Drawing.Size(135, 20);
            this.Lbl_marca.TabIndex = 9;
            this.Lbl_marca.Text = "Marca Producto:";
            this.Lbl_marca.Click += new System.EventHandler(this.label2_Click);
            // 
            // txt_marca
            // 
            this.txt_marca.Location = new System.Drawing.Point(203, 168);
            this.txt_marca.Name = "txt_marca";
            this.txt_marca.Size = new System.Drawing.Size(150, 20);
            this.txt_marca.TabIndex = 10;
            // 
            // Lbl_cantidad
            // 
            this.Lbl_cantidad.AutoSize = true;
            this.Lbl_cantidad.Font = new System.Drawing.Font("Century Gothic", 11.25F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.Lbl_cantidad.Location = new System.Drawing.Point(53, 206);
            this.Lbl_cantidad.Name = "Lbl_cantidad";
            this.Lbl_cantidad.Size = new System.Drawing.Size(82, 20);
            this.Lbl_cantidad.TabIndex = 11;
            this.Lbl_cantidad.Text = "Cantidad:";
            this.Lbl_cantidad.Click += new System.EventHandler(this.label3_Click);
            // 
            // txt_cantidad
            // 
            this.txt_cantidad.Location = new System.Drawing.Point(203, 208);
            this.txt_cantidad.Name = "txt_cantidad";
            this.txt_cantidad.Size = new System.Drawing.Size(150, 20);
            this.txt_cantidad.TabIndex = 12;
            // 
            // Lbl_fecha
            // 
            this.Lbl_fecha.AutoSize = true;
            this.Lbl_fecha.Font = new System.Drawing.Font("Century Gothic", 11.25F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.Lbl_fecha.Location = new System.Drawing.Point(389, 134);
            this.Lbl_fecha.Name = "Lbl_fecha";
            this.Lbl_fecha.RightToLeft = System.Windows.Forms.RightToLeft.No;
            this.Lbl_fecha.Size = new System.Drawing.Size(59, 20);
            this.Lbl_fecha.TabIndex = 13;
            this.Lbl_fecha.Text = "Fecha:";
            this.Lbl_fecha.Click += new System.EventHandler(this.label1_Click_1);
            // 
            // txt_fecha
            // 
            this.txt_fecha.Location = new System.Drawing.Point(454, 134);
            this.txt_fecha.Name = "txt_fecha";
            this.txt_fecha.Size = new System.Drawing.Size(150, 20);
            this.txt_fecha.TabIndex = 14;
            // 
            // Lbl_cliente
            // 
            this.Lbl_cliente.AutoSize = true;
            this.Lbl_cliente.Font = new System.Drawing.Font("Century Gothic", 11.25F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.Lbl_cliente.Location = new System.Drawing.Point(389, 168);
            this.Lbl_cliente.Name = "Lbl_cliente";
            this.Lbl_cliente.RightToLeft = System.Windows.Forms.RightToLeft.No;
            this.Lbl_cliente.Size = new System.Drawing.Size(65, 20);
            this.Lbl_cliente.TabIndex = 15;
            this.Lbl_cliente.Text = "Cliente:";
            this.Lbl_cliente.Click += new System.EventHandler(this.label2_Click_1);
            // 
            // cbo_cliente
            // 
            this.cbo_cliente.FormattingEnabled = true;
            this.cbo_cliente.Location = new System.Drawing.Point(454, 167);
            this.cbo_cliente.Name = "cbo_cliente";
            this.cbo_cliente.Size = new System.Drawing.Size(150, 21);
            this.cbo_cliente.TabIndex = 16;
            // 
            // gbp_menu
            // 
            this.gbp_menu.Controls.Add(this.Btn_registro_pedido);
            this.gbp_menu.Controls.Add(this.Btn_modificar_pedido);
            this.gbp_menu.Location = new System.Drawing.Point(57, 53);
            this.gbp_menu.Name = "gbp_menu";
            this.gbp_menu.Size = new System.Drawing.Size(805, 57);
            this.gbp_menu.TabIndex = 17;
            this.gbp_menu.TabStop = false;
            this.gbp_menu.Text = "MENU";
            // 
            // Btn_guardar_pedido
            // 
            this.Btn_guardar_pedido.BackgroundImage = ((System.Drawing.Image)(resources.GetObject("Btn_guardar_pedido.BackgroundImage")));
            this.Btn_guardar_pedido.BackgroundImageLayout = System.Windows.Forms.ImageLayout.Stretch;
            this.Btn_guardar_pedido.Font = new System.Drawing.Font("Century Gothic", 11.25F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.Btn_guardar_pedido.ImageAlign = System.Drawing.ContentAlignment.TopLeft;
            this.Btn_guardar_pedido.Location = new System.Drawing.Point(792, 145);
            this.Btn_guardar_pedido.Name = "Btn_guardar_pedido";
            this.Btn_guardar_pedido.Size = new System.Drawing.Size(65, 65);
            this.Btn_guardar_pedido.TabIndex = 18;
            this.Btn_guardar_pedido.TextAlign = System.Drawing.ContentAlignment.TopRight;
            this.Btn_guardar_pedido.UseVisualStyleBackColor = true;
            // 
            // Lbl_guardar
            // 
            this.Lbl_guardar.AutoSize = true;
            this.Lbl_guardar.Font = new System.Drawing.Font("Century Gothic", 11.25F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.Lbl_guardar.Location = new System.Drawing.Point(798, 122);
            this.Lbl_guardar.Name = "Lbl_guardar";
            this.Lbl_guardar.RightToLeft = System.Windows.Forms.RightToLeft.No;
            this.Lbl_guardar.Size = new System.Drawing.Size(71, 20);
            this.Lbl_guardar.TabIndex = 19;
            this.Lbl_guardar.Text = "Guardar";
            // 
            // Lbl_cancelar
            // 
            this.Lbl_cancelar.AutoSize = true;
            this.Lbl_cancelar.Font = new System.Drawing.Font("Century Gothic", 11.25F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.Lbl_cancelar.Location = new System.Drawing.Point(788, 224);
            this.Lbl_cancelar.Name = "Lbl_cancelar";
            this.Lbl_cancelar.RightToLeft = System.Windows.Forms.RightToLeft.No;
            this.Lbl_cancelar.Size = new System.Drawing.Size(78, 20);
            this.Lbl_cancelar.TabIndex = 20;
            this.Lbl_cancelar.Text = "Cancelar";
            // 
            // Form_registro_pedido
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.BackColor = System.Drawing.SystemColors.ActiveCaption;
            this.ClientSize = new System.Drawing.Size(929, 532);
            this.Controls.Add(this.Lbl_cancelar);
            this.Controls.Add(this.Lbl_guardar);
            this.Controls.Add(this.Btn_guardar_pedido);
            this.Controls.Add(this.gbp_menu);
            this.Controls.Add(this.cbo_cliente);
            this.Controls.Add(this.Lbl_cliente);
            this.Controls.Add(this.Btn_cancelar_pedido);
            this.Controls.Add(this.txt_fecha);
            this.Controls.Add(this.Lbl_fecha);
            this.Controls.Add(this.txt_cantidad);
            this.Controls.Add(this.Lbl_cantidad);
            this.Controls.Add(this.txt_marca);
            this.Controls.Add(this.Lbl_marca);
            this.Controls.Add(this.Lbl_nombre);
            this.Controls.Add(this.cbo_nombre);
            this.Controls.Add(this.dgv_pedido_cliente);
            this.Controls.Add(this.Lbl_pedido_cliente);
            this.Name = "Form_registro_pedido";
            this.Text = "REGISTRO PEDIDO";
            this.Load += new System.EventHandler(this.Form1_Load);
            ((System.ComponentModel.ISupportInitialize)(this.dgv_pedido_cliente)).EndInit();
            this.gbp_menu.ResumeLayout(false);
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Label Lbl_pedido_cliente;
        private System.Windows.Forms.Button Btn_registro_pedido;
        private System.Windows.Forms.Button Btn_modificar_pedido;
        private System.Windows.Forms.DataGridView dgv_pedido_cliente;
        private System.Windows.Forms.Button Btn_cancelar_pedido;
        private System.Windows.Forms.ComboBox cbo_nombre;
        private System.Windows.Forms.Label Lbl_nombre;
        private System.Windows.Forms.Label Lbl_marca;
        private System.Windows.Forms.TextBox txt_marca;
        private System.Windows.Forms.Label Lbl_cantidad;
        private System.Windows.Forms.TextBox txt_cantidad;
        private System.Windows.Forms.Label Lbl_fecha;
        private System.Windows.Forms.TextBox txt_fecha;
        private System.Windows.Forms.Label Lbl_cliente;
        private System.Windows.Forms.ComboBox cbo_cliente;
        private System.Windows.Forms.GroupBox gbp_menu;
        private System.Windows.Forms.Button Btn_guardar_pedido;
        private System.Windows.Forms.Label Lbl_guardar;
        private System.Windows.Forms.Label Lbl_cancelar;
    }
}
