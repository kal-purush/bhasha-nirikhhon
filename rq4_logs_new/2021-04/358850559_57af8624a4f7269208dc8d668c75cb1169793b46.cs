namespace WindowsFormsApplication1
{
    partial class Form1
    {
        /// <summary>
        /// Variable del diseñador requerida.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// Limpiar los recursos que se estén utilizando.
        /// </summary>
        /// <param name="disposing">true si los recursos administrados se deben eliminar; false en caso contrario, false.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Código generado por el Diseñador de Windows Forms

        /// <summary>
        /// Método necesario para admitir el Diseñador. No se puede modificar
        /// el contenido del método con el editor de código.
        /// </summary>
        private void InitializeComponent()
        {
            this.label2 = new System.Windows.Forms.Label();
            this.nombre = new System.Windows.Forms.TextBox();
            this.button2 = new System.Windows.Forms.Button();
            this.groupBox1 = new System.Windows.Forms.GroupBox();
            this.muestraconectados = new System.Windows.Forms.Button();
            this.MostrarConectados = new System.Windows.Forms.DataGridView();
            this.Usuario = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.consulta4 = new System.Windows.Forms.RadioButton();
            this.label3 = new System.Windows.Forms.Label();
            this.label1 = new System.Windows.Forms.Label();
            this.textcontra = new System.Windows.Forms.TextBox();
            this.textnombre = new System.Windows.Forms.TextBox();
            this.entrar = new System.Windows.Forms.Button();
            this.registrar = new System.Windows.Forms.Button();
            this.consulta3 = new System.Windows.Forms.RadioButton();
            this.consulta2 = new System.Windows.Forms.RadioButton();
            this.button3 = new System.Windows.Forms.Button();
            this.button1 = new System.Windows.Forms.Button();
            this.consulta1 = new System.Windows.Forms.RadioButton();
            this.groupBox1.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)(this.MostrarConectados)).BeginInit();
            this.SuspendLayout();
            // 
            // label2
            // 
            this.label2.AutoSize = true;
            this.label2.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label2.Location = new System.Drawing.Point(67, 278);
            this.label2.Margin = new System.Windows.Forms.Padding(4, 0, 4, 0);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(81, 25);
            this.label2.TabIndex = 1;
            this.label2.Text = "Nombre";
            this.label2.Visible = false;
            // 
            // nombre
            // 
            this.nombre.Location = new System.Drawing.Point(190, 278);
            this.nombre.Margin = new System.Windows.Forms.Padding(4);
            this.nombre.Name = "nombre";
            this.nombre.Size = new System.Drawing.Size(232, 22);
            this.nombre.TabIndex = 3;
            this.nombre.Visible = false;
            // 
            // button2
            // 
            this.button2.Location = new System.Drawing.Point(434, 429);
            this.button2.Margin = new System.Windows.Forms.Padding(4);
            this.button2.Name = "button2";
            this.button2.Size = new System.Drawing.Size(103, 28);
            this.button2.TabIndex = 5;
            this.button2.Text = "Enviar";
            this.button2.UseVisualStyleBackColor = true;
            this.button2.Visible = false;
            this.button2.Click += new System.EventHandler(this.button2_Click);
            // 
            // groupBox1
            // 
            this.groupBox1.BackColor = System.Drawing.SystemColors.ActiveBorder;
            this.groupBox1.Controls.Add(this.muestraconectados);
            this.groupBox1.Controls.Add(this.MostrarConectados);
            this.groupBox1.Controls.Add(this.consulta4);
            this.groupBox1.Controls.Add(this.label3);
            this.groupBox1.Controls.Add(this.label1);
            this.groupBox1.Controls.Add(this.textcontra);
            this.groupBox1.Controls.Add(this.textnombre);
            this.groupBox1.Controls.Add(this.entrar);
            this.groupBox1.Controls.Add(this.registrar);
            this.groupBox1.Controls.Add(this.consulta3);
            this.groupBox1.Controls.Add(this.consulta2);
            this.groupBox1.Controls.Add(this.button3);
            this.groupBox1.Controls.Add(this.button1);
            this.groupBox1.Controls.Add(this.consulta1);
            this.groupBox1.Controls.Add(this.label2);
            this.groupBox1.Controls.Add(this.button2);
            this.groupBox1.Controls.Add(this.nombre);
            this.groupBox1.Location = new System.Drawing.Point(16, 34);
            this.groupBox1.Margin = new System.Windows.Forms.Padding(4);
            this.groupBox1.Name = "groupBox1";
            this.groupBox1.Padding = new System.Windows.Forms.Padding(4);
            this.groupBox1.Size = new System.Drawing.Size(1020, 641);
            this.groupBox1.TabIndex = 6;
            this.groupBox1.TabStop = false;
            this.groupBox1.Text = "Peticion";
            // 
            // muestraconectados
            // 
            this.muestraconectados.Location = new System.Drawing.Point(840, 306);
            this.muestraconectados.Name = "muestraconectados";
            this.muestraconectados.Size = new System.Drawing.Size(173, 38);
            this.muestraconectados.TabIndex = 25;
            this.muestraconectados.Text = "Muestra conectados";
            this.muestraconectados.UseVisualStyleBackColor = true;
            this.muestraconectados.Click += new System.EventHandler(this.muestraconectados_Click);
            // 
            // MostrarConectados
            // 
            this.MostrarConectados.ColumnHeadersHeightSizeMode = System.Windows.Forms.DataGridViewColumnHeadersHeightSizeMode.AutoSize;
            this.MostrarConectados.Columns.AddRange(new System.Windows.Forms.DataGridViewColumn[] {
            this.Usuario});
            this.MostrarConectados.Location = new System.Drawing.Point(773, 22);
            this.MostrarConectados.Name = "MostrarConectados";
            this.MostrarConectados.RowTemplate.Height = 24;
            this.MostrarConectados.Size = new System.Drawing.Size(240, 278);
            this.MostrarConectados.TabIndex = 24;
            this.MostrarConectados.CellContentClick += new System.Windows.Forms.DataGridViewCellEventHandler(this.MostrarConectados_CellContentClick);
            // 
            // Usuario
            // 
            this.Usuario.HeaderText = "Usuario";
            this.Usuario.Name = "Usuario";
            // 
            // consulta4
            // 
            this.consulta4.AutoSize = true;
            this.consulta4.Location = new System.Drawing.Point(72, 166);
            this.consulta4.Margin = new System.Windows.Forms.Padding(4);
            this.consulta4.Name = "consulta4";
            this.consulta4.Size = new System.Drawing.Size(347, 21);
            this.consulta4.TabIndex = 23;
            this.consulta4.TabStop = true;
            this.consulta4.Text = "Dame el número de partidas que ganó tal persona";
            this.consulta4.UseVisualStyleBackColor = true;
            this.consulta4.Visible = false;
            // 
            // label3
            // 
            this.label3.AutoSize = true;
            this.label3.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label3.Location = new System.Drawing.Point(16, 400);
            this.label3.Margin = new System.Windows.Forms.Padding(4, 0, 4, 0);
            this.label3.Name = "label3";
            this.label3.Size = new System.Drawing.Size(114, 25);
            this.label3.TabIndex = 22;
            this.label3.Text = "Contraseña";
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label1.Location = new System.Drawing.Point(16, 361);
            this.label1.Margin = new System.Windows.Forms.Padding(4, 0, 4, 0);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(81, 25);
            this.label1.TabIndex = 21;
            this.label1.Text = "Nombre";
            // 
            // textcontra
            // 
            this.textcontra.Location = new System.Drawing.Point(157, 404);
            this.textcontra.Margin = new System.Windows.Forms.Padding(4);
            this.textcontra.Name = "textcontra";
            this.textcontra.Size = new System.Drawing.Size(166, 22);
            this.textcontra.TabIndex = 20;
            // 
            // textnombre
            // 
            this.textnombre.Location = new System.Drawing.Point(157, 365);
            this.textnombre.Margin = new System.Windows.Forms.Padding(4);
            this.textnombre.Name = "textnombre";
            this.textnombre.Size = new System.Drawing.Size(166, 22);
            this.textnombre.TabIndex = 19;
            // 
            // entrar
            // 
            this.entrar.Location = new System.Drawing.Point(489, 365);
            this.entrar.Name = "entrar";
            this.entrar.Size = new System.Drawing.Size(113, 38);
            this.entrar.TabIndex = 18;
            this.entrar.Text = "Login";
            this.entrar.UseVisualStyleBackColor = true;
            this.entrar.Click += new System.EventHandler(this.entrar_Click);
            // 
            // registrar
            // 
            this.registrar.Location = new System.Drawing.Point(370, 365);
            this.registrar.Name = "registrar";
            this.registrar.Size = new System.Drawing.Size(113, 38);
            this.registrar.TabIndex = 17;
            this.registrar.Text = "Registrarse";
            this.registrar.UseVisualStyleBackColor = true;
            this.registrar.Click += new System.EventHandler(this.registrar_Click);
            // 
            // consulta3
            // 
            this.consulta3.AutoSize = true;
            this.consulta3.Location = new System.Drawing.Point(72, 137);
            this.consulta3.Margin = new System.Windows.Forms.Padding(4);
            this.consulta3.Name = "consulta3";
            this.consulta3.Size = new System.Drawing.Size(328, 21);
            this.consulta3.TabIndex = 16;
            this.consulta3.TabStop = true;
            this.consulta3.Text = "Dame el tiempo total que ha jugado tal persona";
            this.consulta3.UseVisualStyleBackColor = true;
            this.consulta3.Visible = false;
            // 
            // consulta2
            // 
            this.consulta2.AutoSize = true;
            this.consulta2.Location = new System.Drawing.Point(72, 108);
            this.consulta2.Margin = new System.Windows.Forms.Padding(4);
            this.consulta2.Name = "consulta2";
            this.consulta2.Size = new System.Drawing.Size(589, 21);
            this.consulta2.TabIndex = 15;
            this.consulta2.TabStop = true;
            this.consulta2.Text = "Dame la ID de la partida, la duración y el ganador de las partidas donde jugó tal" +
                " persona";
            this.consulta2.UseVisualStyleBackColor = true;
            this.consulta2.Visible = false;
            // 
            // button3
            // 
            this.button3.Location = new System.Drawing.Point(301, 22);
            this.button3.Name = "button3";
            this.button3.Size = new System.Drawing.Size(121, 23);
            this.button3.TabIndex = 14;
            this.button3.Text = "Desconectar";
            this.button3.UseVisualStyleBackColor = true;
            this.button3.Visible = false;
            this.button3.Click += new System.EventHandler(this.button3_Click);
            // 
            // button1
            // 
            this.button1.Location = new System.Drawing.Point(190, 22);
            this.button1.Name = "button1";
            this.button1.Size = new System.Drawing.Size(75, 23);
            this.button1.TabIndex = 13;
            this.button1.Text = "Conectar";
            this.button1.UseVisualStyleBackColor = true;
            this.button1.Visible = false;
            // 
            // consulta1
            // 
            this.consulta1.AutoSize = true;
            this.consulta1.Location = new System.Drawing.Point(72, 79);
            this.consulta1.Margin = new System.Windows.Forms.Padding(4);
            this.consulta1.Name = "consulta1";
            this.consulta1.Size = new System.Drawing.Size(411, 21);
            this.consulta1.TabIndex = 8;
            this.consulta1.TabStop = true;
            this.consulta1.Text = "Dame la ID de la partida y los puntos donde jugó tal persona";
            this.consulta1.UseVisualStyleBackColor = true;
            this.consulta1.Visible = false;
            // 
            // Form1
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(8F, 16F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(1058, 717);
            this.Controls.Add(this.groupBox1);
            this.Margin = new System.Windows.Forms.Padding(4);
            this.Name = "Form1";
            this.Text = "Form1";
            this.Load += new System.EventHandler(this.Form1_Load);
            this.groupBox1.ResumeLayout(false);
            this.groupBox1.PerformLayout();
            ((System.ComponentModel.ISupportInitialize)(this.MostrarConectados)).EndInit();
            this.ResumeLayout(false);

        }

        #endregion

        private System.Windows.Forms.Label label2;
        private System.Windows.Forms.TextBox nombre;
        private System.Windows.Forms.Button button2;
        private System.Windows.Forms.GroupBox groupBox1;
        private System.Windows.Forms.RadioButton consulta1;
        private System.Windows.Forms.Button button3;
        private System.Windows.Forms.Button button1;
        private System.Windows.Forms.RadioButton consulta2;
        private System.Windows.Forms.RadioButton consulta3;
        private System.Windows.Forms.Button entrar;
        private System.Windows.Forms.Button registrar;
        private System.Windows.Forms.Label label3;
        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.TextBox textcontra;
        private System.Windows.Forms.TextBox textnombre;
        private System.Windows.Forms.RadioButton consulta4;
        private System.Windows.Forms.DataGridView MostrarConectados;
        private System.Windows.Forms.DataGridViewTextBoxColumn Usuario;
        private System.Windows.Forms.Button muestraconectados;
    }
}
