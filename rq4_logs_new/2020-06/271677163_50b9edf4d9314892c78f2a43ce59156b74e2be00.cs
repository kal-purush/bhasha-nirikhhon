namespace ProyectoFinal_InteligenciaArtificial
{
    partial class Form1
    {
        /// <summary>
        /// Variable del diseñador necesaria.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// Limpiar los recursos que se estén usando.
        /// </summary>
        /// <param name="disposing">true si los recursos administrados se deben desechar; false en caso contrario.</param>
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
        /// el contenido de este método con el editor de código.
        /// </summary>
        private void InitializeComponent()
        {
            this.ltb_Computadores = new System.Windows.Forms.ListBox();
            this.cmb_procesadores = new System.Windows.Forms.ComboBox();
            this.btn_Cargar = new System.Windows.Forms.Button();
            this.label1 = new System.Windows.Forms.Label();
            this.label2 = new System.Windows.Forms.Label();
            this.label3 = new System.Windows.Forms.Label();
            this.label4 = new System.Windows.Forms.Label();
            this.label5 = new System.Windows.Forms.Label();
            this.label6 = new System.Windows.Forms.Label();
            this.label7 = new System.Windows.Forms.Label();
            this.label8 = new System.Windows.Forms.Label();
            this.label9 = new System.Windows.Forms.Label();
            this.cmb_PlacaMadre = new System.Windows.Forms.ComboBox();
            this.cmb_Ram = new System.Windows.Forms.ComboBox();
            this.cmb_GPU = new System.Windows.Forms.ComboBox();
            this.cmb_Gabinete = new System.Windows.Forms.ComboBox();
            this.cmb_Monitor = new System.Windows.Forms.ComboBox();
            this.cmb_Disco = new System.Windows.Forms.ComboBox();
            this.cmb_Periferico = new System.Windows.Forms.ComboBox();
            this.cmb_FuentePw = new System.Windows.Forms.ComboBox();
            this.label10 = new System.Windows.Forms.Label();
            this.txt_Presupuesto = new System.Windows.Forms.TextBox();
            this.SuspendLayout();
            // 
            // ltb_Computadores
            // 
            this.ltb_Computadores.FormattingEnabled = true;
            this.ltb_Computadores.Location = new System.Drawing.Point(43, 254);
            this.ltb_Computadores.Name = "ltb_Computadores";
            this.ltb_Computadores.Size = new System.Drawing.Size(412, 212);
            this.ltb_Computadores.TabIndex = 0;
            // 
            // cmb_procesadores
            // 
            this.cmb_procesadores.FormattingEnabled = true;
            this.cmb_procesadores.Items.AddRange(new object[] {
            "todos",
            "intel",
            "amd"});
            this.cmb_procesadores.Location = new System.Drawing.Point(130, 33);
            this.cmb_procesadores.Name = "cmb_procesadores";
            this.cmb_procesadores.Size = new System.Drawing.Size(97, 21);
            this.cmb_procesadores.TabIndex = 1;
            this.cmb_procesadores.Text = "todos";
            // 
            // btn_Cargar
            // 
            this.btn_Cargar.Location = new System.Drawing.Point(380, 225);
            this.btn_Cargar.Name = "btn_Cargar";
            this.btn_Cargar.Size = new System.Drawing.Size(75, 23);
            this.btn_Cargar.TabIndex = 2;
            this.btn_Cargar.Text = "Cargar";
            this.btn_Cargar.UseVisualStyleBackColor = true;
            this.btn_Cargar.Click += new System.EventHandler(this.btn_Cargar_Click);
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Location = new System.Drawing.Point(40, 36);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(61, 13);
            this.label1.TabIndex = 3;
            this.label1.Text = "Procesador";
            // 
            // label2
            // 
            this.label2.AutoSize = true;
            this.label2.Location = new System.Drawing.Point(40, 70);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(67, 13);
            this.label2.TabIndex = 4;
            this.label2.Text = "Placa Madre";
            // 
            // label3
            // 
            this.label3.AutoSize = true;
            this.label3.Location = new System.Drawing.Point(40, 109);
            this.label3.Name = "label3";
            this.label3.Size = new System.Drawing.Size(29, 13);
            this.label3.TabIndex = 5;
            this.label3.Text = "Ram";
            // 
            // label4
            // 
            this.label4.AutoSize = true;
            this.label4.Location = new System.Drawing.Point(40, 144);
            this.label4.Name = "label4";
            this.label4.Size = new System.Drawing.Size(30, 13);
            this.label4.TabIndex = 6;
            this.label4.Text = "GPU";
            // 
            // label5
            // 
            this.label5.AutoSize = true;
            this.label5.Location = new System.Drawing.Point(282, 144);
            this.label5.Name = "label5";
            this.label5.Size = new System.Drawing.Size(42, 13);
            this.label5.TabIndex = 10;
            this.label5.Text = "Monitor";
            // 
            // label6
            // 
            this.label6.AutoSize = true;
            this.label6.Location = new System.Drawing.Point(282, 109);
            this.label6.Name = "label6";
            this.label6.Size = new System.Drawing.Size(34, 13);
            this.label6.TabIndex = 9;
            this.label6.Text = "Disco";
            // 
            // label7
            // 
            this.label7.AutoSize = true;
            this.label7.Location = new System.Drawing.Point(282, 70);
            this.label7.Name = "label7";
            this.label7.Size = new System.Drawing.Size(51, 13);
            this.label7.TabIndex = 8;
            this.label7.Text = "Periferico";
            // 
            // label8
            // 
            this.label8.AutoSize = true;
            this.label8.Location = new System.Drawing.Point(282, 36);
            this.label8.Name = "label8";
            this.label8.Size = new System.Drawing.Size(58, 13);
            this.label8.TabIndex = 7;
            this.label8.Text = "Fuente Pw";
            // 
            // label9
            // 
            this.label9.AutoSize = true;
            this.label9.Location = new System.Drawing.Point(40, 178);
            this.label9.Name = "label9";
            this.label9.Size = new System.Drawing.Size(50, 13);
            this.label9.TabIndex = 11;
            this.label9.Text = "Gabinete";
            // 
            // cmb_PlacaMadre
            // 
            this.cmb_PlacaMadre.FormattingEnabled = true;
            this.cmb_PlacaMadre.Items.AddRange(new object[] {
            "todos",
            "msi",
            "asus"});
            this.cmb_PlacaMadre.Location = new System.Drawing.Point(130, 67);
            this.cmb_PlacaMadre.Name = "cmb_PlacaMadre";
            this.cmb_PlacaMadre.Size = new System.Drawing.Size(97, 21);
            this.cmb_PlacaMadre.TabIndex = 12;
            this.cmb_PlacaMadre.Text = "todos";
            // 
            // cmb_Ram
            // 
            this.cmb_Ram.FormattingEnabled = true;
            this.cmb_Ram.Items.AddRange(new object[] {
            "todos",
            "ddr_tres",
            "ddr_cuatro"});
            this.cmb_Ram.Location = new System.Drawing.Point(130, 101);
            this.cmb_Ram.Name = "cmb_Ram";
            this.cmb_Ram.Size = new System.Drawing.Size(97, 21);
            this.cmb_Ram.TabIndex = 13;
            this.cmb_Ram.Text = "todos";
            // 
            // cmb_GPU
            // 
            this.cmb_GPU.FormattingEnabled = true;
            this.cmb_GPU.Items.AddRange(new object[] {
            "todos",
            "nvidia",
            "razen"});
            this.cmb_GPU.Location = new System.Drawing.Point(130, 136);
            this.cmb_GPU.Name = "cmb_GPU";
            this.cmb_GPU.Size = new System.Drawing.Size(97, 21);
            this.cmb_GPU.TabIndex = 14;
            this.cmb_GPU.Text = "todos";
            // 
            // cmb_Gabinete
            // 
            this.cmb_Gabinete.FormattingEnabled = true;
            this.cmb_Gabinete.Items.AddRange(new object[] {
            "todos",
            "corsair",
            "cooler_Master"});
            this.cmb_Gabinete.Location = new System.Drawing.Point(130, 170);
            this.cmb_Gabinete.Name = "cmb_Gabinete";
            this.cmb_Gabinete.Size = new System.Drawing.Size(97, 21);
            this.cmb_Gabinete.TabIndex = 15;
            this.cmb_Gabinete.Text = "todos";
            // 
            // cmb_Monitor
            // 
            this.cmb_Monitor.FormattingEnabled = true;
            this.cmb_Monitor.Items.AddRange(new object[] {
            "todos",
            "asus",
            "hp"});
            this.cmb_Monitor.Location = new System.Drawing.Point(358, 141);
            this.cmb_Monitor.Name = "cmb_Monitor";
            this.cmb_Monitor.Size = new System.Drawing.Size(97, 21);
            this.cmb_Monitor.TabIndex = 19;
            this.cmb_Monitor.Text = "todos";
            // 
            // cmb_Disco
            // 
            this.cmb_Disco.FormattingEnabled = true;
            this.cmb_Disco.Items.AddRange(new object[] {
            "todos",
            "kingstong",
            "samsumg"});
            this.cmb_Disco.Location = new System.Drawing.Point(358, 106);
            this.cmb_Disco.Name = "cmb_Disco";
            this.cmb_Disco.Size = new System.Drawing.Size(97, 21);
            this.cmb_Disco.TabIndex = 18;
            this.cmb_Disco.Text = "todos";
            // 
            // cmb_Periferico
            // 
            this.cmb_Periferico.FormattingEnabled = true;
            this.cmb_Periferico.Items.AddRange(new object[] {
            "todos",
            "corsair",
            "cooler_Master"});
            this.cmb_Periferico.Location = new System.Drawing.Point(358, 67);
            this.cmb_Periferico.Name = "cmb_Periferico";
            this.cmb_Periferico.Size = new System.Drawing.Size(97, 21);
            this.cmb_Periferico.TabIndex = 17;
            this.cmb_Periferico.Text = "todos";
            // 
            // cmb_FuentePw
            // 
            this.cmb_FuentePw.FormattingEnabled = true;
            this.cmb_FuentePw.Items.AddRange(new object[] {
            "todos",
            "cooler_Master",
            "corsair"});
            this.cmb_FuentePw.Location = new System.Drawing.Point(358, 33);
            this.cmb_FuentePw.Name = "cmb_FuentePw";
            this.cmb_FuentePw.Size = new System.Drawing.Size(97, 21);
            this.cmb_FuentePw.TabIndex = 16;
            this.cmb_FuentePw.Text = "todos";
            // 
            // label10
            // 
            this.label10.AutoSize = true;
            this.label10.Location = new System.Drawing.Point(282, 178);
            this.label10.Name = "label10";
            this.label10.Size = new System.Drawing.Size(66, 13);
            this.label10.TabIndex = 20;
            this.label10.Text = "Presupuesto";
            // 
            // txt_Presupuesto
            // 
            this.txt_Presupuesto.Location = new System.Drawing.Point(358, 178);
            this.txt_Presupuesto.Name = "txt_Presupuesto";
            this.txt_Presupuesto.Size = new System.Drawing.Size(97, 20);
            this.txt_Presupuesto.TabIndex = 21;
            // 
            // Form1
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(517, 478);
            this.Controls.Add(this.txt_Presupuesto);
            this.Controls.Add(this.label10);
            this.Controls.Add(this.cmb_Monitor);
            this.Controls.Add(this.cmb_Disco);
            this.Controls.Add(this.cmb_Periferico);
            this.Controls.Add(this.cmb_FuentePw);
            this.Controls.Add(this.cmb_Gabinete);
            this.Controls.Add(this.cmb_GPU);
            this.Controls.Add(this.cmb_Ram);
            this.Controls.Add(this.cmb_PlacaMadre);
            this.Controls.Add(this.label9);
            this.Controls.Add(this.label5);
            this.Controls.Add(this.label6);
            this.Controls.Add(this.label7);
            this.Controls.Add(this.label8);
            this.Controls.Add(this.label4);
            this.Controls.Add(this.label3);
            this.Controls.Add(this.label2);
            this.Controls.Add(this.label1);
            this.Controls.Add(this.btn_Cargar);
            this.Controls.Add(this.cmb_procesadores);
            this.Controls.Add(this.ltb_Computadores);
            this.Name = "Form1";
            this.Text = "Form1";
            this.Load += new System.EventHandler(this.Form1_Load);
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.ListBox ltb_Computadores;
        private System.Windows.Forms.ComboBox cmb_procesadores;
        private System.Windows.Forms.Button btn_Cargar;
        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.Label label2;
        private System.Windows.Forms.Label label3;
        private System.Windows.Forms.Label label4;
        private System.Windows.Forms.Label label5;
        private System.Windows.Forms.Label label6;
        private System.Windows.Forms.Label label7;
        private System.Windows.Forms.Label label8;
        private System.Windows.Forms.Label label9;
        private System.Windows.Forms.ComboBox cmb_PlacaMadre;
        private System.Windows.Forms.ComboBox cmb_Ram;
        private System.Windows.Forms.ComboBox cmb_GPU;
        private System.Windows.Forms.ComboBox cmb_Gabinete;
        private System.Windows.Forms.ComboBox cmb_Monitor;
        private System.Windows.Forms.ComboBox cmb_Disco;
        private System.Windows.Forms.ComboBox cmb_Periferico;
        private System.Windows.Forms.ComboBox cmb_FuentePw;
        private System.Windows.Forms.Label label10;
        private System.Windows.Forms.TextBox txt_Presupuesto;
    }
}
