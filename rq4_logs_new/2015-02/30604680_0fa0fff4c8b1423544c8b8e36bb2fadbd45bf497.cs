namespace WebCargo
{
    partial class flowLayoutPanel1
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
            this.txtArquivo = new System.Windows.Forms.TextBox();
            this.btnSair = new System.Windows.Forms.Button();
            this.menuStrip1 = new System.Windows.Forms.MenuStrip();
            this.menuToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.selecionarArquivosToolStripMenuItem1 = new System.Windows.Forms.ToolStripMenuItem();
            this.opçãoToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.btnExecutar = new System.Windows.Forms.Button();
            this.btnobfuscar = new System.Windows.Forms.RadioButton();
            this.btnMinificar = new System.Windows.Forms.RadioButton();
            this.txtLog = new System.Windows.Forms.TextBox();
            this.abrirpasta = new System.Windows.Forms.FolderBrowserDialog();
            this.ofd1 = new System.Windows.Forms.OpenFileDialog();
            this.menuStrip1.SuspendLayout();
            this.SuspendLayout();
            // 
            // txtArquivo
            // 
            this.txtArquivo.Location = new System.Drawing.Point(12, 30);
            this.txtArquivo.Multiline = true;
            this.txtArquivo.Name = "txtArquivo";
            this.txtArquivo.Size = new System.Drawing.Size(408, 241);
            this.txtArquivo.TabIndex = 1;
            this.txtArquivo.TextChanged += new System.EventHandler(this.txtArquivo_TextChanged);
            // 
            // btnSair
            // 
            this.btnSair.Location = new System.Drawing.Point(325, 376);
            this.btnSair.Name = "btnSair";
            this.btnSair.Size = new System.Drawing.Size(95, 36);
            this.btnSair.TabIndex = 2;
            this.btnSair.Text = "Sair";
            this.btnSair.UseVisualStyleBackColor = true;
            this.btnSair.Click += new System.EventHandler(this.btnSair_Click);
            // 
            // menuStrip1
            // 
            this.menuStrip1.Items.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.menuToolStripMenuItem,
            this.opçãoToolStripMenuItem});
            this.menuStrip1.Location = new System.Drawing.Point(0, 0);
            this.menuStrip1.Name = "menuStrip1";
            this.menuStrip1.Size = new System.Drawing.Size(434, 24);
            this.menuStrip1.TabIndex = 4;
            this.menuStrip1.Text = "menuStrip1";
            // 
            // menuToolStripMenuItem
            // 
            this.menuToolStripMenuItem.DropDownItems.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.selecionarArquivosToolStripMenuItem1});
            this.menuToolStripMenuItem.Name = "menuToolStripMenuItem";
            this.menuToolStripMenuItem.Size = new System.Drawing.Size(50, 20);
            this.menuToolStripMenuItem.Text = "Menu";
            this.menuToolStripMenuItem.Click += new System.EventHandler(this.menuToolStripMenuItem_Click);
            // 
            // selecionarArquivosToolStripMenuItem1
            // 
            this.selecionarArquivosToolStripMenuItem1.Name = "selecionarArquivosToolStripMenuItem1";
            this.selecionarArquivosToolStripMenuItem1.Size = new System.Drawing.Size(178, 22);
            this.selecionarArquivosToolStripMenuItem1.Text = "Selecionar Arquivos";
            this.selecionarArquivosToolStripMenuItem1.Click += new System.EventHandler(this.selecionarArquivosToolStripMenuItem1_Click);
            // 
            // opçãoToolStripMenuItem
            // 
            this.opçãoToolStripMenuItem.Name = "opçãoToolStripMenuItem";
            this.opçãoToolStripMenuItem.Size = new System.Drawing.Size(54, 20);
            this.opçãoToolStripMenuItem.Text = "Opção";
            // 
            // btnExecutar
            // 
            this.btnExecutar.Location = new System.Drawing.Point(222, 376);
            this.btnExecutar.Name = "btnExecutar";
            this.btnExecutar.Size = new System.Drawing.Size(97, 36);
            this.btnExecutar.TabIndex = 8;
            this.btnExecutar.Text = "Executar";
            this.btnExecutar.UseVisualStyleBackColor = true;
            this.btnExecutar.Click += new System.EventHandler(this.button1_Click);
            // 
            // btnobfuscar
            // 
            this.btnobfuscar.AutoSize = true;
            this.btnobfuscar.Location = new System.Drawing.Point(292, 7);
            this.btnobfuscar.Name = "btnobfuscar";
            this.btnobfuscar.Size = new System.Drawing.Size(68, 17);
            this.btnobfuscar.TabIndex = 9;
            this.btnobfuscar.TabStop = true;
            this.btnobfuscar.Text = "Obfuscar";
            this.btnobfuscar.UseVisualStyleBackColor = true;
            this.btnobfuscar.CheckedChanged += new System.EventHandler(this.radioButton1_CheckedChanged);
            // 
            // btnMinificar
            // 
            this.btnMinificar.AutoSize = true;
            this.btnMinificar.Location = new System.Drawing.Point(366, 7);
            this.btnMinificar.Name = "btnMinificar";
            this.btnMinificar.Size = new System.Drawing.Size(64, 17);
            this.btnMinificar.TabIndex = 10;
            this.btnMinificar.TabStop = true;
            this.btnMinificar.Text = "Minificar";
            this.btnMinificar.UseVisualStyleBackColor = true;
            // 
            // txtLog
            // 
            this.txtLog.Location = new System.Drawing.Point(12, 277);
            this.txtLog.Multiline = true;
            this.txtLog.Name = "txtLog";
            this.txtLog.Size = new System.Drawing.Size(408, 93);
            this.txtLog.TabIndex = 11;
            this.txtLog.TextChanged += new System.EventHandler(this.textBox1_TextChanged);
            // 
            // abrirpasta
            // 
            this.abrirpasta.HelpRequest += new System.EventHandler(this.abrirpasta_HelpRequest);
            // 
            // ofd1
            // 
            this.ofd1.FileName = "ofd1";
            this.ofd1.FileOk += new System.ComponentModel.CancelEventHandler(this.ofd1_FileOk);
            // 
            // flowLayoutPanel1
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(434, 424);
            this.Controls.Add(this.txtLog);
            this.Controls.Add(this.btnMinificar);
            this.Controls.Add(this.btnobfuscar);
            this.Controls.Add(this.btnExecutar);
            this.Controls.Add(this.btnSair);
            this.Controls.Add(this.txtArquivo);
            this.Controls.Add(this.menuStrip1);
            this.MainMenuStrip = this.menuStrip1;
            this.Name = "flowLayoutPanel1";
            this.Text = "Form1";
            this.Load += new System.EventHandler(this.Form1_Load);
            this.menuStrip1.ResumeLayout(false);
            this.menuStrip1.PerformLayout();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.TextBox txtArquivo;
        private System.Windows.Forms.Button btnSair;
        private System.Windows.Forms.MenuStrip menuStrip1;
        private System.Windows.Forms.ToolStripMenuItem menuToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem selecionarArquivosToolStripMenuItem1;
        private System.Windows.Forms.ToolStripMenuItem opçãoToolStripMenuItem;
        private System.Windows.Forms.Button btnExecutar;
        private System.Windows.Forms.RadioButton btnobfuscar;
        private System.Windows.Forms.RadioButton btnMinificar;
        private System.Windows.Forms.TextBox txtLog;
        private System.Windows.Forms.FolderBrowserDialog abrirpasta;
        private System.Windows.Forms.OpenFileDialog ofd1;
    }
}
