namespace Notas
{
    partial class Form1
    {
        /// <summary>
        /// Variável de designer necessária.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// Limpar os recursos que estão sendo usados.
        /// </summary>
        /// <param name="disposing">true se for necessário descartar os recursos gerenciados; caso contrário, false.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Código gerado pelo Windows Form Designer

        /// <summary>
        /// Método necessário para suporte ao Designer - não modifique 
        /// o conteúdo deste método com o editor de código.
        /// </summary>
        private void InitializeComponent()
        {
            System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(Form1));
            this.menuStrip1 = new System.Windows.Forms.MenuStrip();
            this.arquivoToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.novoCtrlNToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.abrirCtrlOToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.toolStripSeparator1 = new System.Windows.Forms.ToolStripSeparator();
            this.salvarToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.salvarComoToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.toolStripSeparator2 = new System.Windows.Forms.ToolStripSeparator();
            this.sairCtrlQToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.editarToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.recortarCtrlXToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.copiarCtrlCToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.colarCtrlVToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.apagarDelToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.toolStripSeparator3 = new System.Windows.Forms.ToolStripSeparator();
            this.selecionarTudoCtrlAToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.formatarToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.fonteToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.temaToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.padrãoToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.pretoToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.cinzaToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.ajudaToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.sobreToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.richTextBox1 = new System.Windows.Forms.RichTextBox();
            this.openFileDialog1 = new System.Windows.Forms.OpenFileDialog();
            this.saveFileDialog1 = new System.Windows.Forms.SaveFileDialog();
            this.fontDialog1 = new System.Windows.Forms.FontDialog();
            this.titulo = new System.Windows.Forms.Label();
            this.pictureBox1 = new System.Windows.Forms.PictureBox();
            this.btnMaximizar = new System.Windows.Forms.PictureBox();
            this.btnSair = new System.Windows.Forms.PictureBox();
            this.pictureBox2 = new System.Windows.Forms.PictureBox();
            this.panel1 = new System.Windows.Forms.Panel();
            this.menuStrip1.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)(this.pictureBox1)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.btnMaximizar)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.btnSair)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.pictureBox2)).BeginInit();
            this.panel1.SuspendLayout();
            this.SuspendLayout();
            // 
            // menuStrip1
            // 
            this.menuStrip1.Dock = System.Windows.Forms.DockStyle.None;
            this.menuStrip1.Items.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.arquivoToolStripMenuItem,
            this.editarToolStripMenuItem,
            this.formatarToolStripMenuItem,
            this.temaToolStripMenuItem,
            this.ajudaToolStripMenuItem});
            this.menuStrip1.Location = new System.Drawing.Point(3, 26);
            this.menuStrip1.Name = "menuStrip1";
            this.menuStrip1.Size = new System.Drawing.Size(402, 24);
            this.menuStrip1.TabIndex = 0;
            this.menuStrip1.Text = "menuStrip1";
            // 
            // arquivoToolStripMenuItem
            // 
            this.arquivoToolStripMenuItem.DropDownItems.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.novoCtrlNToolStripMenuItem,
            this.abrirCtrlOToolStripMenuItem,
            this.toolStripSeparator1,
            this.salvarToolStripMenuItem,
            this.salvarComoToolStripMenuItem,
            this.toolStripSeparator2,
            this.sairCtrlQToolStripMenuItem});
            this.arquivoToolStripMenuItem.Name = "arquivoToolStripMenuItem";
            this.arquivoToolStripMenuItem.Size = new System.Drawing.Size(61, 20);
            this.arquivoToolStripMenuItem.Text = "Arquivo";
            // 
            // novoCtrlNToolStripMenuItem
            // 
            this.novoCtrlNToolStripMenuItem.Name = "novoCtrlNToolStripMenuItem";
            this.novoCtrlNToolStripMenuItem.Size = new System.Drawing.Size(186, 22);
            this.novoCtrlNToolStripMenuItem.Text = "Novo             (Ctrl+N)";
            this.novoCtrlNToolStripMenuItem.Click += new System.EventHandler(this.novoCtrlNToolStripMenuItem_Click);
            // 
            // abrirCtrlOToolStripMenuItem
            // 
            this.abrirCtrlOToolStripMenuItem.Name = "abrirCtrlOToolStripMenuItem";
            this.abrirCtrlOToolStripMenuItem.Size = new System.Drawing.Size(186, 22);
            this.abrirCtrlOToolStripMenuItem.Text = "Abrir              (Ctrl+O)";
            this.abrirCtrlOToolStripMenuItem.Click += new System.EventHandler(this.abrirCtrlOToolStripMenuItem_Click);
            // 
            // toolStripSeparator1
            // 
            this.toolStripSeparator1.Name = "toolStripSeparator1";
            this.toolStripSeparator1.Size = new System.Drawing.Size(183, 6);
            // 
            // salvarToolStripMenuItem
            // 
            this.salvarToolStripMenuItem.Name = "salvarToolStripMenuItem";
            this.salvarToolStripMenuItem.Size = new System.Drawing.Size(186, 22);
            this.salvarToolStripMenuItem.Text = "Salvar             (Ctrl+S)";
            this.salvarToolStripMenuItem.Click += new System.EventHandler(this.salvarToolStripMenuItem_Click);
            // 
            // salvarComoToolStripMenuItem
            // 
            this.salvarComoToolStripMenuItem.Name = "salvarComoToolStripMenuItem";
            this.salvarComoToolStripMenuItem.Size = new System.Drawing.Size(186, 22);
            this.salvarComoToolStripMenuItem.Text = "Salvar como";
            this.salvarComoToolStripMenuItem.Click += new System.EventHandler(this.salvarComoToolStripMenuItem_Click);
            // 
            // toolStripSeparator2
            // 
            this.toolStripSeparator2.Name = "toolStripSeparator2";
            this.toolStripSeparator2.Size = new System.Drawing.Size(183, 6);
            // 
            // sairCtrlQToolStripMenuItem
            // 
            this.sairCtrlQToolStripMenuItem.Name = "sairCtrlQToolStripMenuItem";
            this.sairCtrlQToolStripMenuItem.Size = new System.Drawing.Size(186, 22);
            this.sairCtrlQToolStripMenuItem.Text = "Sair                (Ctrl+Q)";
            this.sairCtrlQToolStripMenuItem.Click += new System.EventHandler(this.sairCtrlQToolStripMenuItem_Click);
            // 
            // editarToolStripMenuItem
            // 
            this.editarToolStripMenuItem.DropDownItems.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.recortarCtrlXToolStripMenuItem,
            this.copiarCtrlCToolStripMenuItem,
            this.colarCtrlVToolStripMenuItem,
            this.apagarDelToolStripMenuItem,
            this.toolStripSeparator3,
            this.selecionarTudoCtrlAToolStripMenuItem});
            this.editarToolStripMenuItem.Name = "editarToolStripMenuItem";
            this.editarToolStripMenuItem.Size = new System.Drawing.Size(49, 20);
            this.editarToolStripMenuItem.Text = "Editar";
            // 
            // recortarCtrlXToolStripMenuItem
            // 
            this.recortarCtrlXToolStripMenuItem.Name = "recortarCtrlXToolStripMenuItem";
            this.recortarCtrlXToolStripMenuItem.Size = new System.Drawing.Size(200, 22);
            this.recortarCtrlXToolStripMenuItem.Text = "Recortar             (Ctrl+X)";
            this.recortarCtrlXToolStripMenuItem.Click += new System.EventHandler(this.recortarCtrlXToolStripMenuItem_Click);
            // 
            // copiarCtrlCToolStripMenuItem
            // 
            this.copiarCtrlCToolStripMenuItem.Name = "copiarCtrlCToolStripMenuItem";
            this.copiarCtrlCToolStripMenuItem.Size = new System.Drawing.Size(200, 22);
            this.copiarCtrlCToolStripMenuItem.Text = "Copiar                (Ctrl+C)";
            this.copiarCtrlCToolStripMenuItem.Click += new System.EventHandler(this.copiarCtrlCToolStripMenuItem_Click);
            // 
            // colarCtrlVToolStripMenuItem
            // 
            this.colarCtrlVToolStripMenuItem.Name = "colarCtrlVToolStripMenuItem";
            this.colarCtrlVToolStripMenuItem.Size = new System.Drawing.Size(200, 22);
            this.colarCtrlVToolStripMenuItem.Text = "Colar                  (Ctrl+V)";
            this.colarCtrlVToolStripMenuItem.Click += new System.EventHandler(this.colarCtrlVToolStripMenuItem_Click);
            // 
            // apagarDelToolStripMenuItem
            // 
            this.apagarDelToolStripMenuItem.Name = "apagarDelToolStripMenuItem";
            this.apagarDelToolStripMenuItem.Size = new System.Drawing.Size(200, 22);
            this.apagarDelToolStripMenuItem.Text = "Apagar               (Del)";
            this.apagarDelToolStripMenuItem.Click += new System.EventHandler(this.apagarDelToolStripMenuItem_Click);
            // 
            // toolStripSeparator3
            // 
            this.toolStripSeparator3.Name = "toolStripSeparator3";
            this.toolStripSeparator3.Size = new System.Drawing.Size(197, 6);
            // 
            // selecionarTudoCtrlAToolStripMenuItem
            // 
            this.selecionarTudoCtrlAToolStripMenuItem.Name = "selecionarTudoCtrlAToolStripMenuItem";
            this.selecionarTudoCtrlAToolStripMenuItem.Size = new System.Drawing.Size(200, 22);
            this.selecionarTudoCtrlAToolStripMenuItem.Text = "Selec. Tudo       (Ctrl+A)";
            this.selecionarTudoCtrlAToolStripMenuItem.Click += new System.EventHandler(this.selecionarTudoCtrlAToolStripMenuItem_Click);
            // 
            // formatarToolStripMenuItem
            // 
            this.formatarToolStripMenuItem.DropDownItems.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.fonteToolStripMenuItem});
            this.formatarToolStripMenuItem.Name = "formatarToolStripMenuItem";
            this.formatarToolStripMenuItem.Size = new System.Drawing.Size(67, 20);
            this.formatarToolStripMenuItem.Text = "Formatar";
            // 
            // fonteToolStripMenuItem
            // 
            this.fonteToolStripMenuItem.Name = "fonteToolStripMenuItem";
            this.fonteToolStripMenuItem.Size = new System.Drawing.Size(199, 22);
            this.fonteToolStripMenuItem.Text = "Fonte                  (Ctrl+F)";
            this.fonteToolStripMenuItem.Click += new System.EventHandler(this.fonteToolStripMenuItem_Click);
            // 
            // temaToolStripMenuItem
            // 
            this.temaToolStripMenuItem.DropDownItems.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.padrãoToolStripMenuItem,
            this.pretoToolStripMenuItem,
            this.cinzaToolStripMenuItem});
            this.temaToolStripMenuItem.Name = "temaToolStripMenuItem";
            this.temaToolStripMenuItem.Size = new System.Drawing.Size(47, 20);
            this.temaToolStripMenuItem.Text = "Tema";
            // 
            // padrãoToolStripMenuItem
            // 
            this.padrãoToolStripMenuItem.Name = "padrãoToolStripMenuItem";
            this.padrãoToolStripMenuItem.Size = new System.Drawing.Size(111, 22);
            this.padrãoToolStripMenuItem.Text = "Padrão";
            this.padrãoToolStripMenuItem.Click += new System.EventHandler(this.padrãoToolStripMenuItem_Click);
            // 
            // pretoToolStripMenuItem
            // 
            this.pretoToolStripMenuItem.Name = "pretoToolStripMenuItem";
            this.pretoToolStripMenuItem.Size = new System.Drawing.Size(111, 22);
            this.pretoToolStripMenuItem.Text = "Preto";
            this.pretoToolStripMenuItem.Click += new System.EventHandler(this.pretoToolStripMenuItem_Click);
            // 
            // cinzaToolStripMenuItem
            // 
            this.cinzaToolStripMenuItem.Name = "cinzaToolStripMenuItem";
            this.cinzaToolStripMenuItem.Size = new System.Drawing.Size(111, 22);
            this.cinzaToolStripMenuItem.Text = "Cinza";
            this.cinzaToolStripMenuItem.Click += new System.EventHandler(this.cinzaToolStripMenuItem_Click);
            // 
            // ajudaToolStripMenuItem
            // 
            this.ajudaToolStripMenuItem.DropDownItems.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.sobreToolStripMenuItem});
            this.ajudaToolStripMenuItem.Name = "ajudaToolStripMenuItem";
            this.ajudaToolStripMenuItem.Size = new System.Drawing.Size(50, 20);
            this.ajudaToolStripMenuItem.Text = "Ajuda";
            // 
            // sobreToolStripMenuItem
            // 
            this.sobreToolStripMenuItem.Name = "sobreToolStripMenuItem";
            this.sobreToolStripMenuItem.Size = new System.Drawing.Size(180, 22);
            this.sobreToolStripMenuItem.Text = "Sobre";
            this.sobreToolStripMenuItem.Click += new System.EventHandler(this.sobreToolStripMenuItem_Click);
            // 
            // richTextBox1
            // 
            this.richTextBox1.Anchor = ((System.Windows.Forms.AnchorStyles)((((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom) 
            | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.richTextBox1.Location = new System.Drawing.Point(2, 56);
            this.richTextBox1.Name = "richTextBox1";
            this.richTextBox1.ScrollBars = System.Windows.Forms.RichTextBoxScrollBars.ForcedVertical;
            this.richTextBox1.Size = new System.Drawing.Size(797, 391);
            this.richTextBox1.TabIndex = 2;
            this.richTextBox1.Text = "";
            // 
            // openFileDialog1
            // 
            this.openFileDialog1.FileName = "Arquivo.txt";
            // 
            // saveFileDialog1
            // 
            this.saveFileDialog1.FileName = "Arquivo";
            this.saveFileDialog1.Filter = "Arquivo C#|*.cs|Arquivo C++|*.cpp|Arquivo C|*.c|Arquivo JavaScript|*.js|Arquivo H" +
    "TML|*.html|Arquivo CSS|*.css|Script Linux|*.sh|Script Windows|*.bat|Arquivo de t" +
    "exto|*.txt|Todos os arquivos|*.*";
            // 
            // titulo
            // 
            this.titulo.AutoSize = true;
            this.titulo.Location = new System.Drawing.Point(32, 6);
            this.titulo.Name = "titulo";
            this.titulo.Size = new System.Drawing.Size(80, 13);
            this.titulo.TabIndex = 7;
            this.titulo.Text = "Bloco de Notas";
            // 
            // pictureBox1
            // 
            this.pictureBox1.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Right)));
            this.pictureBox1.Image = global::Notas.Properties.Resources.minimizeNormalx_Image;
            this.pictureBox1.Location = new System.Drawing.Point(738, 6);
            this.pictureBox1.Name = "pictureBox1";
            this.pictureBox1.Size = new System.Drawing.Size(17, 21);
            this.pictureBox1.TabIndex = 5;
            this.pictureBox1.TabStop = false;
            this.pictureBox1.Click += new System.EventHandler(this.pictureBox1_Click);
            // 
            // btnMaximizar
            // 
            this.btnMaximizar.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Right)));
            this.btnMaximizar.Image = global::Notas.Properties.Resources.maximizeNormalx_Image;
            this.btnMaximizar.Location = new System.Drawing.Point(757, 6);
            this.btnMaximizar.Name = "btnMaximizar";
            this.btnMaximizar.Size = new System.Drawing.Size(18, 21);
            this.btnMaximizar.TabIndex = 4;
            this.btnMaximizar.TabStop = false;
            this.btnMaximizar.Click += new System.EventHandler(this.btnMaximizar_Click);
            // 
            // btnSair
            // 
            this.btnSair.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Right)));
            this.btnSair.Image = global::Notas.Properties.Resources.closeNormalx_Image;
            this.btnSair.Location = new System.Drawing.Point(776, 6);
            this.btnSair.Name = "btnSair";
            this.btnSair.Size = new System.Drawing.Size(16, 21);
            this.btnSair.TabIndex = 3;
            this.btnSair.TabStop = false;
            this.btnSair.Click += new System.EventHandler(this.btnSair_Click);
            // 
            // pictureBox2
            // 
            this.pictureBox2.ErrorImage = global::Notas.Properties.Resources.icone;
            this.pictureBox2.Image = global::Notas.Properties.Resources.Webp_net_resizeimage;
            this.pictureBox2.Location = new System.Drawing.Point(3, 0);
            this.pictureBox2.Name = "pictureBox2";
            this.pictureBox2.Size = new System.Drawing.Size(23, 21);
            this.pictureBox2.TabIndex = 6;
            this.pictureBox2.TabStop = false;
            // 
            // panel1
            // 
            this.panel1.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.panel1.Controls.Add(this.pictureBox2);
            this.panel1.Controls.Add(this.pictureBox1);
            this.panel1.Controls.Add(this.btnMaximizar);
            this.panel1.Controls.Add(this.titulo);
            this.panel1.Controls.Add(this.btnSair);
            this.panel1.Controls.Add(this.menuStrip1);
            this.panel1.Location = new System.Drawing.Point(2, 3);
            this.panel1.Name = "panel1";
            this.panel1.Size = new System.Drawing.Size(797, 52);
            this.panel1.TabIndex = 8;
            this.panel1.MouseDown += new System.Windows.Forms.MouseEventHandler(this.panel1_MouseDown);
            this.panel1.MouseMove += new System.Windows.Forms.MouseEventHandler(this.panel1_MouseMove);
            this.panel1.MouseUp += new System.Windows.Forms.MouseEventHandler(this.panel1_MouseUp);
            // 
            // Form1
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(800, 450);
            this.Controls.Add(this.panel1);
            this.Controls.Add(this.richTextBox1);
            this.FormBorderStyle = System.Windows.Forms.FormBorderStyle.None;
            this.Icon = ((System.Drawing.Icon)(resources.GetObject("$this.Icon")));
            this.MainMenuStrip = this.menuStrip1;
            this.Name = "Form1";
            this.Text = "Bloco de Notas";
            this.menuStrip1.ResumeLayout(false);
            this.menuStrip1.PerformLayout();
            ((System.ComponentModel.ISupportInitialize)(this.pictureBox1)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.btnMaximizar)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.btnSair)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.pictureBox2)).EndInit();
            this.panel1.ResumeLayout(false);
            this.panel1.PerformLayout();
            this.ResumeLayout(false);

        }

        #endregion

        private System.Windows.Forms.MenuStrip menuStrip1;
        private System.Windows.Forms.ToolStripMenuItem arquivoToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem novoCtrlNToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem abrirCtrlOToolStripMenuItem;
        private System.Windows.Forms.ToolStripSeparator toolStripSeparator1;
        private System.Windows.Forms.ToolStripMenuItem salvarToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem salvarComoToolStripMenuItem;
        private System.Windows.Forms.ToolStripSeparator toolStripSeparator2;
        private System.Windows.Forms.ToolStripMenuItem sairCtrlQToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem editarToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem formatarToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem ajudaToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem sobreToolStripMenuItem;
        private System.Windows.Forms.RichTextBox richTextBox1;
        private System.Windows.Forms.OpenFileDialog openFileDialog1;
        private System.Windows.Forms.SaveFileDialog saveFileDialog1;
        private System.Windows.Forms.ToolStripMenuItem copiarCtrlCToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem recortarCtrlXToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem colarCtrlVToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem apagarDelToolStripMenuItem;
        private System.Windows.Forms.ToolStripSeparator toolStripSeparator3;
        private System.Windows.Forms.ToolStripMenuItem selecionarTudoCtrlAToolStripMenuItem;
        private System.Windows.Forms.FontDialog fontDialog1;
        private System.Windows.Forms.ToolStripMenuItem fonteToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem temaToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem padrãoToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem pretoToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem cinzaToolStripMenuItem;
        private System.Windows.Forms.PictureBox btnSair;
        private System.Windows.Forms.PictureBox btnMaximizar;
        private System.Windows.Forms.PictureBox pictureBox1;
        private System.Windows.Forms.PictureBox pictureBox2;
        private System.Windows.Forms.Label titulo;
        private System.Windows.Forms.Panel panel1;
    }
}
