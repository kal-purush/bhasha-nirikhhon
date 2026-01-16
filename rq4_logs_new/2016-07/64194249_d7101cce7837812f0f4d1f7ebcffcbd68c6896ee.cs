namespace AutoHome2
{
    partial class Form1
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
            this.components = new System.ComponentModel.Container();
            this.statusStrip1 = new System.Windows.Forms.StatusStrip();
            this.statusConexao = new System.Windows.Forms.ToolStripStatusLabel();
            this.statusReceiver = new System.Windows.Forms.ToolStripStatusLabel();
            this.lblHora = new System.Windows.Forms.ToolStripStatusLabel();
            this.lblEndIp = new System.Windows.Forms.ToolStripStatusLabel();
            this.serialPort1 = new System.IO.Ports.SerialPort(this.components);
            this.timerPhp = new System.Windows.Forms.Timer(this.components);
            this.menuStrip1 = new System.Windows.Forms.MenuStrip();
            this.arquivoToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.conectarToolStripMenuItem1 = new System.Windows.Forms.ToolStripMenuItem();
            this.telaCheiaToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.sairToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.ferramentasToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.portaToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.dispositivosToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.consumoToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.cadastrarToolStripMenuItem2 = new System.Windows.Forms.ToolStripMenuItem();
            this.gerenciarToolStripMenuItem1 = new System.Windows.Forms.ToolStripMenuItem();
            this.toolStripMenuItem1 = new System.Windows.Forms.ToolStripMenuItem();
            this.ajudaToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.pictureBox1 = new System.Windows.Forms.PictureBox();
            this.btnEntrada = new System.Windows.Forms.Button();
            this.btnGaragem = new System.Windows.Forms.Button();
            this.btnRol = new System.Windows.Forms.Button();
            this.btnPlantas = new System.Windows.Forms.Button();
            this.btnChurrasqueira = new System.Windows.Forms.Button();
            this.btnLuzExterna = new System.Windows.Forms.Button();
            this.btnArandelasPisc = new System.Windows.Forms.Button();
            this.btnPisc = new System.Windows.Forms.Button();
            this.btnArandelaTorre = new System.Windows.Forms.Button();
            this.btnArandelasGaragem = new System.Windows.Forms.Button();
            this.pictureBox3 = new System.Windows.Forms.PictureBox();
            this.button1 = new System.Windows.Forms.Button();
            this.pictureBox2 = new System.Windows.Forms.PictureBox();
            this.button2 = new System.Windows.Forms.Button();
            this.groupBox1 = new System.Windows.Forms.GroupBox();
            this.btnFimDeSemana = new System.Windows.Forms.Button();
            this.btnAmanha = new System.Windows.Forms.Button();
            this.groupBox2 = new System.Windows.Forms.GroupBox();
            this.label2 = new System.Windows.Forms.Label();
            this.trackBarAudioPlayer = new System.Windows.Forms.TrackBar();
            this.progressBarAudioLevel = new System.Windows.Forms.ProgressBar();
            this.label1 = new System.Windows.Forms.Label();
            this.timerHora = new System.Windows.Forms.Timer(this.components);
            this.listBoxClientes = new System.Windows.Forms.ListBox();
            this.groupBox3 = new System.Windows.Forms.GroupBox();
            this.statusStrip1.SuspendLayout();
            this.menuStrip1.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)(this.pictureBox1)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.pictureBox3)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.pictureBox2)).BeginInit();
            this.groupBox1.SuspendLayout();
            this.groupBox2.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)(this.trackBarAudioPlayer)).BeginInit();
            this.groupBox3.SuspendLayout();
            this.SuspendLayout();
            // 
            // statusStrip1
            // 
            this.statusStrip1.BackColor = System.Drawing.SystemColors.Control;
            this.statusStrip1.Items.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.statusConexao,
            this.statusReceiver,
            this.lblHora,
            this.lblEndIp});
            this.statusStrip1.Location = new System.Drawing.Point(0, 672);
            this.statusStrip1.Name = "statusStrip1";
            this.statusStrip1.Size = new System.Drawing.Size(1016, 24);
            this.statusStrip1.TabIndex = 8;
            this.statusStrip1.Text = "statusStrip1";
            // 
            // statusConexao
            // 
            this.statusConexao.Image = global::AutoHome2.Properties.Resources.imgDisconnected;
            this.statusConexao.LinkBehavior = System.Windows.Forms.LinkBehavior.NeverUnderline;
            this.statusConexao.Name = "statusConexao";
            this.statusConexao.Padding = new System.Windows.Forms.Padding(10, 0, 30, 0);
            this.statusConexao.Size = new System.Drawing.Size(176, 19);
            this.statusConexao.Text = "Status: Desconectado";
            this.statusConexao.Click += new System.EventHandler(this.statusConexao_Click);
            // 
            // statusReceiver
            // 
            this.statusReceiver.BorderSides = ((System.Windows.Forms.ToolStripStatusLabelBorderSides)((System.Windows.Forms.ToolStripStatusLabelBorderSides.Left | System.Windows.Forms.ToolStripStatusLabelBorderSides.Right)));
            this.statusReceiver.Name = "statusReceiver";
            this.statusReceiver.Padding = new System.Windows.Forms.Padding(30, 0, 30, 0);
            this.statusReceiver.Size = new System.Drawing.Size(241, 19);
            this.statusReceiver.Text = "Último Comando Enviado: Vazio";
            // 
            // lblHora
            // 
            this.lblHora.BorderSides = System.Windows.Forms.ToolStripStatusLabelBorderSides.Right;
            this.lblHora.Name = "lblHora";
            this.lblHora.Padding = new System.Windows.Forms.Padding(30, 0, 30, 0);
            this.lblHora.Size = new System.Drawing.Size(97, 19);
            this.lblHora.Text = "Hora";
            this.lblHora.Click += new System.EventHandler(this.lblHora_Click);
            // 
            // lblEndIp
            // 
            this.lblEndIp.Name = "lblEndIp";
            this.lblEndIp.Size = new System.Drawing.Size(72, 19);
            this.lblEndIp.Text = "Endereço IP:";
            this.lblEndIp.Click += new System.EventHandler(this.toolStripStatusLabel1_Click);
            // 
            // menuStrip1
            // 
            this.menuStrip1.BackColor = System.Drawing.SystemColors.ControlDark;
            this.menuStrip1.Items.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.arquivoToolStripMenuItem,
            this.ferramentasToolStripMenuItem,
            this.dispositivosToolStripMenuItem,
            this.ajudaToolStripMenuItem});
            this.menuStrip1.Location = new System.Drawing.Point(0, 0);
            this.menuStrip1.Name = "menuStrip1";
            this.menuStrip1.Size = new System.Drawing.Size(1016, 24);
            this.menuStrip1.TabIndex = 11;
            this.menuStrip1.Text = "menuStrip1";
            // 
            // arquivoToolStripMenuItem
            // 
            this.arquivoToolStripMenuItem.DropDownItems.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.conectarToolStripMenuItem1,
            this.telaCheiaToolStripMenuItem,
            this.sairToolStripMenuItem});
            this.arquivoToolStripMenuItem.Name = "arquivoToolStripMenuItem";
            this.arquivoToolStripMenuItem.Size = new System.Drawing.Size(61, 20);
            this.arquivoToolStripMenuItem.Text = "Arquivo";
            // 
            // conectarToolStripMenuItem1
            // 
            this.conectarToolStripMenuItem1.Name = "conectarToolStripMenuItem1";
            this.conectarToolStripMenuItem1.Size = new System.Drawing.Size(128, 22);
            this.conectarToolStripMenuItem1.Text = "Conectar";
            this.conectarToolStripMenuItem1.Click += new System.EventHandler(this.conectarToolStripMenuItem1_Click);
            // 
            // telaCheiaToolStripMenuItem
            // 
            this.telaCheiaToolStripMenuItem.Name = "telaCheiaToolStripMenuItem";
            this.telaCheiaToolStripMenuItem.Size = new System.Drawing.Size(128, 22);
            this.telaCheiaToolStripMenuItem.Text = "Tela Cheia";
            this.telaCheiaToolStripMenuItem.Click += new System.EventHandler(this.telaCheiaToolStripMenuItem_Click);
            // 
            // sairToolStripMenuItem
            // 
            this.sairToolStripMenuItem.Name = "sairToolStripMenuItem";
            this.sairToolStripMenuItem.Size = new System.Drawing.Size(128, 22);
            this.sairToolStripMenuItem.Text = "Sair";
            this.sairToolStripMenuItem.Click += new System.EventHandler(this.sairToolStripMenuItem_Click);
            // 
            // ferramentasToolStripMenuItem
            // 
            this.ferramentasToolStripMenuItem.DropDownItems.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.portaToolStripMenuItem});
            this.ferramentasToolStripMenuItem.Name = "ferramentasToolStripMenuItem";
            this.ferramentasToolStripMenuItem.Size = new System.Drawing.Size(84, 20);
            this.ferramentasToolStripMenuItem.Text = "Ferramentas";
            // 
            // portaToolStripMenuItem
            // 
            this.portaToolStripMenuItem.Name = "portaToolStripMenuItem";
            this.portaToolStripMenuItem.Size = new System.Drawing.Size(102, 22);
            this.portaToolStripMenuItem.Text = "Porta";
            // 
            // dispositivosToolStripMenuItem
            // 
            this.dispositivosToolStripMenuItem.DropDownItems.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.consumoToolStripMenuItem,
            this.cadastrarToolStripMenuItem2,
            this.gerenciarToolStripMenuItem1,
            this.toolStripMenuItem1});
            this.dispositivosToolStripMenuItem.Name = "dispositivosToolStripMenuItem";
            this.dispositivosToolStripMenuItem.Size = new System.Drawing.Size(82, 20);
            this.dispositivosToolStripMenuItem.Text = "Dispositivos";
            // 
            // consumoToolStripMenuItem
            // 
            this.consumoToolStripMenuItem.Name = "consumoToolStripMenuItem";
            this.consumoToolStripMenuItem.Size = new System.Drawing.Size(182, 22);
            this.consumoToolStripMenuItem.Text = "Relatório de Acessos";
            this.consumoToolStripMenuItem.Click += new System.EventHandler(this.consumoToolStripMenuItem_Click);
            // 
            // cadastrarToolStripMenuItem2
            // 
            this.cadastrarToolStripMenuItem2.Name = "cadastrarToolStripMenuItem2";
            this.cadastrarToolStripMenuItem2.Size = new System.Drawing.Size(182, 22);
            this.cadastrarToolStripMenuItem2.Text = "Cadastrar";
            this.cadastrarToolStripMenuItem2.Click += new System.EventHandler(this.cadastrarToolStripMenuItem2_Click);
            // 
            // gerenciarToolStripMenuItem1
            // 
            this.gerenciarToolStripMenuItem1.Name = "gerenciarToolStripMenuItem1";
            this.gerenciarToolStripMenuItem1.Size = new System.Drawing.Size(182, 22);
            this.gerenciarToolStripMenuItem1.Text = "Gerenciar";
            this.gerenciarToolStripMenuItem1.Click += new System.EventHandler(this.gerenciarToolStripMenuItem1_Click);
            // 
            // toolStripMenuItem1
            // 
            this.toolStripMenuItem1.Name = "toolStripMenuItem1";
            this.toolStripMenuItem1.Size = new System.Drawing.Size(182, 22);
            this.toolStripMenuItem1.Text = "Agendamento";
            this.toolStripMenuItem1.Click += new System.EventHandler(this.toolStripMenuItem1_Click);
            // 
            // ajudaToolStripMenuItem
            // 
            this.ajudaToolStripMenuItem.Name = "ajudaToolStripMenuItem";
            this.ajudaToolStripMenuItem.Size = new System.Drawing.Size(50, 20);
            this.ajudaToolStripMenuItem.Text = "Ajuda";
            // 
            // pictureBox1
            // 
            this.pictureBox1.BackColor = System.Drawing.Color.Transparent;
            this.pictureBox1.Image = global::AutoHome2.Properties.Resources.vistaSuperiorT;
            this.pictureBox1.Location = new System.Drawing.Point(12, 41);
            this.pictureBox1.Name = "pictureBox1";
            this.pictureBox1.Size = new System.Drawing.Size(481, 315);
            this.pictureBox1.SizeMode = System.Windows.Forms.PictureBoxSizeMode.StretchImage;
            this.pictureBox1.TabIndex = 13;
            this.pictureBox1.TabStop = false;
            // 
            // btnEntrada
            // 
            this.btnEntrada.Image = global::AutoHome2.Properties.Resources.btnoff;
            this.btnEntrada.Location = new System.Drawing.Point(257, 253);
            this.btnEntrada.Name = "btnEntrada";
            this.btnEntrada.Size = new System.Drawing.Size(32, 27);
            this.btnEntrada.TabIndex = 16;
            this.btnEntrada.UseVisualStyleBackColor = true;
            this.btnEntrada.Click += new System.EventHandler(this.btnEntrada_Click);
            // 
            // btnGaragem
            // 
            this.btnGaragem.BackColor = System.Drawing.Color.Transparent;
            this.btnGaragem.Image = global::AutoHome2.Properties.Resources.btnoff;
            this.btnGaragem.Location = new System.Drawing.Point(383, 252);
            this.btnGaragem.Name = "btnGaragem";
            this.btnGaragem.Size = new System.Drawing.Size(32, 27);
            this.btnGaragem.TabIndex = 17;
            this.btnGaragem.UseVisualStyleBackColor = false;
            this.btnGaragem.Click += new System.EventHandler(this.btnGaragem_Click);
            // 
            // btnRol
            // 
            this.btnRol.Image = global::AutoHome2.Properties.Resources.btnoff;
            this.btnRol.Location = new System.Drawing.Point(237, 74);
            this.btnRol.Name = "btnRol";
            this.btnRol.Size = new System.Drawing.Size(32, 27);
            this.btnRol.TabIndex = 18;
            this.btnRol.UseVisualStyleBackColor = true;
            this.btnRol.Click += new System.EventHandler(this.btnRol_Click);
            // 
            // btnPlantas
            // 
            this.btnPlantas.Image = global::AutoHome2.Properties.Resources.btnoff;
            this.btnPlantas.Location = new System.Drawing.Point(300, 209);
            this.btnPlantas.Name = "btnPlantas";
            this.btnPlantas.Size = new System.Drawing.Size(32, 27);
            this.btnPlantas.TabIndex = 19;
            this.btnPlantas.UseVisualStyleBackColor = true;
            // 
            // btnChurrasqueira
            // 
            this.btnChurrasqueira.Image = global::AutoHome2.Properties.Resources.btnoff;
            this.btnChurrasqueira.Location = new System.Drawing.Point(82, 233);
            this.btnChurrasqueira.Name = "btnChurrasqueira";
            this.btnChurrasqueira.Size = new System.Drawing.Size(32, 27);
            this.btnChurrasqueira.TabIndex = 20;
            this.btnChurrasqueira.UseVisualStyleBackColor = true;
            this.btnChurrasqueira.Click += new System.EventHandler(this.btnChurrasqueira_Click);
            // 
            // btnLuzExterna
            // 
            this.btnLuzExterna.Image = global::AutoHome2.Properties.Resources.btnoff;
            this.btnLuzExterna.Location = new System.Drawing.Point(180, 253);
            this.btnLuzExterna.Name = "btnLuzExterna";
            this.btnLuzExterna.Size = new System.Drawing.Size(32, 27);
            this.btnLuzExterna.TabIndex = 21;
            this.btnLuzExterna.UseVisualStyleBackColor = true;
            this.btnLuzExterna.Click += new System.EventHandler(this.button9_Click);
            // 
            // btnArandelasPisc
            // 
            this.btnArandelasPisc.Image = global::AutoHome2.Properties.Resources.btnoff;
            this.btnArandelasPisc.Location = new System.Drawing.Point(32, 140);
            this.btnArandelasPisc.Name = "btnArandelasPisc";
            this.btnArandelasPisc.Size = new System.Drawing.Size(32, 27);
            this.btnArandelasPisc.TabIndex = 22;
            this.btnArandelasPisc.UseVisualStyleBackColor = true;
            this.btnArandelasPisc.Click += new System.EventHandler(this.btnArandelasPisc_Click);
            // 
            // btnPisc
            // 
            this.btnPisc.Image = global::AutoHome2.Properties.Resources.btnoff;
            this.btnPisc.Location = new System.Drawing.Point(124, 174);
            this.btnPisc.Name = "btnPisc";
            this.btnPisc.Size = new System.Drawing.Size(32, 27);
            this.btnPisc.TabIndex = 23;
            this.btnPisc.UseVisualStyleBackColor = true;
            this.btnPisc.Click += new System.EventHandler(this.btnPisc_Click);
            // 
            // btnArandelaTorre
            // 
            this.btnArandelaTorre.Image = global::AutoHome2.Properties.Resources.btnoff;
            this.btnArandelaTorre.Location = new System.Drawing.Point(295, 127);
            this.btnArandelaTorre.Name = "btnArandelaTorre";
            this.btnArandelaTorre.Size = new System.Drawing.Size(32, 27);
            this.btnArandelaTorre.TabIndex = 25;
            this.btnArandelaTorre.UseVisualStyleBackColor = true;
            // 
            // btnArandelasGaragem
            // 
            this.btnArandelasGaragem.Image = global::AutoHome2.Properties.Resources.btnoff;
            this.btnArandelasGaragem.Location = new System.Drawing.Point(445, 164);
            this.btnArandelasGaragem.Name = "btnArandelasGaragem";
            this.btnArandelasGaragem.Size = new System.Drawing.Size(32, 27);
            this.btnArandelasGaragem.TabIndex = 26;
            this.btnArandelasGaragem.UseVisualStyleBackColor = true;
            // 
            // pictureBox3
            // 
            this.pictureBox3.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom) 
            | System.Windows.Forms.AnchorStyles.Left)));
            this.pictureBox3.BackColor = System.Drawing.Color.Transparent;
            this.pictureBox3.Image = global::AutoHome2.Properties.Resources.vistaGaragemT;
            this.pictureBox3.Location = new System.Drawing.Point(12, 369);
            this.pictureBox3.Name = "pictureBox3";
            this.pictureBox3.Size = new System.Drawing.Size(481, 280);
            this.pictureBox3.SizeMode = System.Windows.Forms.PictureBoxSizeMode.StretchImage;
            this.pictureBox3.TabIndex = 28;
            this.pictureBox3.TabStop = false;
            // 
            // button1
            // 
            this.button1.Image = global::AutoHome2.Properties.Resources.btnoff;
            this.button1.Location = new System.Drawing.Point(329, 437);
            this.button1.Name = "button1";
            this.button1.Size = new System.Drawing.Size(32, 27);
            this.button1.TabIndex = 29;
            this.button1.UseVisualStyleBackColor = true;
            this.button1.Click += new System.EventHandler(this.button1_Click_1);
            // 
            // pictureBox2
            // 
            this.pictureBox2.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.pictureBox2.BackColor = System.Drawing.Color.Transparent;
            this.pictureBox2.Image = global::AutoHome2.Properties.Resources.painel12;
            this.pictureBox2.Location = new System.Drawing.Point(529, 41);
            this.pictureBox2.Name = "pictureBox2";
            this.pictureBox2.Size = new System.Drawing.Size(475, 608);
            this.pictureBox2.SizeMode = System.Windows.Forms.PictureBoxSizeMode.StretchImage;
            this.pictureBox2.TabIndex = 30;
            this.pictureBox2.TabStop = false;
            // 
            // button2
            // 
            this.button2.ForeColor = System.Drawing.SystemColors.ActiveCaptionText;
            this.button2.Location = new System.Drawing.Point(162, 19);
            this.button2.Name = "button2";
            this.button2.Size = new System.Drawing.Size(122, 23);
            this.button2.TabIndex = 31;
            this.button2.Text = "Hoje";
            this.button2.UseVisualStyleBackColor = true;
            this.button2.Click += new System.EventHandler(this.button2_Click_1);
            // 
            // groupBox1
            // 
            this.groupBox1.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.groupBox1.BackColor = System.Drawing.Color.Transparent;
            this.groupBox1.Controls.Add(this.btnFimDeSemana);
            this.groupBox1.Controls.Add(this.btnAmanha);
            this.groupBox1.Controls.Add(this.button2);
            this.groupBox1.ForeColor = System.Drawing.SystemColors.ButtonHighlight;
            this.groupBox1.Location = new System.Drawing.Point(550, 380);
            this.groupBox1.Name = "groupBox1";
            this.groupBox1.Size = new System.Drawing.Size(438, 54);
            this.groupBox1.TabIndex = 32;
            this.groupBox1.TabStop = false;
            this.groupBox1.Text = "Previsão do Tempo";
            // 
            // btnFimDeSemana
            // 
            this.btnFimDeSemana.ForeColor = System.Drawing.SystemColors.ActiveCaptionText;
            this.btnFimDeSemana.Location = new System.Drawing.Point(19, 19);
            this.btnFimDeSemana.Name = "btnFimDeSemana";
            this.btnFimDeSemana.Size = new System.Drawing.Size(122, 23);
            this.btnFimDeSemana.TabIndex = 33;
            this.btnFimDeSemana.Text = "Fim de Semana";
            this.btnFimDeSemana.UseVisualStyleBackColor = true;
            this.btnFimDeSemana.Click += new System.EventHandler(this.btnFimDeSemana_Click);
            // 
            // btnAmanha
            // 
            this.btnAmanha.ForeColor = System.Drawing.SystemColors.ActiveCaptionText;
            this.btnAmanha.Location = new System.Drawing.Point(299, 19);
            this.btnAmanha.Name = "btnAmanha";
            this.btnAmanha.Size = new System.Drawing.Size(122, 23);
            this.btnAmanha.TabIndex = 32;
            this.btnAmanha.Text = "Amanhã";
            this.btnAmanha.UseVisualStyleBackColor = true;
            this.btnAmanha.Click += new System.EventHandler(this.btnAmanha_Click);
            // 
            // groupBox2
            // 
            this.groupBox2.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Right)));
            this.groupBox2.BackColor = System.Drawing.Color.Transparent;
            this.groupBox2.Controls.Add(this.label2);
            this.groupBox2.Controls.Add(this.trackBarAudioPlayer);
            this.groupBox2.ForeColor = System.Drawing.SystemColors.ButtonHighlight;
            this.groupBox2.Location = new System.Drawing.Point(550, 242);
            this.groupBox2.Name = "groupBox2";
            this.groupBox2.Size = new System.Drawing.Size(438, 114);
            this.groupBox2.TabIndex = 34;
            this.groupBox2.TabStop = false;
            this.groupBox2.Text = "Player";
            // 
            // label2
            // 
            this.label2.AutoSize = true;
            this.label2.BackColor = System.Drawing.Color.Transparent;
            this.label2.ForeColor = System.Drawing.SystemColors.Control;
            this.label2.Location = new System.Drawing.Point(18, 26);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(45, 13);
            this.label2.TabIndex = 37;
            this.label2.Text = "Volume:";
            // 
            // trackBarAudioPlayer
            // 
            this.trackBarAudioPlayer.BackColor = System.Drawing.SystemColors.ControlLightLight;
            this.trackBarAudioPlayer.Location = new System.Drawing.Point(19, 42);
            this.trackBarAudioPlayer.Maximum = 100;
            this.trackBarAudioPlayer.Name = "trackBarAudioPlayer";
            this.trackBarAudioPlayer.Size = new System.Drawing.Size(402, 45);
            this.trackBarAudioPlayer.TabIndex = 37;
            this.trackBarAudioPlayer.TickStyle = System.Windows.Forms.TickStyle.None;
            this.trackBarAudioPlayer.Value = 50;
            this.trackBarAudioPlayer.Scroll += new System.EventHandler(this.trackBarAudioPlayer_Scroll);
            this.trackBarAudioPlayer.MouseUp += new System.Windows.Forms.MouseEventHandler(this.trackBarAudioPlayer_MouseUp);
            // 
            // progressBarAudioLevel
            // 
            this.progressBarAudioLevel.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Right)));
            this.progressBarAudioLevel.Location = new System.Drawing.Point(550, 74);
            this.progressBarAudioLevel.Name = "progressBarAudioLevel";
            this.progressBarAudioLevel.Size = new System.Drawing.Size(438, 14);
            this.progressBarAudioLevel.TabIndex = 35;
            // 
            // label1
            // 
            this.label1.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Right)));
            this.label1.AutoSize = true;
            this.label1.BackColor = System.Drawing.Color.Transparent;
            this.label1.Font = new System.Drawing.Font("Microsoft Sans Serif", 9.75F, System.Drawing.FontStyle.Italic, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label1.ForeColor = System.Drawing.SystemColors.Control;
            this.label1.Location = new System.Drawing.Point(550, 55);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(70, 16);
            this.label1.TabIndex = 36;
            this.label1.Text = "Microfone:";
            // 
            // timerHora
            // 
            this.timerHora.Enabled = true;
            this.timerHora.Interval = 1000;
            this.timerHora.Tick += new System.EventHandler(this.timerHora_Tick);
            // 
            // listBoxClientes
            // 
            this.listBoxClientes.FormattingEnabled = true;
            this.listBoxClientes.Location = new System.Drawing.Point(30, 24);
            this.listBoxClientes.Name = "listBoxClientes";
            this.listBoxClientes.Size = new System.Drawing.Size(378, 69);
            this.listBoxClientes.TabIndex = 37;
            // 
            // groupBox3
            // 
            this.groupBox3.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Right)));
            this.groupBox3.BackColor = System.Drawing.Color.Transparent;
            this.groupBox3.Controls.Add(this.listBoxClientes);
            this.groupBox3.ForeColor = System.Drawing.SystemColors.ButtonHighlight;
            this.groupBox3.Location = new System.Drawing.Point(550, 108);
            this.groupBox3.Name = "groupBox3";
            this.groupBox3.Size = new System.Drawing.Size(438, 114);
            this.groupBox3.TabIndex = 39;
            this.groupBox3.TabStop = false;
            this.groupBox3.Text = "Clientes conectados:";
            // 
            // Form1
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.BackgroundImage = global::AutoHome2.Properties.Resources.black;
            this.BackgroundImageLayout = System.Windows.Forms.ImageLayout.Stretch;
            this.ClientSize = new System.Drawing.Size(1016, 696);
            this.Controls.Add(this.groupBox3);
            this.Controls.Add(this.label1);
            this.Controls.Add(this.progressBarAudioLevel);
            this.Controls.Add(this.groupBox2);
            this.Controls.Add(this.groupBox1);
            this.Controls.Add(this.pictureBox2);
            this.Controls.Add(this.button1);
            this.Controls.Add(this.pictureBox3);
            this.Controls.Add(this.btnArandelasGaragem);
            this.Controls.Add(this.btnArandelaTorre);
            this.Controls.Add(this.btnPisc);
            this.Controls.Add(this.btnArandelasPisc);
            this.Controls.Add(this.btnLuzExterna);
            this.Controls.Add(this.btnChurrasqueira);
            this.Controls.Add(this.btnPlantas);
            this.Controls.Add(this.btnRol);
            this.Controls.Add(this.btnGaragem);
            this.Controls.Add(this.btnEntrada);
            this.Controls.Add(this.pictureBox1);
            this.Controls.Add(this.statusStrip1);
            this.Controls.Add(this.menuStrip1);
            this.FormBorderStyle = System.Windows.Forms.FormBorderStyle.Fixed3D;
            this.MainMenuStrip = this.menuStrip1;
            this.Name = "Form1";
            this.StartPosition = System.Windows.Forms.FormStartPosition.CenterScreen;
            this.Text = "AutoHome";
            this.WindowState = System.Windows.Forms.FormWindowState.Maximized;
            this.Load += new System.EventHandler(this.Form1_Load);
            this.statusStrip1.ResumeLayout(false);
            this.statusStrip1.PerformLayout();
            this.menuStrip1.ResumeLayout(false);
            this.menuStrip1.PerformLayout();
            ((System.ComponentModel.ISupportInitialize)(this.pictureBox1)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.pictureBox3)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.pictureBox2)).EndInit();
            this.groupBox1.ResumeLayout(false);
            this.groupBox2.ResumeLayout(false);
            this.groupBox2.PerformLayout();
            ((System.ComponentModel.ISupportInitialize)(this.trackBarAudioPlayer)).EndInit();
            this.groupBox3.ResumeLayout(false);
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.StatusStrip statusStrip1;
        private System.Windows.Forms.ToolStripStatusLabel statusConexao;
        private System.Windows.Forms.ToolStripStatusLabel statusReceiver;
        private System.Windows.Forms.ToolStripStatusLabel lblHora;
        private System.IO.Ports.SerialPort serialPort1;
        private System.Windows.Forms.Timer timerPhp;
        private System.Windows.Forms.MenuStrip menuStrip1;
        private System.Windows.Forms.ToolStripMenuItem arquivoToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem conectarToolStripMenuItem1;
        private System.Windows.Forms.ToolStripMenuItem ferramentasToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem portaToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem dispositivosToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem consumoToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem ajudaToolStripMenuItem;
        private System.Windows.Forms.PictureBox pictureBox1;
        private System.Windows.Forms.ToolStripMenuItem cadastrarToolStripMenuItem2;
        private System.Windows.Forms.ToolStripMenuItem gerenciarToolStripMenuItem1;
        private System.Windows.Forms.Button btnEntrada;
        private System.Windows.Forms.Button btnGaragem;
        private System.Windows.Forms.Button btnRol;
        private System.Windows.Forms.Button btnPlantas;
        private System.Windows.Forms.Button btnChurrasqueira;
        private System.Windows.Forms.Button btnLuzExterna;
        private System.Windows.Forms.Button btnArandelasPisc;
        private System.Windows.Forms.Button btnPisc;
        private System.Windows.Forms.Button btnArandelaTorre;
        private System.Windows.Forms.Button btnArandelasGaragem;
        private System.Windows.Forms.PictureBox pictureBox3;
        private System.Windows.Forms.Button button1;
        private System.Windows.Forms.PictureBox pictureBox2;
        private System.Windows.Forms.Button button2;
        private System.Windows.Forms.GroupBox groupBox1;
        private System.Windows.Forms.Button btnFimDeSemana;
        private System.Windows.Forms.Button btnAmanha;
        private System.Windows.Forms.ToolStripMenuItem toolStripMenuItem1;
        private System.Windows.Forms.GroupBox groupBox2;
        private System.Windows.Forms.ProgressBar progressBarAudioLevel;
        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.ToolStripMenuItem sairToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem telaCheiaToolStripMenuItem;
        private System.Windows.Forms.TrackBar trackBarAudioPlayer;
        private System.Windows.Forms.Label label2;
        private System.Windows.Forms.Timer timerHora;
        private System.Windows.Forms.ListBox listBoxClientes;
        private System.Windows.Forms.GroupBox groupBox3;
        private System.Windows.Forms.ToolStripStatusLabel lblEndIp;
    }
}
