using System;
using System.Windows.Forms;
using ComboCounter.Classes;

namespace ComboCounter.Forms
{
    class HomeScreen : UserControl
    {
        private CustomControls.OutlineButton comboCounterDefault;
        private H3 h31;
        private H3 h32;
        private CustomControls.OutlineButton totalForceDefault;
        private H3 h33;
        private CustomControls.OutlineButton totalTimeDefault;
        private CustomControls.OutlineButton comboCountCustom;
        private CustomControls.OutlineButton compPound;
        private CustomControls.OutlineButton sensorAdj;
        private CustomControls.OutlineButton punchCountDefault;
        private CustomControls.OutlineButton punchCountCustom;
        private CustomControls.OutlineButton totalTimeCustom;
        private CustomControls.OutlineButton totalForce2Play;
        private CustomControls.OutlineButton punchChallengeDefault;
        private CustomControls.OutlineButton punchChallengeCustom;
        private H3 h35;
        private H3 h36;
        private H3 h34;
        private Timer timer;
        private System.ComponentModel.IContainer components;
        private Panel ghostPanel2;
        private Panel clockPanel;
        private SmallInfo clockText;
        private SmallInfo date;
        private Panel ghostPanel;
        private TableLayoutPanel tableLayoutPanel1;

        public HomeScreen()
        {
            InitializeComponent();
            tableLayoutPanel1.Dock = DockStyle.None;
            tableLayoutPanel1.Anchor = AnchorStyles.None;
        }

        private void InitializeComponent()
        {
            this.components = new System.ComponentModel.Container();
            this.tableLayoutPanel1 = new System.Windows.Forms.TableLayoutPanel();
            this.h31 = new ComboCounter.Classes.H3();
            this.h32 = new ComboCounter.Classes.H3();
            this.h33 = new ComboCounter.Classes.H3();
            this.compPound = new ComboCounter.CustomControls.OutlineButton();
            this.sensorAdj = new ComboCounter.CustomControls.OutlineButton();
            this.punchCountDefault = new ComboCounter.CustomControls.OutlineButton();
            this.punchCountCustom = new ComboCounter.CustomControls.OutlineButton();
            this.punchChallengeDefault = new ComboCounter.CustomControls.OutlineButton();
            this.punchChallengeCustom = new ComboCounter.CustomControls.OutlineButton();
            this.h35 = new ComboCounter.Classes.H3();
            this.h36 = new ComboCounter.Classes.H3();
            this.h34 = new ComboCounter.Classes.H3();
            this.comboCountCustom = new ComboCounter.CustomControls.OutlineButton();
            this.totalForce2Play = new ComboCounter.CustomControls.OutlineButton();
            this.totalTimeCustom = new ComboCounter.CustomControls.OutlineButton();
            this.totalTimeDefault = new ComboCounter.CustomControls.OutlineButton();
            this.totalForceDefault = new ComboCounter.CustomControls.OutlineButton();
            this.comboCounterDefault = new ComboCounter.CustomControls.OutlineButton();
            this.ghostPanel2 = new System.Windows.Forms.Panel();
            this.clockPanel = new System.Windows.Forms.Panel();
            this.date = new ComboCounter.Classes.SmallInfo();
            this.clockText = new ComboCounter.Classes.SmallInfo();
            this.timer = new System.Windows.Forms.Timer(this.components);
            this.ghostPanel = new System.Windows.Forms.Panel();
            this.tableLayoutPanel1.SuspendLayout();
            this.clockPanel.SuspendLayout();
            this.SuspendLayout();
            // 
            // tableLayoutPanel1
            // 
            this.tableLayoutPanel1.ColumnCount = 3;
            this.tableLayoutPanel1.ColumnStyles.Add(new System.Windows.Forms.ColumnStyle(System.Windows.Forms.SizeType.Percent, 33.31247F));
            this.tableLayoutPanel1.ColumnStyles.Add(new System.Windows.Forms.ColumnStyle(System.Windows.Forms.SizeType.Percent, 33.21601F));
            this.tableLayoutPanel1.ColumnStyles.Add(new System.Windows.Forms.ColumnStyle(System.Windows.Forms.SizeType.Percent, 33.47152F));
            this.tableLayoutPanel1.Controls.Add(this.h31, 0, 1);
            this.tableLayoutPanel1.Controls.Add(this.h32, 1, 1);
            this.tableLayoutPanel1.Controls.Add(this.h33, 2, 1);
            this.tableLayoutPanel1.Controls.Add(this.compPound, 0, 5);
            this.tableLayoutPanel1.Controls.Add(this.sensorAdj, 0, 6);
            this.tableLayoutPanel1.Controls.Add(this.punchCountDefault, 1, 5);
            this.tableLayoutPanel1.Controls.Add(this.punchCountCustom, 1, 6);
            this.tableLayoutPanel1.Controls.Add(this.punchChallengeDefault, 2, 5);
            this.tableLayoutPanel1.Controls.Add(this.punchChallengeCustom, 2, 6);
            this.tableLayoutPanel1.Controls.Add(this.h35, 1, 4);
            this.tableLayoutPanel1.Controls.Add(this.h36, 2, 4);
            this.tableLayoutPanel1.Controls.Add(this.h34, 0, 4);
            this.tableLayoutPanel1.Controls.Add(this.comboCountCustom, 0, 3);
            this.tableLayoutPanel1.Controls.Add(this.totalForce2Play, 1, 3);
            this.tableLayoutPanel1.Controls.Add(this.totalTimeCustom, 2, 3);
            this.tableLayoutPanel1.Controls.Add(this.totalTimeDefault, 2, 2);
            this.tableLayoutPanel1.Controls.Add(this.totalForceDefault, 1, 2);
            this.tableLayoutPanel1.Controls.Add(this.comboCounterDefault, 0, 2);
            this.tableLayoutPanel1.Controls.Add(this.ghostPanel, 1, 0);
            this.tableLayoutPanel1.Controls.Add(this.ghostPanel2, 0, 0);
            this.tableLayoutPanel1.Controls.Add(this.clockPanel, 2, 0);
            this.tableLayoutPanel1.Dock = System.Windows.Forms.DockStyle.Fill;
            this.tableLayoutPanel1.Location = new System.Drawing.Point(0, 0);
            this.tableLayoutPanel1.Margin = new System.Windows.Forms.Padding(40);
            this.tableLayoutPanel1.Name = "tableLayoutPanel1";
            this.tableLayoutPanel1.RowCount = 7;
            this.tableLayoutPanel1.RowStyles.Add(new System.Windows.Forms.RowStyle(System.Windows.Forms.SizeType.Percent, 14.81056F));
            this.tableLayoutPanel1.RowStyles.Add(new System.Windows.Forms.RowStyle(System.Windows.Forms.SizeType.Percent, 10.44776F));
            this.tableLayoutPanel1.RowStyles.Add(new System.Windows.Forms.RowStyle(System.Windows.Forms.SizeType.Percent, 17.12615F));
            this.tableLayoutPanel1.RowStyles.Add(new System.Windows.Forms.RowStyle(System.Windows.Forms.SizeType.Percent, 12.00919F));
            this.tableLayoutPanel1.RowStyles.Add(new System.Windows.Forms.RowStyle(System.Windows.Forms.SizeType.Percent, 15.15943F));
            this.tableLayoutPanel1.RowStyles.Add(new System.Windows.Forms.RowStyle(System.Windows.Forms.SizeType.Percent, 15.15943F));
            this.tableLayoutPanel1.RowStyles.Add(new System.Windows.Forms.RowStyle(System.Windows.Forms.SizeType.Percent, 15.15943F));
            this.tableLayoutPanel1.Size = new System.Drawing.Size(1565, 871);
            this.tableLayoutPanel1.TabIndex = 0;
            this.tableLayoutPanel1.Paint += new System.Windows.Forms.PaintEventHandler(this.tableLayoutPanel1_Paint_1);
            // 
            // h31
            // 
            this.h31.AutoSize = true;
            this.h31.Dock = System.Windows.Forms.DockStyle.Fill;
            this.h31.ForeColor = System.Drawing.Color.SteelBlue;
            this.h31.Location = new System.Drawing.Point(3, 129);
            this.h31.Name = "h31";
            this.h31.Size = new System.Drawing.Size(515, 91);
            this.h31.TabIndex = 2;
            this.h31.Text = "Combo Counter";
            this.h31.TextAlign = System.Drawing.ContentAlignment.BottomCenter;
            // 
            // h32
            // 
            this.h32.AutoSize = true;
            this.h32.Dock = System.Windows.Forms.DockStyle.Fill;
            this.h32.ForeColor = System.Drawing.Color.SteelBlue;
            this.h32.ImageAlign = System.Drawing.ContentAlignment.TopRight;
            this.h32.Location = new System.Drawing.Point(524, 129);
            this.h32.Name = "h32";
            this.h32.Size = new System.Drawing.Size(513, 91);
            this.h32.TabIndex = 3;
            this.h32.Text = "Total Force";
            this.h32.TextAlign = System.Drawing.ContentAlignment.BottomCenter;
            this.h32.Click += new System.EventHandler(this.h32_Click);
            // 
            // h33
            // 
            this.h33.AutoSize = true;
            this.h33.Dock = System.Windows.Forms.DockStyle.Fill;
            this.h33.ForeColor = System.Drawing.Color.SteelBlue;
            this.h33.Location = new System.Drawing.Point(1043, 129);
            this.h33.Name = "h33";
            this.h33.Size = new System.Drawing.Size(519, 91);
            this.h33.TabIndex = 5;
            this.h33.Text = "Total Time";
            this.h33.TextAlign = System.Drawing.ContentAlignment.BottomCenter;
            // 
            // compPound
            // 
            this.compPound.Anchor = System.Windows.Forms.AnchorStyles.None;
            this.compPound.AutoSize = true;
            this.compPound.BackColor = System.Drawing.Color.Transparent;
            this.compPound.FlatAppearance.BorderColor = System.Drawing.Color.SteelBlue;
            this.compPound.FlatAppearance.BorderSize = 2;
            this.compPound.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.compPound.ForeColor = System.Drawing.Color.White;
            this.compPound.Location = new System.Drawing.Point(64, 638);
            this.compPound.Name = "compPound";
            this.compPound.Padding = new System.Windows.Forms.Padding(10, 5, 10, 5);
            this.compPound.Size = new System.Drawing.Size(393, 65);
            this.compPound.TabIndex = 8;
            this.compPound.Text = "POUND-FOR-POUND";
            this.compPound.UseVisualStyleBackColor = false;
            this.compPound.Click += new System.EventHandler(this.compPound_Click);
            // 
            // sensorAdj
            // 
            this.sensorAdj.Anchor = System.Windows.Forms.AnchorStyles.None;
            this.sensorAdj.AutoSize = true;
            this.sensorAdj.BackColor = System.Drawing.Color.Transparent;
            this.sensorAdj.FlatAppearance.BorderColor = System.Drawing.Color.SteelBlue;
            this.sensorAdj.FlatAppearance.BorderSize = 2;
            this.sensorAdj.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.sensorAdj.ForeColor = System.Drawing.Color.White;
            this.sensorAdj.Location = new System.Drawing.Point(42, 771);
            this.sensorAdj.Name = "sensorAdj";
            this.sensorAdj.Padding = new System.Windows.Forms.Padding(10, 5, 10, 5);
            this.sensorAdj.Size = new System.Drawing.Size(436, 65);
            this.sensorAdj.TabIndex = 9;
            this.sensorAdj.Text = "SENSOR ADJUSTMENT";
            this.sensorAdj.UseVisualStyleBackColor = false;
            this.sensorAdj.Click += new System.EventHandler(this.sensorAdj_Click);
            // 
            // punchCountDefault
            // 
            this.punchCountDefault.Anchor = System.Windows.Forms.AnchorStyles.None;
            this.punchCountDefault.AutoSize = true;
            this.punchCountDefault.BackColor = System.Drawing.Color.Transparent;
            this.punchCountDefault.FlatAppearance.BorderColor = System.Drawing.Color.SteelBlue;
            this.punchCountDefault.FlatAppearance.BorderSize = 2;
            this.punchCountDefault.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.punchCountDefault.ForeColor = System.Drawing.Color.White;
            this.punchCountDefault.Location = new System.Drawing.Point(676, 638);
            this.punchCountDefault.Name = "punchCountDefault";
            this.punchCountDefault.Padding = new System.Windows.Forms.Padding(10, 5, 10, 5);
            this.punchCountDefault.Size = new System.Drawing.Size(208, 65);
            this.punchCountDefault.TabIndex = 10;
            this.punchCountDefault.Text = "Default";
            this.punchCountDefault.UseVisualStyleBackColor = false;
            this.punchCountDefault.Click += new System.EventHandler(this.punchCountDefault_Click);
            // 
            // punchCountCustom
            // 
            this.punchCountCustom.Anchor = System.Windows.Forms.AnchorStyles.None;
            this.punchCountCustom.AutoSize = true;
            this.punchCountCustom.BackColor = System.Drawing.Color.Transparent;
            this.punchCountCustom.FlatAppearance.BorderColor = System.Drawing.Color.SteelBlue;
            this.punchCountCustom.FlatAppearance.BorderSize = 2;
            this.punchCountCustom.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.punchCountCustom.ForeColor = System.Drawing.Color.White;
            this.punchCountCustom.Location = new System.Drawing.Point(681, 771);
            this.punchCountCustom.Name = "punchCountCustom";
            this.punchCountCustom.Padding = new System.Windows.Forms.Padding(10, 5, 10, 5);
            this.punchCountCustom.Size = new System.Drawing.Size(199, 65);
            this.punchCountCustom.TabIndex = 11;
            this.punchCountCustom.Text = "2 Players";
            this.punchCountCustom.UseVisualStyleBackColor = false;
            this.punchCountCustom.Click += new System.EventHandler(this.punchCountCustom_Click);
            // 
            // punchChallengeDefault
            // 
            this.punchChallengeDefault.Anchor = System.Windows.Forms.AnchorStyles.None;
            this.punchChallengeDefault.AutoSize = true;
            this.punchChallengeDefault.BackColor = System.Drawing.Color.Transparent;
            this.punchChallengeDefault.FlatAppearance.BorderColor = System.Drawing.Color.SteelBlue;
            this.punchChallengeDefault.FlatAppearance.BorderSize = 2;
            this.punchChallengeDefault.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.punchChallengeDefault.ForeColor = System.Drawing.Color.White;
            this.punchChallengeDefault.Location = new System.Drawing.Point(1198, 638);
            this.punchChallengeDefault.Name = "punchChallengeDefault";
            this.punchChallengeDefault.Padding = new System.Windows.Forms.Padding(10, 5, 10, 5);
            this.punchChallengeDefault.Size = new System.Drawing.Size(208, 65);
            this.punchChallengeDefault.TabIndex = 15;
            this.punchChallengeDefault.Text = "Default";
            this.punchChallengeDefault.UseVisualStyleBackColor = false;
            this.punchChallengeDefault.Click += new System.EventHandler(this.punchChallengeDefault_Click);
            // 
            // punchChallengeCustom
            // 
            this.punchChallengeCustom.Anchor = System.Windows.Forms.AnchorStyles.None;
            this.punchChallengeCustom.AutoSize = true;
            this.punchChallengeCustom.BackColor = System.Drawing.Color.Transparent;
            this.punchChallengeCustom.FlatAppearance.BorderColor = System.Drawing.Color.SteelBlue;
            this.punchChallengeCustom.FlatAppearance.BorderSize = 2;
            this.punchChallengeCustom.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.punchChallengeCustom.ForeColor = System.Drawing.Color.White;
            this.punchChallengeCustom.Location = new System.Drawing.Point(1203, 771);
            this.punchChallengeCustom.Name = "punchChallengeCustom";
            this.punchChallengeCustom.Padding = new System.Windows.Forms.Padding(10, 5, 10, 5);
            this.punchChallengeCustom.Size = new System.Drawing.Size(199, 65);
            this.punchChallengeCustom.TabIndex = 16;
            this.punchChallengeCustom.Text = "2 Players";
            this.punchChallengeCustom.UseVisualStyleBackColor = false;
            this.punchChallengeCustom.Click += new System.EventHandler(this.punchChallengeCustom_Click);
            // 
            // h35
            // 
            this.h35.AutoSize = true;
            this.h35.Dock = System.Windows.Forms.DockStyle.Fill;
            this.h35.ForeColor = System.Drawing.Color.SteelBlue;
            this.h35.Location = new System.Drawing.Point(524, 473);
            this.h35.Name = "h35";
            this.h35.Size = new System.Drawing.Size(513, 132);
            this.h35.TabIndex = 18;
            this.h35.Text = "Punch Count";
            this.h35.TextAlign = System.Drawing.ContentAlignment.BottomCenter;
            // 
            // h36
            // 
            this.h36.AutoSize = true;
            this.h36.Dock = System.Windows.Forms.DockStyle.Fill;
            this.h36.ForeColor = System.Drawing.Color.SteelBlue;
            this.h36.Location = new System.Drawing.Point(1043, 473);
            this.h36.Name = "h36";
            this.h36.Size = new System.Drawing.Size(519, 132);
            this.h36.TabIndex = 19;
            this.h36.Text = "Punch Challenge";
            this.h36.TextAlign = System.Drawing.ContentAlignment.BottomCenter;
            // 
            // h34
            // 
            this.h34.AutoSize = true;
            this.h34.Dock = System.Windows.Forms.DockStyle.Fill;
            this.h34.ForeColor = System.Drawing.Color.SteelBlue;
            this.h34.Location = new System.Drawing.Point(3, 473);
            this.h34.Name = "h34";
            this.h34.Size = new System.Drawing.Size(515, 132);
            this.h34.TabIndex = 17;
            this.h34.Text = "Competition";
            this.h34.TextAlign = System.Drawing.ContentAlignment.BottomCenter;
            // 
            // comboCountCustom
            // 
            this.comboCountCustom.Anchor = System.Windows.Forms.AnchorStyles.None;
            this.comboCountCustom.AutoSize = true;
            this.comboCountCustom.BackColor = System.Drawing.Color.Transparent;
            this.comboCountCustom.FlatAppearance.BorderColor = System.Drawing.Color.SteelBlue;
            this.comboCountCustom.FlatAppearance.BorderSize = 2;
            this.comboCountCustom.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.comboCountCustom.ForeColor = System.Drawing.Color.White;
            this.comboCountCustom.Location = new System.Drawing.Point(161, 388);
            this.comboCountCustom.Name = "comboCountCustom";
            this.comboCountCustom.Padding = new System.Windows.Forms.Padding(10, 5, 10, 5);
            this.comboCountCustom.Size = new System.Drawing.Size(199, 65);
            this.comboCountCustom.TabIndex = 7;
            this.comboCountCustom.Text = "Custom";
            this.comboCountCustom.UseVisualStyleBackColor = false;
            this.comboCountCustom.Click += new System.EventHandler(this.comboCountCustom_Click);
            // 
            // totalForce2Play
            // 
            this.totalForce2Play.Anchor = System.Windows.Forms.AnchorStyles.None;
            this.totalForce2Play.AutoSize = true;
            this.totalForce2Play.BackColor = System.Drawing.Color.Transparent;
            this.totalForce2Play.FlatAppearance.BorderColor = System.Drawing.Color.SteelBlue;
            this.totalForce2Play.FlatAppearance.BorderSize = 2;
            this.totalForce2Play.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.totalForce2Play.ForeColor = System.Drawing.Color.White;
            this.totalForce2Play.Location = new System.Drawing.Point(662, 388);
            this.totalForce2Play.Name = "totalForce2Play";
            this.totalForce2Play.Padding = new System.Windows.Forms.Padding(10, 5, 10, 5);
            this.totalForce2Play.Size = new System.Drawing.Size(237, 65);
            this.totalForce2Play.TabIndex = 14;
            this.totalForce2Play.Text = "2 Players";
            this.totalForce2Play.UseVisualStyleBackColor = false;
            this.totalForce2Play.Click += new System.EventHandler(this.totalForce2Play_Click);
            // 
            // totalTimeCustom
            // 
            this.totalTimeCustom.Anchor = System.Windows.Forms.AnchorStyles.None;
            this.totalTimeCustom.AutoSize = true;
            this.totalTimeCustom.BackColor = System.Drawing.Color.Transparent;
            this.totalTimeCustom.FlatAppearance.BorderColor = System.Drawing.Color.SteelBlue;
            this.totalTimeCustom.FlatAppearance.BorderSize = 2;
            this.totalTimeCustom.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.totalTimeCustom.ForeColor = System.Drawing.Color.White;
            this.totalTimeCustom.Location = new System.Drawing.Point(1203, 388);
            this.totalTimeCustom.Name = "totalTimeCustom";
            this.totalTimeCustom.Padding = new System.Windows.Forms.Padding(10, 5, 10, 5);
            this.totalTimeCustom.Size = new System.Drawing.Size(199, 65);
            this.totalTimeCustom.TabIndex = 13;
            this.totalTimeCustom.Text = "2 Players";
            this.totalTimeCustom.UseVisualStyleBackColor = false;
            this.totalTimeCustom.Click += new System.EventHandler(this.totalTimeCustom_Click);
            // 
            // totalTimeDefault
            // 
            this.totalTimeDefault.Anchor = System.Windows.Forms.AnchorStyles.None;
            this.totalTimeDefault.AutoSize = true;
            this.totalTimeDefault.BackColor = System.Drawing.Color.Transparent;
            this.totalTimeDefault.FlatAppearance.BorderColor = System.Drawing.Color.SteelBlue;
            this.totalTimeDefault.FlatAppearance.BorderSize = 2;
            this.totalTimeDefault.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.totalTimeDefault.ForeColor = System.Drawing.Color.White;
            this.totalTimeDefault.Location = new System.Drawing.Point(1198, 262);
            this.totalTimeDefault.Name = "totalTimeDefault";
            this.totalTimeDefault.Padding = new System.Windows.Forms.Padding(10, 5, 10, 5);
            this.totalTimeDefault.Size = new System.Drawing.Size(208, 65);
            this.totalTimeDefault.TabIndex = 6;
            this.totalTimeDefault.Text = "Default";
            this.totalTimeDefault.UseVisualStyleBackColor = false;
            this.totalTimeDefault.Click += new System.EventHandler(this.totalTimeDefault_Click);
            // 
            // totalForceDefault
            // 
            this.totalForceDefault.Anchor = System.Windows.Forms.AnchorStyles.None;
            this.totalForceDefault.AutoSize = true;
            this.totalForceDefault.BackColor = System.Drawing.Color.Transparent;
            this.totalForceDefault.FlatAppearance.BorderColor = System.Drawing.Color.SteelBlue;
            this.totalForceDefault.FlatAppearance.BorderSize = 2;
            this.totalForceDefault.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.totalForceDefault.ForeColor = System.Drawing.Color.WhiteSmoke;
            this.totalForceDefault.Location = new System.Drawing.Point(676, 262);
            this.totalForceDefault.Name = "totalForceDefault";
            this.totalForceDefault.Padding = new System.Windows.Forms.Padding(10, 5, 10, 5);
            this.totalForceDefault.Size = new System.Drawing.Size(208, 65);
            this.totalForceDefault.TabIndex = 4;
            this.totalForceDefault.Text = "Default";
            this.totalForceDefault.UseVisualStyleBackColor = false;
            this.totalForceDefault.Click += new System.EventHandler(this.totalForceDefault_Click);
            // 
            // comboCounterDefault
            // 
            this.comboCounterDefault.Anchor = System.Windows.Forms.AnchorStyles.None;
            this.comboCounterDefault.AutoSize = true;
            this.comboCounterDefault.BackColor = System.Drawing.Color.Transparent;
            this.comboCounterDefault.FlatAppearance.BorderColor = System.Drawing.Color.SteelBlue;
            this.comboCounterDefault.FlatAppearance.BorderSize = 2;
            this.comboCounterDefault.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.comboCounterDefault.ForeColor = System.Drawing.Color.White;
            this.comboCounterDefault.Location = new System.Drawing.Point(156, 262);
            this.comboCounterDefault.Name = "comboCounterDefault";
            this.comboCounterDefault.Padding = new System.Windows.Forms.Padding(10, 5, 10, 5);
            this.comboCounterDefault.Size = new System.Drawing.Size(208, 65);
            this.comboCounterDefault.TabIndex = 1;
            this.comboCounterDefault.Text = "Default";
            this.comboCounterDefault.UseVisualStyleBackColor = false;
            this.comboCounterDefault.Click += new System.EventHandler(this.comboCounterDefault_Click);
            // 
            // ghostPanel2
            // 
            this.ghostPanel2.Location = new System.Drawing.Point(3, 3);
            this.ghostPanel2.Name = "ghostPanel2";
            this.ghostPanel2.Size = new System.Drawing.Size(515, 100);
            this.ghostPanel2.TabIndex = 21;
            // 
            // clockPanel
            // 
            this.clockPanel.Controls.Add(this.date);
            this.clockPanel.Controls.Add(this.clockText);
            this.clockPanel.Location = new System.Drawing.Point(1043, 3);
            this.clockPanel.Name = "clockPanel";
            this.clockPanel.Size = new System.Drawing.Size(519, 123);
            this.clockPanel.TabIndex = 22;
            // 
            // date
            // 
            this.date.AutoSize = true;
            this.date.ForeColor = System.Drawing.Color.White;
            this.date.Location = new System.Drawing.Point(146, 68);
            this.date.Name = "date";
            this.date.Size = new System.Drawing.Size(0, 45);
            this.date.TabIndex = 2;
            // 
            // clockText
            // 
            this.clockText.AutoSize = true;
            this.clockText.ForeColor = System.Drawing.Color.White;
            this.clockText.ImageAlign = System.Drawing.ContentAlignment.MiddleRight;
            this.clockText.Location = new System.Drawing.Point(190, 23);
            this.clockText.Name = "clockText";
            this.clockText.Size = new System.Drawing.Size(0, 45);
            this.clockText.TabIndex = 1;
            this.clockText.TextAlign = System.Drawing.ContentAlignment.MiddleRight;
            // 
            // timer
            // 
            this.timer.Enabled = true;
            this.timer.Interval = 1000;
            this.timer.Tick += new System.EventHandler(this.timer_Tick);
            // 
            // ghostPanel
            // 
            this.ghostPanel.Anchor = System.Windows.Forms.AnchorStyles.None;
            this.ghostPanel.Location = new System.Drawing.Point(524, 4);
            this.ghostPanel.Name = "ghostPanel";
            this.ghostPanel.Size = new System.Drawing.Size(513, 120);
            this.ghostPanel.TabIndex = 20;
            // 
            // HomeScreen
            // 
            this.BackColor = System.Drawing.Color.Transparent;
            this.Controls.Add(this.tableLayoutPanel1);
            this.Name = "HomeScreen";
            this.Size = new System.Drawing.Size(1565, 871);
            this.tableLayoutPanel1.ResumeLayout(false);
            this.tableLayoutPanel1.PerformLayout();
            this.clockPanel.ResumeLayout(false);
            this.clockPanel.PerformLayout();
            this.ResumeLayout(false);

        }

        private void tableLayoutPanel1_Paint(object sender, PaintEventArgs e)
        {
            
        }

        public event EventHandler<OptionClickEventArgs> OnOptionClicked;

        private void Option_Clicked(ClassToCall classCall)
        {
            OnOptionClicked?.Invoke(this, new OptionClickEventArgs(classCall));
        }

        private void comboCounterDefault_Click(object sender, EventArgs e)
        {
            Option_Clicked(ClassToCall.ComboCounter);
        }

        private void totalForceDefault_Click(object sender, EventArgs e)
        {
            Option_Clicked(ClassToCall.TotalForce);
        }

        private void comboCountCustom_Click(object sender, EventArgs e)
        {
            Option_Clicked(ClassToCall.ComboCounterCustom);
        }

        private void totalForce2Play_Click(object sender, EventArgs e)
        {
            Option_Clicked(ClassToCall.TotalForce2Play);
        }

        private void totalTimeDefault_Click(object sender, EventArgs e)
        {
            Option_Clicked(ClassToCall.TotalTime);
        }

        private void totalTimeCustom_Click(object sender, EventArgs e)
        {
            Option_Clicked(ClassToCall.TotalTimeCustom);
        }

        private void compPound_Click(object sender, EventArgs e)
        {
            Option_Clicked(ClassToCall.CompetePound);
        }

        private void sensorAdj_Click(object sender, EventArgs e)
        {
            Option_Clicked(ClassToCall.SensorAdj);
        }

        private void punchCountDefault_Click(object sender, EventArgs e)
        {
            Option_Clicked(ClassToCall.PunchCount);
        }

        private void punchCountCustom_Click(object sender, EventArgs e)
        {
            Option_Clicked(ClassToCall.PunchCountCustom);
        }

        private void punchChallengeDefault_Click(object sender, EventArgs e)
        {
            Option_Clicked(ClassToCall.PunchChallenge);
        }

        private void punchChallengeCustom_Click(object sender, EventArgs e)
        {
            Option_Clicked(ClassToCall.PunchChallengeCustom);
        }

        private void h32_Click(object sender, EventArgs e)
        {

        }

        private void tableLayoutPanel1_Paint_1(object sender, PaintEventArgs e)
        {

        }

        private void timer_Tick(object sender, EventArgs e)
        {
            clockText.Text = DateTime.Now.ToString("HH:mm");
            date.Text = DateTime.Now.ToString("MMM dd yyyy");
        }

        private void smallInfo1_Click(object sender, EventArgs e)
        {

        }
    }

    // Provides an enum to determine which element on screen is needed to be shown
    // Makes it easier to implement new pages if need be
    public enum ClassToCall
    {
        ComboCounter,
        ComboCounterCustom,
        TotalForce,
        TotalForce2Play,
        TotalTime,
        TotalTimeCustom,
        CompetePound,
        SensorAdj,
        PunchCount,
        PunchCountCustom,
        PunchChallenge,
        PunchChallengeCustom
    }
    
    // Allows us to pass through to the main page the enum to tell the main page
    // which page to place in the panel next. This feeds into the switch case.
    public class OptionClickEventArgs : EventArgs
    {

        public ClassToCall ClassToCall { get; set; }

        public OptionClickEventArgs(ClassToCall classToCall)
        {
            ClassToCall = classToCall;
        }
        
    }
}