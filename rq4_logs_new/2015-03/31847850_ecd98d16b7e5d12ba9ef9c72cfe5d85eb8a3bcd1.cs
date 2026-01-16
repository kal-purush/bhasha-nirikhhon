namespace Launcher
{
    partial class MainForm
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
            System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(MainForm));
            this.styleManager1 = new DevComponents.DotNetBar.StyleManager(this.components);
            this.metroStatusBar1 = new DevComponents.DotNetBar.Metro.MetroStatusBar();
            this.lblStatus = new DevComponents.DotNetBar.LabelItem();
            this.lblData = new DevComponents.DotNetBar.LabelItem();
            this.reflectionImage1 = new DevComponents.DotNetBar.Controls.ReflectionImage();
            this.MainMenu = new DevComponents.DotNetBar.RadialMenu();
            this.MenuPlay = new DevComponents.DotNetBar.RadialMenuItem();
            this.radialMenuItem1 = new DevComponents.DotNetBar.RadialMenuItem();
            this.MenuSetResolution = new DevComponents.DotNetBar.RadialMenuItem();
            this.MenuResolution2 = new DevComponents.DotNetBar.RadialMenuItem();
            this.MenuResolution0 = new DevComponents.DotNetBar.RadialMenuItem();
            this.MenuResolution1 = new DevComponents.DotNetBar.RadialMenuItem();
            this.MenuResolution3 = new DevComponents.DotNetBar.RadialMenuItem();
            this.MenuSetLanguage = new DevComponents.DotNetBar.RadialMenuItem();
            this.MenuLanguage0 = new DevComponents.DotNetBar.RadialMenuItem();
            this.pgLoading = new DevComponents.DotNetBar.Controls.ProgressBarX();
            this.panelChangeLog = new DevComponents.DotNetBar.ExpandablePanel();
            this.rtbChangeLog = new DevComponents.DotNetBar.Controls.RichTextBoxEx();
            this.exUpdate = new DevComponents.DotNetBar.ExpandablePanel();
            this.rtbUpdate = new DevComponents.DotNetBar.Controls.RichTextBoxEx();
            this.panelChangeLog.SuspendLayout();
            this.exUpdate.SuspendLayout();
            this.SuspendLayout();
            // 
            // styleManager1
            // 
            this.styleManager1.ManagerColorTint = System.Drawing.Color.White;
            this.styleManager1.ManagerStyle = DevComponents.DotNetBar.eStyle.Metro;
            this.styleManager1.MetroColorParameters = new DevComponents.DotNetBar.Metro.ColorTables.MetroColorGeneratorParameters(System.Drawing.Color.FromArgb(((int)(((byte)(255)))), ((int)(((byte)(255)))), ((int)(((byte)(255))))), System.Drawing.Color.FromArgb(((int)(((byte)(0)))), ((int)(((byte)(114)))), ((int)(((byte)(198))))));
            // 
            // metroStatusBar1
            // 
            this.metroStatusBar1.BackColor = System.Drawing.Color.FromArgb(((int)(((byte)(255)))), ((int)(((byte)(255)))), ((int)(((byte)(255)))));
            // 
            // 
            // 
            this.metroStatusBar1.BackgroundStyle.CornerType = DevComponents.DotNetBar.eCornerType.Square;
            this.metroStatusBar1.ContainerControlProcessDialogKey = true;
            this.metroStatusBar1.Dock = System.Windows.Forms.DockStyle.Bottom;
            this.metroStatusBar1.Font = new System.Drawing.Font("Segoe UI", 8.25F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.metroStatusBar1.ForeColor = System.Drawing.Color.Black;
            this.metroStatusBar1.Items.AddRange(new DevComponents.DotNetBar.BaseItem[] {
            this.lblStatus,
            this.lblData});
            this.metroStatusBar1.Location = new System.Drawing.Point(0, 376);
            this.metroStatusBar1.Name = "metroStatusBar1";
            this.metroStatusBar1.Size = new System.Drawing.Size(624, 21);
            this.metroStatusBar1.TabIndex = 1;
            // 
            // lblStatus
            // 
            this.lblStatus.Name = "lblStatus";
            this.lblStatus.Text = "READY";
            // 
            // lblData
            // 
            this.lblData.ForeColor = System.Drawing.Color.MistyRose;
            this.lblData.ItemAlignment = DevComponents.DotNetBar.eItemAlignment.Far;
            this.lblData.Name = "lblData";
            this.lblData.Text = "No Data";
            // 
            // reflectionImage1
            // 
            this.reflectionImage1.BackColor = System.Drawing.Color.FromArgb(((int)(((byte)(255)))), ((int)(((byte)(255)))), ((int)(((byte)(255)))));
            // 
            // 
            // 
            this.reflectionImage1.BackgroundStyle.CornerType = DevComponents.DotNetBar.eCornerType.Square;
            this.reflectionImage1.BackgroundStyle.TextAlignment = DevComponents.DotNetBar.eStyleTextAlignment.Center;
            this.reflectionImage1.Dock = System.Windows.Forms.DockStyle.Top;
            this.reflectionImage1.ForeColor = System.Drawing.Color.Black;
            this.reflectionImage1.Image = ((System.Drawing.Image)(resources.GetObject("reflectionImage1.Image")));
            this.reflectionImage1.Location = new System.Drawing.Point(0, 0);
            this.reflectionImage1.Name = "reflectionImage1";
            this.reflectionImage1.Size = new System.Drawing.Size(624, 227);
            this.reflectionImage1.TabIndex = 2;
            // 
            // MainMenu
            // 
            this.MainMenu.BackColor = System.Drawing.Color.White;
            this.MainMenu.Enabled = false;
            this.MainMenu.ForeColor = System.Drawing.Color.White;
            this.MainMenu.Items.AddRange(new DevComponents.DotNetBar.BaseItem[] {
            this.MenuPlay,
            this.radialMenuItem1});
            this.MainMenu.Location = new System.Drawing.Point(298, 281);
            this.MainMenu.Name = "MainMenu";
            this.MainMenu.Size = new System.Drawing.Size(28, 28);
            this.MainMenu.Symbol = "";
            this.MainMenu.TabIndex = 3;
            // 
            // MenuPlay
            // 
            this.MenuPlay.Name = "MenuPlay";
            this.MenuPlay.Symbol = "";
            this.MenuPlay.Text = "Start";
            this.MenuPlay.Click += new System.EventHandler(this.MenuPlay_Click);
            // 
            // radialMenuItem1
            // 
            this.radialMenuItem1.Name = "radialMenuItem1";
            this.radialMenuItem1.SubItems.AddRange(new DevComponents.DotNetBar.BaseItem[] {
            this.MenuSetResolution,
            this.MenuSetLanguage});
            this.radialMenuItem1.Text = "Settings";
            // 
            // MenuSetResolution
            // 
            this.MenuSetResolution.Name = "MenuSetResolution";
            this.MenuSetResolution.SubItems.AddRange(new DevComponents.DotNetBar.BaseItem[] {
            this.MenuResolution2,
            this.MenuResolution0,
            this.MenuResolution1,
            this.MenuResolution3});
            this.MenuSetResolution.Symbol = "";
            this.MenuSetResolution.Text = "Resolution";
            // 
            // MenuResolution2
            // 
            this.MenuResolution2.Name = "MenuResolution2";
            this.MenuResolution2.Symbol = "";
            this.MenuResolution2.Text = "1024x768";
            this.MenuResolution2.Click += new System.EventHandler(this.MenuResolution_Click);
            // 
            // MenuResolution0
            // 
            this.MenuResolution0.Name = "MenuResolution0";
            this.MenuResolution0.Symbol = "";
            this.MenuResolution0.Text = "800x600";
            this.MenuResolution0.Click += new System.EventHandler(this.MenuResolution_Click);
            // 
            // MenuResolution1
            // 
            this.MenuResolution1.Name = "MenuResolution1";
            this.MenuResolution1.Symbol = "";
            this.MenuResolution1.Text = "1024x600";
            this.MenuResolution1.Click += new System.EventHandler(this.MenuResolution_Click);
            // 
            // MenuResolution3
            // 
            this.MenuResolution3.Name = "MenuResolution3";
            this.MenuResolution3.Symbol = "";
            this.MenuResolution3.Text = "1152x864";
            this.MenuResolution3.Click += new System.EventHandler(this.MenuResolution_Click);
            // 
            // MenuSetLanguage
            // 
            this.MenuSetLanguage.Name = "MenuSetLanguage";
            this.MenuSetLanguage.SubItems.AddRange(new DevComponents.DotNetBar.BaseItem[] {
            this.MenuLanguage0});
            this.MenuSetLanguage.Text = "Language";
            // 
            // MenuLanguage0
            // 
            this.MenuLanguage0.Name = "MenuLanguage0";
            this.MenuLanguage0.Text = "English";
            this.MenuLanguage0.Click += new System.EventHandler(this.MenuLanguage0_Click);
            // 
            // pgLoading
            // 
            this.pgLoading.BackColor = System.Drawing.Color.White;
            // 
            // 
            // 
            this.pgLoading.BackgroundStyle.CornerType = DevComponents.DotNetBar.eCornerType.Square;
            this.pgLoading.Location = new System.Drawing.Point(0, 360);
            this.pgLoading.Name = "pgLoading";
            this.pgLoading.Size = new System.Drawing.Size(624, 10);
            this.pgLoading.Style = DevComponents.DotNetBar.eDotNetBarStyle.Metro;
            this.pgLoading.TabIndex = 4;
            this.pgLoading.Text = "progressBarX1";
            // 
            // panelChangeLog
            // 
            this.panelChangeLog.CanvasColor = System.Drawing.SystemColors.Control;
            this.panelChangeLog.CollapseDirection = DevComponents.DotNetBar.eCollapseDirection.TopToBottom;
            this.panelChangeLog.ColorSchemeStyle = DevComponents.DotNetBar.eDotNetBarStyle.Metro;
            this.panelChangeLog.Controls.Add(this.rtbChangeLog);
            this.panelChangeLog.ExpandButtonVisible = false;
            this.panelChangeLog.Expanded = false;
            this.panelChangeLog.ExpandedBounds = new System.Drawing.Rectangle(424, 254, 200, 100);
            this.panelChangeLog.ExpandOnTitleClick = true;
            this.panelChangeLog.HideControlsWhenCollapsed = true;
            this.panelChangeLog.Location = new System.Drawing.Point(424, 328);
            this.panelChangeLog.Name = "panelChangeLog";
            this.panelChangeLog.Size = new System.Drawing.Size(200, 26);
            this.panelChangeLog.Style.Alignment = System.Drawing.StringAlignment.Center;
            this.panelChangeLog.Style.BackColor1.ColorSchemePart = DevComponents.DotNetBar.eColorSchemePart.PanelBackground;
            this.panelChangeLog.Style.Border = DevComponents.DotNetBar.eBorderType.SingleLine;
            this.panelChangeLog.Style.ForeColor.ColorSchemePart = DevComponents.DotNetBar.eColorSchemePart.ItemText;
            this.panelChangeLog.Style.GradientAngle = 90;
            this.panelChangeLog.TabIndex = 5;
            this.panelChangeLog.TabStop = true;
            this.panelChangeLog.Text = "Change Log";
            this.panelChangeLog.TitleStyle.Alignment = System.Drawing.StringAlignment.Center;
            this.panelChangeLog.TitleStyle.BackColor1.ColorSchemePart = DevComponents.DotNetBar.eColorSchemePart.PanelBackground;
            this.panelChangeLog.TitleStyle.Border = DevComponents.DotNetBar.eBorderType.RaisedInner;
            this.panelChangeLog.TitleStyle.BorderColor.ColorSchemePart = DevComponents.DotNetBar.eColorSchemePart.PanelBorder;
            this.panelChangeLog.TitleStyle.ForeColor.ColorSchemePart = DevComponents.DotNetBar.eColorSchemePart.PanelText;
            this.panelChangeLog.TitleStyle.GradientAngle = 90;
            this.panelChangeLog.TitleText = "Change Log";
            // 
            // rtbChangeLog
            // 
            // 
            // 
            // 
            this.rtbChangeLog.BackgroundStyle.Class = "RichTextBoxBorder";
            this.rtbChangeLog.BackgroundStyle.CornerType = DevComponents.DotNetBar.eCornerType.Square;
            this.rtbChangeLog.Dock = System.Windows.Forms.DockStyle.Fill;
            this.rtbChangeLog.Location = new System.Drawing.Point(0, 26);
            this.rtbChangeLog.Name = "rtbChangeLog";
            this.rtbChangeLog.ReadOnly = true;
            this.rtbChangeLog.Size = new System.Drawing.Size(200, 0);
            this.rtbChangeLog.TabIndex = 1;
            // 
            // exUpdate
            // 
            this.exUpdate.CanvasColor = System.Drawing.SystemColors.Control;
            this.exUpdate.CollapseDirection = DevComponents.DotNetBar.eCollapseDirection.TopToBottom;
            this.exUpdate.ColorSchemeStyle = DevComponents.DotNetBar.eDotNetBarStyle.Metro;
            this.exUpdate.Controls.Add(this.rtbUpdate);
            this.exUpdate.ExpandButtonVisible = false;
            this.exUpdate.Expanded = false;
            this.exUpdate.ExpandedBounds = new System.Drawing.Rectangle(0, 254, 200, 100);
            this.exUpdate.ExpandOnTitleClick = true;
            this.exUpdate.HideControlsWhenCollapsed = true;
            this.exUpdate.Location = new System.Drawing.Point(0, 328);
            this.exUpdate.Name = "exUpdate";
            this.exUpdate.Size = new System.Drawing.Size(200, 26);
            this.exUpdate.Style.Alignment = System.Drawing.StringAlignment.Center;
            this.exUpdate.Style.BackColor1.ColorSchemePart = DevComponents.DotNetBar.eColorSchemePart.PanelBackground;
            this.exUpdate.Style.Border = DevComponents.DotNetBar.eBorderType.SingleLine;
            this.exUpdate.Style.ForeColor.ColorSchemePart = DevComponents.DotNetBar.eColorSchemePart.ItemText;
            this.exUpdate.Style.GradientAngle = 90;
            this.exUpdate.TabIndex = 6;
            this.exUpdate.TabStop = true;
            this.exUpdate.Text = "Change Log";
            this.exUpdate.TitleStyle.Alignment = System.Drawing.StringAlignment.Center;
            this.exUpdate.TitleStyle.BackColor1.ColorSchemePart = DevComponents.DotNetBar.eColorSchemePart.PanelBackground;
            this.exUpdate.TitleStyle.Border = DevComponents.DotNetBar.eBorderType.RaisedInner;
            this.exUpdate.TitleStyle.BorderColor.ColorSchemePart = DevComponents.DotNetBar.eColorSchemePart.PanelBorder;
            this.exUpdate.TitleStyle.ForeColor.ColorSchemePart = DevComponents.DotNetBar.eColorSchemePart.PanelText;
            this.exUpdate.TitleStyle.GradientAngle = 90;
            this.exUpdate.TitleText = "Update Log";
            // 
            // rtbUpdate
            // 
            // 
            // 
            // 
            this.rtbUpdate.BackgroundStyle.Class = "RichTextBoxBorder";
            this.rtbUpdate.BackgroundStyle.CornerType = DevComponents.DotNetBar.eCornerType.Square;
            this.rtbUpdate.Dock = System.Windows.Forms.DockStyle.Fill;
            this.rtbUpdate.Location = new System.Drawing.Point(0, 26);
            this.rtbUpdate.Name = "rtbUpdate";
            this.rtbUpdate.ReadOnly = true;
            this.rtbUpdate.Size = new System.Drawing.Size(200, 0);
            this.rtbUpdate.TabIndex = 1;
            // 
            // MainForm
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(624, 397);
            this.Controls.Add(this.exUpdate);
            this.Controls.Add(this.panelChangeLog);
            this.Controls.Add(this.pgLoading);
            this.Controls.Add(this.MainMenu);
            this.Controls.Add(this.reflectionImage1);
            this.Controls.Add(this.metroStatusBar1);
            this.Font = new System.Drawing.Font("Segoe UI", 8.25F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.FormBorderStyle = System.Windows.Forms.FormBorderStyle.FixedToolWindow;
            this.Icon = ((System.Drawing.Icon)(resources.GetObject("$this.Icon")));
            this.MaximizeBox = false;
            this.Name = "MainForm";
            this.StartPosition = System.Windows.Forms.FormStartPosition.CenterScreen;
            this.Text = "Terraria Launcher";
            this.Load += new System.EventHandler(this.MainForm_Load);
            this.panelChangeLog.ResumeLayout(false);
            this.exUpdate.ResumeLayout(false);
            this.ResumeLayout(false);

        }

        #endregion

        private DevComponents.DotNetBar.StyleManager styleManager1;
        private DevComponents.DotNetBar.Metro.MetroStatusBar metroStatusBar1;
        private DevComponents.DotNetBar.LabelItem lblStatus;
        private DevComponents.DotNetBar.Controls.ReflectionImage reflectionImage1;
        private DevComponents.DotNetBar.RadialMenu MainMenu;
        private DevComponents.DotNetBar.RadialMenuItem MenuSetResolution;
        private DevComponents.DotNetBar.RadialMenuItem MenuResolution2;
        private DevComponents.DotNetBar.RadialMenuItem MenuResolution0;
        private DevComponents.DotNetBar.RadialMenuItem MenuResolution1;
        private DevComponents.DotNetBar.RadialMenuItem MenuResolution3;
        private DevComponents.DotNetBar.RadialMenuItem MenuPlay;
        private DevComponents.DotNetBar.RadialMenuItem radialMenuItem1;
        private DevComponents.DotNetBar.RadialMenuItem MenuSetLanguage;
        private DevComponents.DotNetBar.RadialMenuItem MenuLanguage0;
        private DevComponents.DotNetBar.Controls.ProgressBarX pgLoading;
        private DevComponents.DotNetBar.LabelItem lblData;
        private DevComponents.DotNetBar.ExpandablePanel panelChangeLog;
        private DevComponents.DotNetBar.Controls.RichTextBoxEx rtbChangeLog;
        private DevComponents.DotNetBar.ExpandablePanel exUpdate;
        private DevComponents.DotNetBar.Controls.RichTextBoxEx rtbUpdate;

    }
}
