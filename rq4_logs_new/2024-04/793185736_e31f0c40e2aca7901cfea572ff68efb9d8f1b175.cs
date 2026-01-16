namespace GogotSharp
{
    partial class Main
    {
        /// <summary>
        /// Variable nécessaire au concepteur.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// Nettoyage des ressources utilisées.
        /// </summary>
        /// <param name="disposing">true si les ressources managées doivent être supprimées ; sinon, false.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Code généré par le Concepteur Windows Form

        /// <summary>
        /// Méthode requise pour la prise en charge du concepteur - ne modifiez pas
        /// le contenu de cette méthode avec l'éditeur de code.
        /// </summary>
        private void InitializeComponent()
        {
            System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(Main));
            this.formStrip = new System.Windows.Forms.MenuStrip();
            this.fileToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.startWithWindowsToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.ignoreGodot4ToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.exitToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.godotInstall = new System.Windows.Forms.Button();
            this.godotUninstall = new System.Windows.Forms.Button();
            this.info = new System.Windows.Forms.Label();
            this.godotVersionSelect = new System.Windows.Forms.ComboBox();
            this.godotBtn = new System.Windows.Forms.Button();
            this.formStrip.SuspendLayout();
            this.SuspendLayout();
            // 
            // formStrip
            // 
            this.formStrip.Font = new System.Drawing.Font("Segoe UI", 10F);
            this.formStrip.GripMargin = new System.Windows.Forms.Padding(0);
            this.formStrip.ImageScalingSize = new System.Drawing.Size(20, 20);
            this.formStrip.Items.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.fileToolStripMenuItem});
            this.formStrip.Location = new System.Drawing.Point(0, 0);
            this.formStrip.Name = "formStrip";
            this.formStrip.Padding = new System.Windows.Forms.Padding(0);
            this.formStrip.RenderMode = System.Windows.Forms.ToolStripRenderMode.System;
            this.formStrip.Size = new System.Drawing.Size(272, 24);
            this.formStrip.TabIndex = 0;
            this.formStrip.Text = "FormStrip";
            // 
            // fileToolStripMenuItem
            // 
            this.fileToolStripMenuItem.DisplayStyle = System.Windows.Forms.ToolStripItemDisplayStyle.Text;
            this.fileToolStripMenuItem.DropDownItems.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.startWithWindowsToolStripMenuItem,
            this.ignoreGodot4ToolStripMenuItem,
            this.exitToolStripMenuItem});
            this.fileToolStripMenuItem.Font = new System.Drawing.Font("Segoe UI", 9F);
            this.fileToolStripMenuItem.Name = "fileToolStripMenuItem";
            this.fileToolStripMenuItem.Size = new System.Drawing.Size(75, 24);
            this.fileToolStripMenuItem.Text = "Options";
            // 
            // startWithWindowsToolStripMenuItem
            // 
            this.startWithWindowsToolStripMenuItem.DisplayStyle = System.Windows.Forms.ToolStripItemDisplayStyle.Text;
            this.startWithWindowsToolStripMenuItem.Name = "startWithWindowsToolStripMenuItem";
            this.startWithWindowsToolStripMenuItem.Size = new System.Drawing.Size(217, 26);
            this.startWithWindowsToolStripMenuItem.Text = "Start with windows";
            this.startWithWindowsToolStripMenuItem.Click += new System.EventHandler(this.startWithWindowsToolStripMenuItem_Click);
            // 
            // ignoreGodot4ToolStripMenuItem
            // 
            this.ignoreGodot4ToolStripMenuItem.DisplayStyle = System.Windows.Forms.ToolStripItemDisplayStyle.Text;
            this.ignoreGodot4ToolStripMenuItem.Name = "ignoreGodot4ToolStripMenuItem";
            this.ignoreGodot4ToolStripMenuItem.Size = new System.Drawing.Size(217, 26);
            this.ignoreGodot4ToolStripMenuItem.Text = "Ignore Godot 4";
            this.ignoreGodot4ToolStripMenuItem.Click += new System.EventHandler(this.ignoreGodot4ToolStripMenuItem_Click);
            // 
            // exitToolStripMenuItem
            // 
            this.exitToolStripMenuItem.DisplayStyle = System.Windows.Forms.ToolStripItemDisplayStyle.Text;
            this.exitToolStripMenuItem.Name = "exitToolStripMenuItem";
            this.exitToolStripMenuItem.Size = new System.Drawing.Size(217, 26);
            this.exitToolStripMenuItem.Text = "Exit";
            this.exitToolStripMenuItem.Click += new System.EventHandler(this.exitToolStripMenuItem_Click);
            // 
            // godotInstall
            // 
            this.godotInstall.Enabled = false;
            this.godotInstall.Location = new System.Drawing.Point(60, 210);
            this.godotInstall.Name = "godotInstall";
            this.godotInstall.Size = new System.Drawing.Size(150, 30);
            this.godotInstall.TabIndex = 2;
            this.godotInstall.Text = "Install";
            this.godotInstall.UseVisualStyleBackColor = true;
            this.godotInstall.Click += new System.EventHandler(this.godotInstall_Click);
            // 
            // godotUninstall
            // 
            this.godotUninstall.Enabled = false;
            this.godotUninstall.Location = new System.Drawing.Point(60, 250);
            this.godotUninstall.Name = "godotUninstall";
            this.godotUninstall.Size = new System.Drawing.Size(150, 30);
            this.godotUninstall.TabIndex = 3;
            this.godotUninstall.Text = "Uninstall";
            this.godotUninstall.UseVisualStyleBackColor = true;
            this.godotUninstall.Click += new System.EventHandler(this.godotUninstall_Click);
            // 
            // info
            // 
            this.info.AutoSize = true;
            this.info.Location = new System.Drawing.Point(15, 310);
            this.info.Name = "info";
            this.info.Size = new System.Drawing.Size(0, 16);
            this.info.TabIndex = 19;
            // 
            // godotVersionSelect
            // 
            this.godotVersionSelect.DropDownStyle = System.Windows.Forms.ComboBoxStyle.DropDownList;
            this.godotVersionSelect.FormattingEnabled = true;
            this.godotVersionSelect.Location = new System.Drawing.Point(60, 165);
            this.godotVersionSelect.MinimumSize = new System.Drawing.Size(150, 0);
            this.godotVersionSelect.Name = "godotVersionSelect";
            this.godotVersionSelect.Size = new System.Drawing.Size(150, 24);
            this.godotVersionSelect.TabIndex = 20;
            this.godotVersionSelect.SelectedIndexChanged += new System.EventHandler(this.godotVersionSelect_SelectedIndexChanged);
            // 
            // godotBtn
            // 
            this.godotBtn.BackgroundImage = global::GogotSharp.Properties.Resources.Godot_png;
            this.godotBtn.BackgroundImageLayout = System.Windows.Forms.ImageLayout.Zoom;
            this.godotBtn.FlatAppearance.BorderSize = 0;
            this.godotBtn.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.godotBtn.Location = new System.Drawing.Point(60, 50);
            this.godotBtn.Name = "godotBtn";
            this.godotBtn.Size = new System.Drawing.Size(150, 100);
            this.godotBtn.TabIndex = 1;
            this.godotBtn.UseVisualStyleBackColor = true;
            this.godotBtn.Click += new System.EventHandler(this.godotBtn_Click);
            // 
            // Main
            // 
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.None;
            this.ClientSize = new System.Drawing.Size(272, 323);
            this.Controls.Add(this.godotVersionSelect);
            this.Controls.Add(this.info);
            this.Controls.Add(this.godotUninstall);
            this.Controls.Add(this.godotInstall);
            this.Controls.Add(this.godotBtn);
            this.Controls.Add(this.formStrip);
            this.Font = new System.Drawing.Font("Microsoft Sans Serif", 7.8F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.FormBorderStyle = System.Windows.Forms.FormBorderStyle.FixedSingle;
            this.Icon = ((System.Drawing.Icon)(resources.GetObject("$this.Icon")));
            this.MainMenuStrip = this.formStrip;
            this.MaximizeBox = false;
            this.MaximumSize = new System.Drawing.Size(290, 370);
            this.MinimizeBox = false;
            this.MinimumSize = new System.Drawing.Size(290, 370);
            this.Name = "Main";
            this.ShowInTaskbar = false;
            this.SizeGripStyle = System.Windows.Forms.SizeGripStyle.Hide;
            this.StartPosition = System.Windows.Forms.FormStartPosition.Manual;
            this.Text = "GodotSharp";
            this.Load += new System.EventHandler(this.Main_Load);
            this.formStrip.ResumeLayout(false);
            this.formStrip.PerformLayout();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.MenuStrip formStrip;
        private System.Windows.Forms.ToolStripMenuItem fileToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem startWithWindowsToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem exitToolStripMenuItem;
        private System.Windows.Forms.Button godotBtn;
        private System.Windows.Forms.Button godotInstall;
        private System.Windows.Forms.Button godotUninstall;
        private System.Windows.Forms.Label info;
        private System.Windows.Forms.ToolStripMenuItem ignoreGodot4ToolStripMenuItem;
        private System.Windows.Forms.ComboBox godotVersionSelect;
    }
}
