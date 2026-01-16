
namespace cs_win_fs_watcher
{
    partial class FrmMain
    {
        /// <summary>
        ///  Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        ///  Clean up any resources being used.
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
        ///  Required method for Designer support - do not modify
        ///  the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(FrmMain));
            this.branding = new System.Windows.Forms.PictureBox();
            this.lblStatus = new System.Windows.Forms.Label();
            this.FolderBro = new System.Windows.Forms.FolderBrowserDialog();
            this.btnStart = new System.Windows.Forms.Button();
            this.btnStop = new System.Windows.Forms.Button();
            this.grpActions = new System.Windows.Forms.GroupBox();
            this.c4 = new System.Windows.Forms.CheckBox();
            this.c3 = new System.Windows.Forms.CheckBox();
            this.c2 = new System.Windows.Forms.CheckBox();
            this.c1 = new System.Windows.Forms.CheckBox();
            this.brandLabel = new System.Windows.Forms.Label();
            this.richUpdate = new System.Windows.Forms.RichTextBox();
            this.btnSelect = new System.Windows.Forms.Button();
            this.ChkSubPath = new System.Windows.Forms.CheckBox();
            this.FSWatcher = new System.IO.FileSystemWatcher();
            ((System.ComponentModel.ISupportInitialize)(this.branding)).BeginInit();
            this.grpActions.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)(this.FSWatcher)).BeginInit();
            this.SuspendLayout();
            // 
            // branding
            // 
            this.branding.BackgroundImage = global::cs_win_fs_watcher.Properties.Resources.spy;
            this.branding.BackgroundImageLayout = System.Windows.Forms.ImageLayout.Stretch;
            this.branding.Location = new System.Drawing.Point(106, 41);
            this.branding.Name = "branding";
            this.branding.Size = new System.Drawing.Size(261, 261);
            this.branding.TabIndex = 0;
            this.branding.TabStop = false;
            // 
            // lblStatus
            // 
            this.lblStatus.BackColor = System.Drawing.SystemColors.ControlLight;
            this.lblStatus.Dock = System.Windows.Forms.DockStyle.Bottom;
            this.lblStatus.Location = new System.Drawing.Point(0, 754);
            this.lblStatus.Name = "lblStatus";
            this.lblStatus.Size = new System.Drawing.Size(473, 23);
            this.lblStatus.TabIndex = 1;
            this.lblStatus.Text = "Waiting....";
            this.lblStatus.TextAlign = System.Drawing.ContentAlignment.MiddleCenter;
            // 
            // FolderBro
            // 
            this.FolderBro.Description = "Select Watcher Path";
            this.FolderBro.ShowNewFolderButton = false;
            this.FolderBro.UseDescriptionForTitle = true;
            // 
            // btnStart
            // 
            this.btnStart.Enabled = false;
            this.btnStart.FlatAppearance.BorderColor = System.Drawing.Color.FromArgb(((int)(((byte)(255)))), ((int)(((byte)(128)))), ((int)(((byte)(0)))));
            this.btnStart.FlatAppearance.MouseDownBackColor = System.Drawing.Color.FromArgb(((int)(((byte)(255)))), ((int)(((byte)(192)))), ((int)(((byte)(128)))));
            this.btnStart.FlatAppearance.MouseOverBackColor = System.Drawing.Color.FromArgb(((int)(((byte)(255)))), ((int)(((byte)(224)))), ((int)(((byte)(192)))));
            this.btnStart.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.btnStart.Location = new System.Drawing.Point(106, 478);
            this.btnStart.Name = "btnStart";
            this.btnStart.Size = new System.Drawing.Size(123, 33);
            this.btnStart.TabIndex = 2;
            this.btnStart.Text = "Start Monitoring";
            this.btnStart.UseVisualStyleBackColor = true;
            this.btnStart.Click += new System.EventHandler(this.btnStart_Click);
            // 
            // btnStop
            // 
            this.btnStop.Enabled = false;
            this.btnStop.FlatAppearance.BorderColor = System.Drawing.Color.FromArgb(((int)(((byte)(255)))), ((int)(((byte)(128)))), ((int)(((byte)(0)))));
            this.btnStop.FlatAppearance.MouseDownBackColor = System.Drawing.Color.FromArgb(((int)(((byte)(255)))), ((int)(((byte)(224)))), ((int)(((byte)(192)))));
            this.btnStop.FlatAppearance.MouseOverBackColor = System.Drawing.Color.FromArgb(((int)(((byte)(255)))), ((int)(((byte)(192)))), ((int)(((byte)(128)))));
            this.btnStop.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.btnStop.Location = new System.Drawing.Point(244, 478);
            this.btnStop.Name = "btnStop";
            this.btnStop.Size = new System.Drawing.Size(123, 33);
            this.btnStop.TabIndex = 3;
            this.btnStop.Text = "Stop Monitoring";
            this.btnStop.UseVisualStyleBackColor = true;
            this.btnStop.Click += new System.EventHandler(this.btnStop_Click);
            // 
            // grpActions
            // 
            this.grpActions.Controls.Add(this.c4);
            this.grpActions.Controls.Add(this.c3);
            this.grpActions.Controls.Add(this.c2);
            this.grpActions.Controls.Add(this.c1);
            this.grpActions.Location = new System.Drawing.Point(109, 340);
            this.grpActions.Name = "grpActions";
            this.grpActions.Size = new System.Drawing.Size(258, 73);
            this.grpActions.TabIndex = 4;
            this.grpActions.TabStop = false;
            this.grpActions.Text = "Actions";
            // 
            // c4
            // 
            this.c4.AutoSize = true;
            this.c4.Location = new System.Drawing.Point(214, 34);
            this.c4.Name = "c4";
            this.c4.Size = new System.Drawing.Size(34, 19);
            this.c4.TabIndex = 3;
            this.c4.Tag = "4";
            this.c4.Text = "D";
            this.c4.UseVisualStyleBackColor = true;
            // 
            // c3
            // 
            this.c3.AutoSize = true;
            this.c3.Location = new System.Drawing.Point(146, 34);
            this.c3.Name = "c3";
            this.c3.Size = new System.Drawing.Size(34, 19);
            this.c3.TabIndex = 2;
            this.c3.Tag = "3";
            this.c3.Text = "U";
            this.c3.UseVisualStyleBackColor = true;
            // 
            // c2
            // 
            this.c2.AutoSize = true;
            this.c2.Location = new System.Drawing.Point(79, 34);
            this.c2.Name = "c2";
            this.c2.Size = new System.Drawing.Size(33, 19);
            this.c2.TabIndex = 1;
            this.c2.Tag = "2";
            this.c2.Text = "R";
            this.c2.UseVisualStyleBackColor = true;
            // 
            // c1
            // 
            this.c1.AutoSize = true;
            this.c1.Checked = true;
            this.c1.CheckState = System.Windows.Forms.CheckState.Checked;
            this.c1.Location = new System.Drawing.Point(11, 34);
            this.c1.Name = "c1";
            this.c1.Size = new System.Drawing.Size(34, 19);
            this.c1.TabIndex = 0;
            this.c1.Tag = "1";
            this.c1.Text = "C";
            this.c1.UseVisualStyleBackColor = true;
            // 
            // brandLabel
            // 
            this.brandLabel.AutoSize = true;
            this.brandLabel.Font = new System.Drawing.Font("Segoe UI", 18F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point);
            this.brandLabel.ForeColor = System.Drawing.Color.FromArgb(((int)(((byte)(255)))), ((int)(((byte)(128)))), ((int)(((byte)(0)))));
            this.brandLabel.Location = new System.Drawing.Point(173, 305);
            this.brandLabel.Name = "brandLabel";
            this.brandLabel.Size = new System.Drawing.Size(127, 32);
            this.brandLabel.TabIndex = 6;
            this.brandLabel.Text = "Watchhhh";
            // 
            // richUpdate
            // 
            this.richUpdate.Location = new System.Drawing.Point(21, 528);
            this.richUpdate.Name = "richUpdate";
            this.richUpdate.ScrollBars = System.Windows.Forms.RichTextBoxScrollBars.Vertical;
            this.richUpdate.Size = new System.Drawing.Size(430, 203);
            this.richUpdate.TabIndex = 7;
            this.richUpdate.Text = "";
            // 
            // btnSelect
            // 
            this.btnSelect.FlatAppearance.BorderColor = System.Drawing.Color.FromArgb(((int)(((byte)(255)))), ((int)(((byte)(128)))), ((int)(((byte)(0)))));
            this.btnSelect.FlatAppearance.MouseDownBackColor = System.Drawing.Color.FromArgb(((int)(((byte)(255)))), ((int)(((byte)(192)))), ((int)(((byte)(128)))));
            this.btnSelect.FlatAppearance.MouseOverBackColor = System.Drawing.Color.FromArgb(((int)(((byte)(255)))), ((int)(((byte)(224)))), ((int)(((byte)(192)))));
            this.btnSelect.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.btnSelect.Location = new System.Drawing.Point(107, 430);
            this.btnSelect.Name = "btnSelect";
            this.btnSelect.Size = new System.Drawing.Size(79, 33);
            this.btnSelect.TabIndex = 8;
            this.btnSelect.Text = "....";
            this.btnSelect.UseVisualStyleBackColor = true;
            this.btnSelect.Click += new System.EventHandler(this.BtnSelect_Click);
            // 
            // ChkSubPath
            // 
            this.ChkSubPath.AutoSize = true;
            this.ChkSubPath.Location = new System.Drawing.Point(217, 438);
            this.ChkSubPath.Name = "ChkSubPath";
            this.ChkSubPath.Size = new System.Drawing.Size(148, 19);
            this.ChkSubPath.TabIndex = 9;
            this.ChkSubPath.Text = "Watch Sub-Folders Too";
            this.ChkSubPath.UseVisualStyleBackColor = true;
            this.ChkSubPath.CheckedChanged += new System.EventHandler(this.ChkSubPath_CheckedChanged);
            // 
            // FSWatcher
            // 
            this.FSWatcher.EnableRaisingEvents = true;
            this.FSWatcher.SynchronizingObject = this;
            this.FSWatcher.Changed += new System.IO.FileSystemEventHandler(this.FSWatcher_Changed);
            this.FSWatcher.Created += new System.IO.FileSystemEventHandler(this.FSWatcher_Created);
            this.FSWatcher.Deleted += new System.IO.FileSystemEventHandler(this.FSWatcher_Deleted);
            this.FSWatcher.Error += new System.IO.ErrorEventHandler(this.FSWatcher_Error);
            this.FSWatcher.Renamed += new System.IO.RenamedEventHandler(this.FSWatcher_Renamed);
            // 
            // FrmMain
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(7F, 15F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(473, 777);
            this.Controls.Add(this.ChkSubPath);
            this.Controls.Add(this.btnSelect);
            this.Controls.Add(this.richUpdate);
            this.Controls.Add(this.brandLabel);
            this.Controls.Add(this.grpActions);
            this.Controls.Add(this.btnStop);
            this.Controls.Add(this.btnStart);
            this.Controls.Add(this.lblStatus);
            this.Controls.Add(this.branding);
            this.Icon = ((System.Drawing.Icon)(resources.GetObject("$this.Icon")));
            this.MaximizeBox = false;
            this.Name = "FrmMain";
            this.StartPosition = System.Windows.Forms.FormStartPosition.CenterScreen;
            this.Text = "Watcher";
            this.Load += new System.EventHandler(this.FrmMain_Load);
            ((System.ComponentModel.ISupportInitialize)(this.branding)).EndInit();
            this.grpActions.ResumeLayout(false);
            this.grpActions.PerformLayout();
            ((System.ComponentModel.ISupportInitialize)(this.FSWatcher)).EndInit();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.PictureBox branding;
        private System.Windows.Forms.Label lblStatus;
        private System.IO.FileSystemWatcher fileSystemWatcher1;
        private System.Windows.Forms.Button btnStart;
        private System.Windows.Forms.FolderBrowserDialog FolderBro;
        private System.Windows.Forms.RichTextBox richUpdate;
        private System.Windows.Forms.Label brandLabel;
        private System.Windows.Forms.GroupBox grpActions;
        private System.Windows.Forms.CheckBox c4;
        private System.Windows.Forms.CheckBox c3;
        private System.Windows.Forms.CheckBox c2;
        private System.Windows.Forms.CheckBox c1;
        private System.Windows.Forms.Button btnStop;
        private System.Windows.Forms.Button btnSelect;
        private System.Windows.Forms.CheckBox ChkSubPath;
        private System.IO.FileSystemWatcher FSWatcher;
    }
}
