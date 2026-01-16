
namespace BlightnautsDialogue
{
    partial class EditorWindow
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
            System.Windows.Forms.MenuStrip topBar;
            System.Windows.Forms.ToolStripSeparator toolStripSeparator1;
            System.Windows.Forms.ToolStripSeparator toolStripSeparator2;
            System.Windows.Forms.ToolStripSeparator toolStripSeparator3;
            this.topBarFile = new System.Windows.Forms.ToolStripMenuItem();
            this.topBarNew = new System.Windows.Forms.ToolStripMenuItem();
            this.topBarOpen = new System.Windows.Forms.ToolStripMenuItem();
            this.topBarSave = new System.Windows.Forms.ToolStripMenuItem();
            this.topBarSaveAs = new System.Windows.Forms.ToolStripMenuItem();
            this.topBarExit = new System.Windows.Forms.ToolStripMenuItem();
            this.topBarProject = new System.Windows.Forms.ToolStripMenuItem();
            this.topBarNewTrigger = new System.Windows.Forms.ToolStripMenuItem();
            this.topBarLoadTrigger = new System.Windows.Forms.ToolStripMenuItem();
            this.topBarInfo = new System.Windows.Forms.ToolStripMenuItem();
            this.topBarHelp = new System.Windows.Forms.ToolStripMenuItem();
            this.topBarAbout = new System.Windows.Forms.ToolStripMenuItem();
            topBar = new System.Windows.Forms.MenuStrip();
            toolStripSeparator1 = new System.Windows.Forms.ToolStripSeparator();
            toolStripSeparator2 = new System.Windows.Forms.ToolStripSeparator();
            toolStripSeparator3 = new System.Windows.Forms.ToolStripSeparator();
            topBar.SuspendLayout();
            this.SuspendLayout();
            // 
            // topBar
            // 
            topBar.Items.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.topBarFile,
            this.topBarProject,
            this.topBarInfo});
            topBar.Location = new System.Drawing.Point(0, 0);
            topBar.Name = "topBar";
            topBar.Size = new System.Drawing.Size(800, 24);
            topBar.TabIndex = 0;
            topBar.Text = "topBar";
            // 
            // topBarFile
            // 
            this.topBarFile.DropDownItems.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.topBarNew,
            toolStripSeparator1,
            this.topBarOpen,
            toolStripSeparator2,
            this.topBarSave,
            this.topBarSaveAs,
            toolStripSeparator3,
            this.topBarExit});
            this.topBarFile.Name = "topBarFile";
            this.topBarFile.Size = new System.Drawing.Size(37, 20);
            this.topBarFile.Text = "File";
            // 
            // topBarNew
            // 
            this.topBarNew.Name = "topBarNew";
            this.topBarNew.Size = new System.Drawing.Size(180, 22);
            this.topBarNew.Text = "New";
            this.topBarNew.Click += new System.EventHandler(this.topBarNew_Click);
            // 
            // topBarOpen
            // 
            this.topBarOpen.Name = "topBarOpen";
            this.topBarOpen.Size = new System.Drawing.Size(180, 22);
            this.topBarOpen.Text = "Open";
            this.topBarOpen.Click += new System.EventHandler(this.topBarOpen_Click);
            // 
            // topBarSave
            // 
            this.topBarSave.Name = "topBarSave";
            this.topBarSave.Size = new System.Drawing.Size(180, 22);
            this.topBarSave.Text = "Save";
            this.topBarSave.Click += new System.EventHandler(this.topBarSave_Click);
            // 
            // topBarSaveAs
            // 
            this.topBarSaveAs.Name = "topBarSaveAs";
            this.topBarSaveAs.Size = new System.Drawing.Size(180, 22);
            this.topBarSaveAs.Text = "Save as";
            this.topBarSaveAs.Click += new System.EventHandler(this.topBarSaveAs_Click);
            // 
            // toolStripSeparator1
            // 
            toolStripSeparator1.Name = "toolStripSeparator1";
            toolStripSeparator1.Size = new System.Drawing.Size(177, 6);
            // 
            // toolStripSeparator2
            // 
            toolStripSeparator2.Name = "toolStripSeparator2";
            toolStripSeparator2.Size = new System.Drawing.Size(177, 6);
            // 
            // toolStripSeparator3
            // 
            toolStripSeparator3.Name = "toolStripSeparator3";
            toolStripSeparator3.Size = new System.Drawing.Size(177, 6);
            // 
            // topBarExit
            // 
            this.topBarExit.Name = "topBarExit";
            this.topBarExit.Size = new System.Drawing.Size(180, 22);
            this.topBarExit.Text = "Exit";
            this.topBarExit.Click += new System.EventHandler(this.topBarExit_Click);
            // 
            // topBarProject
            // 
            this.topBarProject.DropDownItems.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.topBarNewTrigger,
            this.topBarLoadTrigger});
            this.topBarProject.Name = "topBarProject";
            this.topBarProject.Size = new System.Drawing.Size(56, 20);
            this.topBarProject.Text = "Project";
            // 
            // topBarNewTrigger
            // 
            this.topBarNewTrigger.Name = "topBarNewTrigger";
            this.topBarNewTrigger.Size = new System.Drawing.Size(180, 22);
            this.topBarNewTrigger.Text = "New Trigger";
            this.topBarNewTrigger.Click += new System.EventHandler(this.topBarNewTrigger_Click);
            // 
            // topBarLoadTrigger
            // 
            this.topBarLoadTrigger.Name = "topBarLoadTrigger";
            this.topBarLoadTrigger.Size = new System.Drawing.Size(180, 22);
            this.topBarLoadTrigger.Text = "Load Trigger";
            this.topBarLoadTrigger.Click += new System.EventHandler(this.topBarLoadTrigger_Click);
            // 
            // topBarInfo
            // 
            this.topBarInfo.DropDownItems.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.topBarHelp,
            this.topBarAbout});
            this.topBarInfo.Name = "topBarInfo";
            this.topBarInfo.Size = new System.Drawing.Size(40, 20);
            this.topBarInfo.Text = "Info";
            // 
            // topBarHelp
            // 
            this.topBarHelp.Name = "topBarHelp";
            this.topBarHelp.Size = new System.Drawing.Size(180, 22);
            this.topBarHelp.Text = "Help";
            // 
            // topBarAbout
            // 
            this.topBarAbout.Name = "topBarAbout";
            this.topBarAbout.Size = new System.Drawing.Size(180, 22);
            this.topBarAbout.Text = "About";
            // 
            // EditorWindow
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(800, 450);
            this.Controls.Add(topBar);
            this.MainMenuStrip = topBar;
            this.Name = "EditorWindow";
            this.Text = "EditorWindow";
            topBar.ResumeLayout(false);
            topBar.PerformLayout();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.ToolStripMenuItem topBarFile;
        private System.Windows.Forms.ToolStripMenuItem topBarNew;
        private System.Windows.Forms.ToolStripMenuItem topBarOpen;
        private System.Windows.Forms.ToolStripMenuItem topBarSave;
        private System.Windows.Forms.ToolStripMenuItem topBarSaveAs;
        private System.Windows.Forms.ToolStripMenuItem topBarExit;
        private System.Windows.Forms.ToolStripMenuItem topBarProject;
        private System.Windows.Forms.ToolStripMenuItem topBarNewTrigger;
        private System.Windows.Forms.ToolStripMenuItem topBarLoadTrigger;
        private System.Windows.Forms.ToolStripMenuItem topBarInfo;
        private System.Windows.Forms.ToolStripMenuItem topBarHelp;
        private System.Windows.Forms.ToolStripMenuItem topBarAbout;
    }
}