namespace SquareRootsSolver
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
            this.itsMenuStrip = new System.Windows.Forms.MenuStrip();
            this.fileToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.itsMenuItem_File_NewGrid = new System.Windows.Forms.ToolStripMenuItem();
            this.toolStripSeparator1 = new System.Windows.Forms.ToolStripSeparator();
            this.itsMenuItem_File_Close = new System.Windows.Forms.ToolStripMenuItem();
            this.itsMenuItem_Tools_Solve = new System.Windows.Forms.ToolStripMenuItem();
            this.solveToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.itsMenuStrip.SuspendLayout();
            this.SuspendLayout();
            // 
            // itsMenuStrip
            // 
            this.itsMenuStrip.Items.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.fileToolStripMenuItem,
            this.itsMenuItem_Tools_Solve});
            this.itsMenuStrip.Location = new System.Drawing.Point(0, 0);
            this.itsMenuStrip.Name = "itsMenuStrip";
            this.itsMenuStrip.Size = new System.Drawing.Size(559, 24);
            this.itsMenuStrip.TabIndex = 0;
            this.itsMenuStrip.Text = "menuStrip1";
            // 
            // fileToolStripMenuItem
            // 
            this.fileToolStripMenuItem.DropDownItems.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.itsMenuItem_File_NewGrid,
            this.toolStripSeparator1,
            this.itsMenuItem_File_Close});
            this.fileToolStripMenuItem.Name = "fileToolStripMenuItem";
            this.fileToolStripMenuItem.Size = new System.Drawing.Size(37, 20);
            this.fileToolStripMenuItem.Text = "File";
            // 
            // itsMenuItem_File_NewGrid
            // 
            this.itsMenuItem_File_NewGrid.Name = "itsMenuItem_File_NewGrid";
            this.itsMenuItem_File_NewGrid.Size = new System.Drawing.Size(152, 22);
            this.itsMenuItem_File_NewGrid.Text = "New Grid";
            // 
            // toolStripSeparator1
            // 
            this.toolStripSeparator1.Name = "toolStripSeparator1";
            this.toolStripSeparator1.Size = new System.Drawing.Size(149, 6);
            // 
            // itsMenuItem_File_Close
            // 
            this.itsMenuItem_File_Close.Name = "itsMenuItem_File_Close";
            this.itsMenuItem_File_Close.Size = new System.Drawing.Size(152, 22);
            this.itsMenuItem_File_Close.Text = "Close";
            // 
            // itsMenuItem_Tools_Solve
            // 
            this.itsMenuItem_Tools_Solve.DropDownItems.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.solveToolStripMenuItem});
            this.itsMenuItem_Tools_Solve.Name = "itsMenuItem_Tools_Solve";
            this.itsMenuItem_Tools_Solve.Size = new System.Drawing.Size(48, 20);
            this.itsMenuItem_Tools_Solve.Text = "Tools";
            // 
            // solveToolStripMenuItem
            // 
            this.solveToolStripMenuItem.Name = "solveToolStripMenuItem";
            this.solveToolStripMenuItem.Size = new System.Drawing.Size(152, 22);
            this.solveToolStripMenuItem.Text = "Solve";
            // 
            // MainForm
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(559, 514);
            this.Controls.Add(this.itsMenuStrip);
            this.MainMenuStrip = this.itsMenuStrip;
            this.Name = "MainForm";
            this.Text = "Square Roots Solver";
            this.itsMenuStrip.ResumeLayout(false);
            this.itsMenuStrip.PerformLayout();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.MenuStrip itsMenuStrip;
        private System.Windows.Forms.ToolStripMenuItem fileToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem itsMenuItem_File_NewGrid;
        private System.Windows.Forms.ToolStripSeparator toolStripSeparator1;
        private System.Windows.Forms.ToolStripMenuItem itsMenuItem_File_Close;
        private System.Windows.Forms.ToolStripMenuItem itsMenuItem_Tools_Solve;
        private System.Windows.Forms.ToolStripMenuItem solveToolStripMenuItem;
    }
}
