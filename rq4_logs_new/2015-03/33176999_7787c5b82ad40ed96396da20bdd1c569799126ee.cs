namespace SeriesManager
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
            this.btn_check = new System.Windows.Forms.Button();
            this.defaultSource = new System.Windows.Forms.Button();
            this.fileSystemWatcher1 = new System.IO.FileSystemWatcher();
            this.label_DefaultDownload = new System.Windows.Forms.Label();
            this.btn_move = new System.Windows.Forms.Button();
            this.lb_filesToMove = new System.Windows.Forms.CheckedListBox();
            this.link_checkAll = new System.Windows.Forms.LinkLabel();
            ((System.ComponentModel.ISupportInitialize)(this.fileSystemWatcher1)).BeginInit();
            this.SuspendLayout();
            // 
            // btn_check
            // 
            this.btn_check.BackColor = System.Drawing.SystemColors.ButtonFace;
            this.btn_check.Location = new System.Drawing.Point(5, 118);
            this.btn_check.Name = "btn_check";
            this.btn_check.Size = new System.Drawing.Size(136, 38);
            this.btn_check.TabIndex = 0;
            this.btn_check.Text = "Check";
            this.btn_check.UseVisualStyleBackColor = false;
            this.btn_check.Click += new System.EventHandler(this.btn_check_Click);
            // 
            // defaultSource
            // 
            this.defaultSource.BackColor = System.Drawing.SystemColors.ButtonFace;
            this.defaultSource.Location = new System.Drawing.Point(5, 51);
            this.defaultSource.Name = "defaultSource";
            this.defaultSource.Size = new System.Drawing.Size(136, 38);
            this.defaultSource.TabIndex = 1;
            this.defaultSource.Text = "Change it !";
            this.defaultSource.UseVisualStyleBackColor = false;
            this.defaultSource.Click += new System.EventHandler(this.btnSource_Click);
            // 
            // fileSystemWatcher1
            // 
            this.fileSystemWatcher1.EnableRaisingEvents = true;
            this.fileSystemWatcher1.SynchronizingObject = this;
            // 
            // label_DefaultDownload
            // 
            this.label_DefaultDownload.AutoSize = true;
            this.label_DefaultDownload.Location = new System.Drawing.Point(13, 18);
            this.label_DefaultDownload.Name = "label_DefaultDownload";
            this.label_DefaultDownload.Size = new System.Drawing.Size(128, 13);
            this.label_DefaultDownload.TabIndex = 2;
            this.label_DefaultDownload.Text = "Current download folder : ";
            // 
            // btn_move
            // 
            this.btn_move.BackColor = System.Drawing.SystemColors.ButtonFace;
            this.btn_move.Location = new System.Drawing.Point(5, 181);
            this.btn_move.Name = "btn_move";
            this.btn_move.Size = new System.Drawing.Size(135, 38);
            this.btn_move.TabIndex = 4;
            this.btn_move.Text = "Move";
            this.btn_move.UseVisualStyleBackColor = false;
            this.btn_move.Click += new System.EventHandler(this.btn_move_Click);
            // 
            // lb_filesToMove
            // 
            this.lb_filesToMove.AllowDrop = true;
            this.lb_filesToMove.FormattingEnabled = true;
            this.lb_filesToMove.Location = new System.Drawing.Point(160, 48);
            this.lb_filesToMove.Name = "lb_filesToMove";
            this.lb_filesToMove.Size = new System.Drawing.Size(517, 379);
            this.lb_filesToMove.TabIndex = 5;
            // 
            // link_checkAll
            // 
            this.link_checkAll.AutoSize = true;
            this.link_checkAll.Location = new System.Drawing.Point(88, 414);
            this.link_checkAll.Name = "link_checkAll";
            this.link_checkAll.Size = new System.Drawing.Size(52, 13);
            this.link_checkAll.TabIndex = 6;
            this.link_checkAll.TabStop = true;
            this.link_checkAll.Text = "Check All";
            this.link_checkAll.LinkClicked += new System.Windows.Forms.LinkLabelLinkClickedEventHandler(this.link_checkAll_LinkClicked);
            // 
            // Form1
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(691, 437);
            this.Controls.Add(this.link_checkAll);
            this.Controls.Add(this.lb_filesToMove);
            this.Controls.Add(this.btn_move);
            this.Controls.Add(this.label_DefaultDownload);
            this.Controls.Add(this.defaultSource);
            this.Controls.Add(this.btn_check);
            this.Name = "Form1";
            this.Text = "Download Manager";
            this.Load += new System.EventHandler(this.Form1_Load);
            ((System.ComponentModel.ISupportInitialize)(this.fileSystemWatcher1)).EndInit();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Button btn_check;
        private System.Windows.Forms.Button defaultSource;
        private System.IO.FileSystemWatcher fileSystemWatcher1;
        private System.Windows.Forms.Label label_DefaultDownload;
        private System.Windows.Forms.Button btn_move;
        private System.Windows.Forms.CheckedListBox lb_filesToMove;
        private System.Windows.Forms.LinkLabel link_checkAll;
    }
}
