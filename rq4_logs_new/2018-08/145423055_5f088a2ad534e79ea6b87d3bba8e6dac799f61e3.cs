namespace TIIDE
{
    partial class FormFileDetails
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
            this.lblFileName = new System.Windows.Forms.Label();
            this.lblProgramName = new System.Windows.Forms.Label();
            this.lblComment = new System.Windows.Forms.Label();
            this.lblProtected = new System.Windows.Forms.Label();
            this.btnClose = new System.Windows.Forms.Button();
            this.SuspendLayout();
            // 
            // lblFileName
            // 
            this.lblFileName.AutoSize = true;
            this.lblFileName.Location = new System.Drawing.Point(12, 9);
            this.lblFileName.Name = "lblFileName";
            this.lblFileName.Size = new System.Drawing.Size(34, 17);
            this.lblFileName.TabIndex = 0;
            this.lblFileName.Text = "File:";
            // 
            // lblProgramName
            // 
            this.lblProgramName.AutoSize = true;
            this.lblProgramName.Location = new System.Drawing.Point(12, 26);
            this.lblProgramName.Name = "lblProgramName";
            this.lblProgramName.Size = new System.Drawing.Size(107, 17);
            this.lblProgramName.TabIndex = 1;
            this.lblProgramName.Text = "Program Name:";
            // 
            // lblComment
            // 
            this.lblComment.AutoSize = true;
            this.lblComment.Location = new System.Drawing.Point(12, 43);
            this.lblComment.Name = "lblComment";
            this.lblComment.Size = new System.Drawing.Size(71, 17);
            this.lblComment.TabIndex = 2;
            this.lblComment.Text = "Comment:";
            // 
            // lblProtected
            // 
            this.lblProtected.AutoSize = true;
            this.lblProtected.Location = new System.Drawing.Point(12, 60);
            this.lblProtected.Name = "lblProtected";
            this.lblProtected.Size = new System.Drawing.Size(73, 17);
            this.lblProtected.TabIndex = 3;
            this.lblProtected.Text = "Protected:";
            // 
            // btnClose
            // 
            this.btnClose.Location = new System.Drawing.Point(146, 86);
            this.btnClose.Name = "btnClose";
            this.btnClose.Size = new System.Drawing.Size(75, 23);
            this.btnClose.TabIndex = 4;
            this.btnClose.Text = "Close";
            this.btnClose.UseVisualStyleBackColor = true;
            this.btnClose.Click += new System.EventHandler(this.Button1_Click);
            // 
            // formFileDetails
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(8F, 16F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(366, 121);
            this.Controls.Add(this.btnClose);
            this.Controls.Add(this.lblProtected);
            this.Controls.Add(this.lblComment);
            this.Controls.Add(this.lblProgramName);
            this.Controls.Add(this.lblFileName);
            this.Name = "formFileDetails";
            this.Text = "File Information";
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Label lblFileName;
        private System.Windows.Forms.Label lblProgramName;
        private System.Windows.Forms.Label lblComment;
        private System.Windows.Forms.Label lblProtected;
        private System.Windows.Forms.Button btnClose;
    }
}