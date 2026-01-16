namespace CaptureScreen
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
            this.EnableCapture = new System.Windows.Forms.Button();
            this.DisableCapture = new System.Windows.Forms.Button();
            this.FolderPath = new System.Windows.Forms.TextBox();
            this.SelectFolder = new System.Windows.Forms.Button();
            this.SuspendLayout();
            // 
            // EnableCapture
            // 
            this.EnableCapture.Location = new System.Drawing.Point(280, 75);
            this.EnableCapture.Name = "EnableCapture";
            this.EnableCapture.Size = new System.Drawing.Size(75, 23);
            this.EnableCapture.TabIndex = 0;
            this.EnableCapture.Text = "启动";
            this.EnableCapture.UseVisualStyleBackColor = true;
            this.EnableCapture.Click += new System.EventHandler(this.EnableCapture_Click);
            // 
            // DisableCapture
            // 
            this.DisableCapture.Enabled = false;
            this.DisableCapture.Location = new System.Drawing.Point(373, 75);
            this.DisableCapture.Name = "DisableCapture";
            this.DisableCapture.Size = new System.Drawing.Size(75, 23);
            this.DisableCapture.TabIndex = 1;
            this.DisableCapture.Text = "停止";
            this.DisableCapture.UseVisualStyleBackColor = true;
            this.DisableCapture.Click += new System.EventHandler(this.DisableCapture_Click);
            // 
            // FolderPath
            // 
            this.FolderPath.Location = new System.Drawing.Point(104, 31);
            this.FolderPath.Name = "FolderPath";
            this.FolderPath.Size = new System.Drawing.Size(344, 21);
            this.FolderPath.TabIndex = 2;
            this.FolderPath.Text = "D:\\";
            // 
            // SelectFolder
            // 
            this.SelectFolder.Location = new System.Drawing.Point(12, 29);
            this.SelectFolder.Name = "SelectFolder";
            this.SelectFolder.Size = new System.Drawing.Size(75, 23);
            this.SelectFolder.TabIndex = 3;
            this.SelectFolder.Text = "选择文件夹";
            this.SelectFolder.UseVisualStyleBackColor = true;
            this.SelectFolder.Click += new System.EventHandler(this.SelectFolder_Click);
            // 
            // Form1
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 12F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(462, 113);
            this.Controls.Add(this.SelectFolder);
            this.Controls.Add(this.FolderPath);
            this.Controls.Add(this.DisableCapture);
            this.Controls.Add(this.EnableCapture);
            this.FormBorderStyle = System.Windows.Forms.FormBorderStyle.FixedSingle;
            this.MaximizeBox = false;
            this.Name = "Form1";
            this.Text = "截屏工具";
            this.FormClosed += new System.Windows.Forms.FormClosedEventHandler(this.Form1_FormClosed);
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Button EnableCapture;
        private System.Windows.Forms.Button DisableCapture;
        private System.Windows.Forms.TextBox FolderPath;
        private System.Windows.Forms.Button SelectFolder;


    }
}
