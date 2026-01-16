
namespace ServiceHelper
{
    partial class form1
    {
        /// <summary>
        /// 必需的设计器变量。
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// 清理所有正在使用的资源。
        /// </summary>
        /// <param name="disposing">如果应释放托管资源，为 true；否则为 false。</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Windows 窗体设计器生成的代码

        /// <summary>
        /// 设计器支持所需的方法 - 不要修改
        /// 使用代码编辑器修改此方法的内容。
        /// </summary>
        private void InitializeComponent()
        {
            this.InstallServiceButton = new System.Windows.Forms.Button();
            this.StartServiceButton = new System.Windows.Forms.Button();
            this.StopServiceButton = new System.Windows.Forms.Button();
            this.UninstallServiceButton = new System.Windows.Forms.Button();
            this.SelectFileButton = new System.Windows.Forms.Button();
            this.openFileDialog = new System.Windows.Forms.OpenFileDialog();
            this.SelectFilePathRichTextBox = new System.Windows.Forms.RichTextBox();
            this.ServiceStatusLabel = new System.Windows.Forms.Label();
            this.SuspendLayout();
            // 
            // InstallServiceButton
            // 
            this.InstallServiceButton.Location = new System.Drawing.Point(52, 99);
            this.InstallServiceButton.Name = "InstallServiceButton";
            this.InstallServiceButton.Size = new System.Drawing.Size(94, 38);
            this.InstallServiceButton.TabIndex = 0;
            this.InstallServiceButton.Text = "安装服务";
            this.InstallServiceButton.UseVisualStyleBackColor = true;
            this.InstallServiceButton.Click += new System.EventHandler(this.InstallServiceButton_Click);
            // 
            // StartServiceButton
            // 
            this.StartServiceButton.Location = new System.Drawing.Point(211, 99);
            this.StartServiceButton.Name = "StartServiceButton";
            this.StartServiceButton.Size = new System.Drawing.Size(94, 38);
            this.StartServiceButton.TabIndex = 1;
            this.StartServiceButton.Text = "启动服务";
            this.StartServiceButton.UseVisualStyleBackColor = true;
            this.StartServiceButton.Click += new System.EventHandler(this.StartServiceButton_Click);
            // 
            // StopServiceButton
            // 
            this.StopServiceButton.Location = new System.Drawing.Point(392, 99);
            this.StopServiceButton.Name = "StopServiceButton";
            this.StopServiceButton.Size = new System.Drawing.Size(94, 38);
            this.StopServiceButton.TabIndex = 2;
            this.StopServiceButton.Text = "停止服务";
            this.StopServiceButton.UseVisualStyleBackColor = true;
            this.StopServiceButton.Click += new System.EventHandler(this.StopServiceButton_Click);
            // 
            // UninstallServiceButton
            // 
            this.UninstallServiceButton.Location = new System.Drawing.Point(572, 99);
            this.UninstallServiceButton.Name = "UninstallServiceButton";
            this.UninstallServiceButton.Size = new System.Drawing.Size(94, 38);
            this.UninstallServiceButton.TabIndex = 3;
            this.UninstallServiceButton.Text = "卸载服务";
            this.UninstallServiceButton.UseVisualStyleBackColor = true;
            this.UninstallServiceButton.Click += new System.EventHandler(this.UnInstallServiceButton_Click);
            // 
            // SelectFileButton
            // 
            this.SelectFileButton.Location = new System.Drawing.Point(68, 32);
            this.SelectFileButton.Name = "SelectFileButton";
            this.SelectFileButton.Size = new System.Drawing.Size(111, 24);
            this.SelectFileButton.TabIndex = 4;
            this.SelectFileButton.Text = "选择安装文件";
            this.SelectFileButton.UseVisualStyleBackColor = true;
            this.SelectFileButton.Click += new System.EventHandler(this.SelectFileButton_Click);
            // 
            // openFileDialog
            // 
            this.openFileDialog.Filter = "exe文件(*.exe)|*.exe|所有文件(*.*)|*.*";
            this.openFileDialog.RestoreDirectory = true;
            this.openFileDialog.Title = "选择安装文件";
            // 
            // SelectFilePathRichTextBox
            // 
            this.SelectFilePathRichTextBox.Location = new System.Drawing.Point(227, 12);
            this.SelectFilePathRichTextBox.Name = "SelectFilePathRichTextBox";
            this.SelectFilePathRichTextBox.ReadOnly = true;
            this.SelectFilePathRichTextBox.Size = new System.Drawing.Size(378, 52);
            this.SelectFilePathRichTextBox.TabIndex = 6;
            this.SelectFilePathRichTextBox.Text = "";
            // 
            // ServiceStatusLabel
            // 
            this.ServiceStatusLabel.AutoSize = true;
            this.ServiceStatusLabel.Location = new System.Drawing.Point(634, 32);
            this.ServiceStatusLabel.Name = "ServiceStatusLabel";
            this.ServiceStatusLabel.Size = new System.Drawing.Size(0, 12);
            this.ServiceStatusLabel.TabIndex = 8;
            // 
            // form1
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 12F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(723, 158);
            this.Controls.Add(this.ServiceStatusLabel);
            this.Controls.Add(this.SelectFilePathRichTextBox);
            this.Controls.Add(this.SelectFileButton);
            this.Controls.Add(this.UninstallServiceButton);
            this.Controls.Add(this.StopServiceButton);
            this.Controls.Add(this.StartServiceButton);
            this.Controls.Add(this.InstallServiceButton);
            this.Name = "form1";
            this.Text = "服务相关操作";
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Button InstallServiceButton;
        private System.Windows.Forms.Button StartServiceButton;
        private System.Windows.Forms.Button StopServiceButton;
        private System.Windows.Forms.Button UninstallServiceButton;
        private System.Windows.Forms.Button SelectFileButton;
        private System.Windows.Forms.OpenFileDialog openFileDialog;
        private System.Windows.Forms.RichTextBox SelectFilePathRichTextBox;
        private System.Windows.Forms.Label ServiceStatusLabel;
    }
}
