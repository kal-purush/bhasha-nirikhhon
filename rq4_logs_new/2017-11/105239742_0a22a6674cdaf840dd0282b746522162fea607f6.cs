namespace ItemGenerator
{
    partial class SettingsForm
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
            this.txtItemPath = new System.Windows.Forms.TextBox();
            this.txtSettingsPath = new System.Windows.Forms.TextBox();
            this.btnItemBrowse = new System.Windows.Forms.Button();
            this.btnSettingsBrowse = new System.Windows.Forms.Button();
            this.itemFileDialog = new System.Windows.Forms.SaveFileDialog();
            this.settingsFileDialog = new System.Windows.Forms.SaveFileDialog();
            this.btnAccept = new System.Windows.Forms.Button();
            this.btnCancel = new System.Windows.Forms.Button();
            this.label1 = new System.Windows.Forms.Label();
            this.label2 = new System.Windows.Forms.Label();
            this.SuspendLayout();
            // 
            // txtItemPath
            // 
            this.txtItemPath.Location = new System.Drawing.Point(58, 50);
            this.txtItemPath.Name = "txtItemPath";
            this.txtItemPath.Size = new System.Drawing.Size(162, 20);
            this.txtItemPath.TabIndex = 0;
            this.txtItemPath.TextAlign = System.Windows.Forms.HorizontalAlignment.Right;
            // 
            // txtSettingsPath
            // 
            this.txtSettingsPath.Location = new System.Drawing.Point(58, 100);
            this.txtSettingsPath.Name = "txtSettingsPath";
            this.txtSettingsPath.Size = new System.Drawing.Size(162, 20);
            this.txtSettingsPath.TabIndex = 1;
            this.txtSettingsPath.TextAlign = System.Windows.Forms.HorizontalAlignment.Right;
            // 
            // btnItemBrowse
            // 
            this.btnItemBrowse.Location = new System.Drawing.Point(226, 50);
            this.btnItemBrowse.Name = "btnItemBrowse";
            this.btnItemBrowse.Size = new System.Drawing.Size(75, 23);
            this.btnItemBrowse.TabIndex = 2;
            this.btnItemBrowse.Text = "Browse";
            this.btnItemBrowse.UseVisualStyleBackColor = true;
            this.btnItemBrowse.Click += new System.EventHandler(this.btnItemBrowse_Click);
            // 
            // btnSettingsBrowse
            // 
            this.btnSettingsBrowse.Location = new System.Drawing.Point(226, 98);
            this.btnSettingsBrowse.Name = "btnSettingsBrowse";
            this.btnSettingsBrowse.Size = new System.Drawing.Size(75, 23);
            this.btnSettingsBrowse.TabIndex = 3;
            this.btnSettingsBrowse.Text = "Browse";
            this.btnSettingsBrowse.UseVisualStyleBackColor = true;
            this.btnSettingsBrowse.Click += new System.EventHandler(this.btnSettingsBrowse_Click);
            // 
            // itemFileDialog
            // 
            this.itemFileDialog.DefaultExt = "json";
            this.itemFileDialog.InitialDirectory = "./";
            this.itemFileDialog.OverwritePrompt = false;
            this.itemFileDialog.Title = "Item File";
            // 
            // settingsFileDialog
            // 
            this.settingsFileDialog.DefaultExt = "txt";
            this.settingsFileDialog.InitialDirectory = "./";
            this.settingsFileDialog.OverwritePrompt = false;
            this.settingsFileDialog.Title = "Settings File";
            // 
            // btnAccept
            // 
            this.btnAccept.Location = new System.Drawing.Point(58, 143);
            this.btnAccept.Name = "btnAccept";
            this.btnAccept.Size = new System.Drawing.Size(75, 23);
            this.btnAccept.TabIndex = 6;
            this.btnAccept.Text = "Apply";
            this.btnAccept.UseVisualStyleBackColor = true;
            this.btnAccept.Click += new System.EventHandler(this.btnAccept_Click);
            // 
            // btnCancel
            // 
            this.btnCancel.Location = new System.Drawing.Point(145, 143);
            this.btnCancel.Name = "btnCancel";
            this.btnCancel.Size = new System.Drawing.Size(75, 23);
            this.btnCancel.TabIndex = 7;
            this.btnCancel.Text = "Cancel";
            this.btnCancel.UseVisualStyleBackColor = true;
            this.btnCancel.Click += new System.EventHandler(this.btnCancel_Click);
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Location = new System.Drawing.Point(12, 34);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(71, 13);
            this.label1.TabIndex = 8;
            this.label1.Text = "Item File Path";
            // 
            // label2
            // 
            this.label2.AutoSize = true;
            this.label2.Location = new System.Drawing.Point(13, 81);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(89, 13);
            this.label2.TabIndex = 9;
            this.label2.Text = "Settings File Path";
            // 
            // SettingsForm
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(314, 190);
            this.Controls.Add(this.label2);
            this.Controls.Add(this.label1);
            this.Controls.Add(this.btnCancel);
            this.Controls.Add(this.btnAccept);
            this.Controls.Add(this.btnSettingsBrowse);
            this.Controls.Add(this.btnItemBrowse);
            this.Controls.Add(this.txtSettingsPath);
            this.Controls.Add(this.txtItemPath);
            this.Name = "SettingsForm";
            this.Text = "Settings";
            this.Load += new System.EventHandler(this.SaveForm_Load);
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.TextBox txtItemPath;
        private System.Windows.Forms.TextBox txtSettingsPath;
        private System.Windows.Forms.Button btnItemBrowse;
        private System.Windows.Forms.Button btnSettingsBrowse;
        private System.Windows.Forms.SaveFileDialog itemFileDialog;
        private System.Windows.Forms.SaveFileDialog settingsFileDialog;
        private System.Windows.Forms.Button btnAccept;
        private System.Windows.Forms.Button btnCancel;
        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.Label label2;
    }
}