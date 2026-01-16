
namespace BlightnautsDialogue
{
    partial class LegacyImportWindow
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
            System.Windows.Forms.Label labelInfo;
            System.Windows.Forms.Label labelCharacter;
            System.Windows.Forms.Label labelFile;
            System.Windows.Forms.Button buttonBrowse;
            System.Windows.Forms.Button buttonOk;
            this.dropdownCharacters = new System.Windows.Forms.ComboBox();
            this.textBoxFile = new System.Windows.Forms.TextBox();
            this.openFileDialog = new System.Windows.Forms.OpenFileDialog();
            labelInfo = new System.Windows.Forms.Label();
            labelCharacter = new System.Windows.Forms.Label();
            labelFile = new System.Windows.Forms.Label();
            buttonBrowse = new System.Windows.Forms.Button();
            buttonOk = new System.Windows.Forms.Button();
            this.SuspendLayout();
            // 
            // dropdownCharacters
            // 
            this.dropdownCharacters.DropDownStyle = System.Windows.Forms.ComboBoxStyle.DropDownList;
            this.dropdownCharacters.FormattingEnabled = true;
            this.dropdownCharacters.Location = new System.Drawing.Point(12, 86);
            this.dropdownCharacters.Name = "dropdownCharacters";
            this.dropdownCharacters.Size = new System.Drawing.Size(217, 21);
            this.dropdownCharacters.TabIndex = 0;
            // 
            // labelInfo
            // 
            labelInfo.AutoSize = true;
            labelInfo.Location = new System.Drawing.Point(10, 9);
            labelInfo.Name = "labelInfo";
            labelInfo.Size = new System.Drawing.Size(305, 13);
            labelInfo.TabIndex = 1;
            labelInfo.Text = "Import a legacy dialogue file from the original version of the tool.";
            // 
            // labelCharacter
            // 
            labelCharacter.AutoSize = true;
            labelCharacter.Location = new System.Drawing.Point(9, 70);
            labelCharacter.Name = "labelCharacter";
            labelCharacter.Size = new System.Drawing.Size(53, 13);
            labelCharacter.TabIndex = 2;
            labelCharacter.Text = "Character";
            // 
            // labelFile
            // 
            labelFile.AutoSize = true;
            labelFile.Location = new System.Drawing.Point(10, 31);
            labelFile.Name = "labelFile";
            labelFile.Size = new System.Drawing.Size(23, 13);
            labelFile.TabIndex = 3;
            labelFile.Text = "File";
            // 
            // textBoxFile
            // 
            this.textBoxFile.Location = new System.Drawing.Point(12, 47);
            this.textBoxFile.Name = "textBoxFile";
            this.textBoxFile.Size = new System.Drawing.Size(265, 20);
            this.textBoxFile.TabIndex = 4;
            // 
            // buttonBrowse
            // 
            buttonBrowse.Location = new System.Drawing.Point(283, 46);
            buttonBrowse.Name = "buttonBrowse";
            buttonBrowse.Size = new System.Drawing.Size(27, 21);
            buttonBrowse.TabIndex = 5;
            buttonBrowse.Text = "...";
            buttonBrowse.UseVisualStyleBackColor = true;
            buttonBrowse.Click += new System.EventHandler(this.buttonBrowse_Click);
            // 
            // buttonOk
            // 
            buttonOk.DialogResult = System.Windows.Forms.DialogResult.OK;
            buttonOk.Location = new System.Drawing.Point(235, 84);
            buttonOk.Name = "buttonOk";
            buttonOk.Size = new System.Drawing.Size(75, 23);
            buttonOk.TabIndex = 6;
            buttonOk.Text = "Import";
            buttonOk.UseVisualStyleBackColor = true;
            buttonOk.Click += new System.EventHandler(this.buttonOk_Click);
            // 
            // openFileDialog
            // 
            this.openFileDialog.DefaultExt = "bnd";
            this.openFileDialog.Filter = "Blightnauts Dialogue|*.bnd";
            // 
            // LegacyImportWindow
            // 
            this.AcceptButton = buttonOk;
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(322, 118);
            this.Controls.Add(buttonOk);
            this.Controls.Add(buttonBrowse);
            this.Controls.Add(this.textBoxFile);
            this.Controls.Add(labelFile);
            this.Controls.Add(labelCharacter);
            this.Controls.Add(labelInfo);
            this.Controls.Add(this.dropdownCharacters);
            this.FormBorderStyle = System.Windows.Forms.FormBorderStyle.FixedToolWindow;
            this.MaximizeBox = false;
            this.MinimizeBox = false;
            this.Name = "LegacyImportWindow";
            this.ShowIcon = false;
            this.ShowInTaskbar = false;
            this.Text = "Import Legacy Dialogue";
            this.TopMost = true;
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.ComboBox dropdownCharacters;
        private System.Windows.Forms.TextBox textBoxFile;
        private System.Windows.Forms.OpenFileDialog openFileDialog;
    }
}