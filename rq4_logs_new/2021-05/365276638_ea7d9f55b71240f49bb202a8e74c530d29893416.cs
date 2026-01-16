
namespace BlightnautsDialogue
{
    partial class MapList
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
            System.Windows.Forms.Button buttonCancel;
            System.Windows.Forms.Button buttonOk;
            this.textBoxMaps = new System.Windows.Forms.TextBox();
            labelInfo = new System.Windows.Forms.Label();
            buttonCancel = new System.Windows.Forms.Button();
            buttonOk = new System.Windows.Forms.Button();
            this.SuspendLayout();
            // 
            // labelInfo
            // 
            labelInfo.AutoSize = true;
            labelInfo.Location = new System.Drawing.Point(10, 14);
            labelInfo.Name = "labelInfo";
            labelInfo.Size = new System.Drawing.Size(456, 52);
            labelInfo.TabIndex = 0;
            labelInfo.Text = "Insert the names of the maps included in your mod that you want to use the dialog" +
    "ue system on.\r\nThe names must be separated by a semi-colon (;).\r\nExample:\r\nMap1;" +
    "Map2;Map3";
            // 
            // buttonCancel
            // 
            buttonCancel.DialogResult = System.Windows.Forms.DialogResult.Cancel;
            buttonCancel.Location = new System.Drawing.Point(392, 99);
            buttonCancel.Name = "buttonCancel";
            buttonCancel.Size = new System.Drawing.Size(75, 23);
            buttonCancel.TabIndex = 2;
            buttonCancel.Text = "Cancel";
            buttonCancel.UseVisualStyleBackColor = true;
            buttonCancel.Click += new System.EventHandler(this.buttonCancel_Click);
            // 
            // buttonOk
            // 
            buttonOk.Location = new System.Drawing.Point(311, 99);
            buttonOk.Name = "buttonOk";
            buttonOk.Size = new System.Drawing.Size(75, 23);
            buttonOk.TabIndex = 1;
            buttonOk.Text = "Confirm";
            buttonOk.UseVisualStyleBackColor = true;
            buttonOk.Click += new System.EventHandler(this.buttonOk_Click);
            // 
            // textBoxMaps
            // 
            this.textBoxMaps.Location = new System.Drawing.Point(13, 69);
            this.textBoxMaps.Name = "textBoxMaps";
            this.textBoxMaps.Size = new System.Drawing.Size(454, 20);
            this.textBoxMaps.TabIndex = 0;
            // 
            // MapList
            // 
            this.AcceptButton = buttonOk;
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.CancelButton = buttonCancel;
            this.ClientSize = new System.Drawing.Size(479, 134);
            this.Controls.Add(buttonOk);
            this.Controls.Add(buttonCancel);
            this.Controls.Add(this.textBoxMaps);
            this.Controls.Add(labelInfo);
            this.FormBorderStyle = System.Windows.Forms.FormBorderStyle.FixedToolWindow;
            this.MaximizeBox = false;
            this.MinimizeBox = false;
            this.Name = "MapList";
            this.ShowIcon = false;
            this.ShowInTaskbar = false;
            this.StartPosition = System.Windows.Forms.FormStartPosition.CenterParent;
            this.Text = "Map List";
            this.TopMost = true;
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.TextBox textBoxMaps;
    }
}