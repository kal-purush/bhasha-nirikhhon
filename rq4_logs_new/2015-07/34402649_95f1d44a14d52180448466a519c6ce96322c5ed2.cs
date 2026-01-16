namespace SupplyChainManagementUI
{
    partial class OutputXmlForm
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
            this.xmlText = new System.Windows.Forms.RichTextBox();
            this.saveButton = new System.Windows.Forms.Button();
            this.SuspendLayout();
            // 
            // xmlText
            // 
            this.xmlText.Location = new System.Drawing.Point(12, 12);
            this.xmlText.Name = "xmlText";
            this.xmlText.Size = new System.Drawing.Size(738, 329);
            this.xmlText.TabIndex = 0;
            this.xmlText.Text = "";
            // 
            // saveButton
            // 
            this.saveButton.Location = new System.Drawing.Point(637, 347);
            this.saveButton.Name = "saveButton";
            this.saveButton.Size = new System.Drawing.Size(113, 23);
            this.saveButton.TabIndex = 1;
            this.saveButton.Text = "Save XML file";
            this.saveButton.UseVisualStyleBackColor = true;
            this.saveButton.Click += new System.EventHandler(this.saveButton_Click);
            // 
            // OutputXmlForm
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(762, 382);
            this.Controls.Add(this.saveButton);
            this.Controls.Add(this.xmlText);
            this.Name = "OutputXmlForm";
            this.Text = "OutputXmlForm";
            this.ResumeLayout(false);

        }

        #endregion

        private System.Windows.Forms.RichTextBox xmlText;
        private System.Windows.Forms.Button saveButton;
    }
}