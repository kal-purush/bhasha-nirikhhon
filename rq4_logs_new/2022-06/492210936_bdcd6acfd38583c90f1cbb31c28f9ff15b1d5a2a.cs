namespace Simsprojekat.View.AdministratorView
{
    partial class SectionUpdateForm
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
            this.invalidInfoLabel = new System.Windows.Forms.Label();
            this.submitBtn = new System.Windows.Forms.Button();
            this.distanceTextBox = new System.Windows.Forms.TextBox();
            this.label3 = new System.Windows.Forms.Label();
            this.SuspendLayout();
            // 
            // invalidInfoLabel
            // 
            this.invalidInfoLabel.AutoSize = true;
            this.invalidInfoLabel.ForeColor = System.Drawing.Color.Crimson;
            this.invalidInfoLabel.Location = new System.Drawing.Point(60, 108);
            this.invalidInfoLabel.Name = "invalidInfoLabel";
            this.invalidInfoLabel.Size = new System.Drawing.Size(165, 15);
            this.invalidInfoLabel.TabIndex = 72;
            this.invalidInfoLabel.Text = "Entered information is invalid!";
            this.invalidInfoLabel.Visible = false;
            // 
            // submitBtn
            // 
            this.submitBtn.Location = new System.Drawing.Point(95, 137);
            this.submitBtn.Name = "submitBtn";
            this.submitBtn.Size = new System.Drawing.Size(92, 44);
            this.submitBtn.TabIndex = 71;
            this.submitBtn.Text = "Submit";
            this.submitBtn.UseVisualStyleBackColor = true;
            this.submitBtn.Click += new System.EventHandler(this.submitBtn_Click);
            // 
            // distanceTextBox
            // 
            this.distanceTextBox.Location = new System.Drawing.Point(130, 51);
            this.distanceTextBox.Name = "distanceTextBox";
            this.distanceTextBox.Size = new System.Drawing.Size(100, 23);
            this.distanceTextBox.TabIndex = 70;
            // 
            // label3
            // 
            this.label3.AutoSize = true;
            this.label3.Location = new System.Drawing.Point(58, 54);
            this.label3.Name = "label3";
            this.label3.Size = new System.Drawing.Size(58, 15);
            this.label3.TabIndex = 68;
            this.label3.Text = "Distance :";
            // 
            // SectionUpdateForm
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(7F, 15F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(282, 242);
            this.Controls.Add(this.invalidInfoLabel);
            this.Controls.Add(this.submitBtn);
            this.Controls.Add(this.distanceTextBox);
            this.Controls.Add(this.label3);
            this.Name = "SectionUpdateForm";
            this.Text = "SectionUpdateForm";
            this.Load += new System.EventHandler(this.SectionUpdateForm_Load);
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Label invalidInfoLabel;
        private System.Windows.Forms.Button submitBtn;
        private System.Windows.Forms.TextBox distanceTextBox;
        private System.Windows.Forms.Label label3;
    }
}