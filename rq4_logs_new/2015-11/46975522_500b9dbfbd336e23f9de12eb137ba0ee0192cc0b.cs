namespace NotificationAgent.UI
{
    partial class NotificationPopup
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
            this.imageView = new System.Windows.Forms.PictureBox();
            this.titleView = new System.Windows.Forms.TextBox();
            this.descriptionView = new System.Windows.Forms.TextBox();
            ((System.ComponentModel.ISupportInitialize)(this.imageView)).BeginInit();
            this.SuspendLayout();
            // 
            // imageView
            // 
            this.imageView.BackColor = System.Drawing.SystemColors.Control;
            this.imageView.Location = new System.Drawing.Point(7, 3);
            this.imageView.Name = "imageView";
            this.imageView.Size = new System.Drawing.Size(64, 64);
            this.imageView.TabIndex = 0;
            this.imageView.TabStop = false;
            // 
            // titleView
            // 
            this.titleView.BackColor = System.Drawing.SystemColors.Control;
            this.titleView.BorderStyle = System.Windows.Forms.BorderStyle.None;
            this.titleView.Enabled = false;
            this.titleView.Font = new System.Drawing.Font("Microsoft Sans Serif", 20F, System.Drawing.FontStyle.Bold);
            this.titleView.Location = new System.Drawing.Point(77, 3);
            this.titleView.MaxLength = 44;
            this.titleView.Multiline = true;
            this.titleView.Name = "titleView";
            this.titleView.ReadOnly = true;
            this.titleView.Size = new System.Drawing.Size(354, 64);
            this.titleView.TabIndex = 1;
            this.titleView.Text = "Title";
            // 
            // descriptionView
            // 
            this.descriptionView.BackColor = System.Drawing.SystemColors.Control;
            this.descriptionView.BorderStyle = System.Windows.Forms.BorderStyle.None;
            this.descriptionView.Enabled = false;
            this.descriptionView.Font = new System.Drawing.Font("Microsoft Sans Serif", 14F);
            this.descriptionView.Location = new System.Drawing.Point(7, 73);
            this.descriptionView.MaxLength = 84;
            this.descriptionView.Multiline = true;
            this.descriptionView.Name = "descriptionView";
            this.descriptionView.ReadOnly = true;
            this.descriptionView.Size = new System.Drawing.Size(424, 48);
            this.descriptionView.TabIndex = 2;
            this.descriptionView.Text = "Description";
            // 
            // NotificationPopup
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(436, 127);
            this.Controls.Add(this.descriptionView);
            this.Controls.Add(this.titleView);
            this.Controls.Add(this.imageView);
            this.FormBorderStyle = System.Windows.Forms.FormBorderStyle.None;
            this.Name = "NotificationPopup";
            this.Text = "NotificationPopup";
            ((System.ComponentModel.ISupportInitialize)(this.imageView)).EndInit();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.PictureBox imageView;
        private System.Windows.Forms.TextBox titleView;
        private System.Windows.Forms.TextBox descriptionView;
    }
}