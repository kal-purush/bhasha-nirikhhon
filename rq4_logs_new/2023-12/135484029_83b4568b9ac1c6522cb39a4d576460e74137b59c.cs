namespace PartsApp
{
    partial class DismissEmployeeForm
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
            this.dismissalActionLabel = new System.Windows.Forms.Label();
            this.nameLabel = new System.Windows.Forms.Label();
            this.hireDateTimeLabel = new System.Windows.Forms.Label();
            this.accessLevelLabel = new System.Windows.Forms.Label();
            this.acceptDismissalButton = new System.Windows.Forms.Button();
            this.dismissalDateTimeLabel = new System.Windows.Forms.Label();
            this.SuspendLayout();
            // 
            // dismissalActionLabel
            // 
            this.dismissalActionLabel.AutoSize = true;
            this.dismissalActionLabel.Location = new System.Drawing.Point(19, 21);
            this.dismissalActionLabel.Name = "dismissalActionLabel";
            this.dismissalActionLabel.Size = new System.Drawing.Size(260, 16);
            this.dismissalActionLabel.TabIndex = 0;
            this.dismissalActionLabel.Text = "Подтвердите блокировку доступа для сотрудника:";
            // 
            // nameLabel
            // 
            this.nameLabel.AutoSize = true;
            this.nameLabel.Location = new System.Drawing.Point(19, 69);
            this.nameLabel.Name = "nameLabel";
            this.nameLabel.Size = new System.Drawing.Size(39, 16);
            this.nameLabel.TabIndex = 1;
            this.nameLabel.Text = "Имя: ";
            // 
            // hireDateTimeLabel
            // 
            this.hireDateTimeLabel.AutoSize = true;
            this.hireDateTimeLabel.Location = new System.Drawing.Point(19, 85);
            this.hireDateTimeLabel.Name = "hireDateTimeLabel";
            this.hireDateTimeLabel.Size = new System.Drawing.Size(127, 16);
            this.hireDateTimeLabel.TabIndex = 2;
            this.hireDateTimeLabel.Text = "Принят на работу ";
            // 
            // accessLevelLabel
            // 
            this.accessLevelLabel.AutoSize = true;
            this.accessLevelLabel.Location = new System.Drawing.Point(19, 101);
            this.accessLevelLabel.Name = "accessLevelLabel";
            this.accessLevelLabel.Size = new System.Drawing.Size(112, 16);
            this.accessLevelLabel.TabIndex = 3;
            this.accessLevelLabel.Text = "Право доступа: ";
            // 
            // acceptDismissalButton
            // 
            this.acceptDismissalButton.Location = new System.Drawing.Point(22, 184);
            this.acceptDismissalButton.Margin = new System.Windows.Forms.Padding(3, 7, 3, 0);
            this.acceptDismissalButton.Name = "acceptDismissalButton";
            this.acceptDismissalButton.Size = new System.Drawing.Size(257, 27);
            this.acceptDismissalButton.TabIndex = 4;
            this.acceptDismissalButton.Text = "Заблокировать";
            this.acceptDismissalButton.UseVisualStyleBackColor = true;
            this.acceptDismissalButton.Click += new System.EventHandler(this.OnAcceptDismissalButton);
            // 
            // dismissalDateTimeLabel
            // 
            this.dismissalDateTimeLabel.AutoSize = true;
            this.dismissalDateTimeLabel.Location = new System.Drawing.Point(19, 143);
            this.dismissalDateTimeLabel.Name = "dismissalDateTimeLabel";
            this.dismissalDateTimeLabel.Size = new System.Drawing.Size(207, 16);
            this.dismissalDateTimeLabel.TabIndex = 5;
            this.dismissalDateTimeLabel.Text = "Доступ будет заблокирован ";
            // 
            // DismissEmployeeForm
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(8F, 16F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(415, 253);
            this.Controls.Add(this.dismissalDateTimeLabel);
            this.Controls.Add(this.acceptDismissalButton);
            this.Controls.Add(this.accessLevelLabel);
            this.Controls.Add(this.hireDateTimeLabel);
            this.Controls.Add(this.nameLabel);
            this.Controls.Add(this.dismissalActionLabel);
            this.Margin = new System.Windows.Forms.Padding(3, 7, 3, 7);
            this.Name = "DismissEmployeeForm";
            this.Text = "Заблокировать доступ для сотрудника";
            this.FormBorderStyle = System.Windows.Forms.FormBorderStyle.FixedSingle;
            this.MinimizeBox = false;
            this.MaximizeBox = false;
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Label dismissalActionLabel;
        private System.Windows.Forms.Label nameLabel;
        private System.Windows.Forms.Label hireDateTimeLabel;
        private System.Windows.Forms.Label accessLevelLabel;
        private System.Windows.Forms.Button acceptDismissalButton;
        private System.Windows.Forms.Label dismissalDateTimeLabel;
    }
}