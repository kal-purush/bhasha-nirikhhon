namespace Recipe_Organizer_PRN211.Authentication
{
    partial class ChangePassword
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
            label3 = new Label();
            btnChangePassword = new Button();
            txtOldPassword = new TextBox();
            label5 = new Label();
            label2 = new Label();
            txtNewPassword = new TextBox();
            btnUserProfile = new Button();
            labelcuibap = new Label();
            txtConfirm = new TextBox();
            SuspendLayout();
            // 
            // label3
            // 
            label3.AutoSize = true;
            label3.Font = new Font("Montserrat", 11.25F, FontStyle.Bold, GraphicsUnit.Point);
            label3.Location = new Point(293, 129);
            label3.Name = "label3";
            label3.Size = new Size(117, 21);
            label3.TabIndex = 23;
            label3.Text = "Old password";
            // 
            // btnChangePassword
            // 
            btnChangePassword.Font = new Font("Montserrat", 11.25F, FontStyle.Bold, GraphicsUnit.Point);
            btnChangePassword.Location = new Point(293, 337);
            btnChangePassword.Name = "btnChangePassword";
            btnChangePassword.Size = new Size(245, 36);
            btnChangePassword.TabIndex = 21;
            btnChangePassword.Text = "Change password";
            btnChangePassword.UseVisualStyleBackColor = true;
            btnChangePassword.Click += btnChangePassword_Click;
            // 
            // txtOldPassword
            // 
            txtOldPassword.Location = new Point(293, 153);
            txtOldPassword.Name = "txtOldPassword";
            txtOldPassword.PasswordChar = '*';
            txtOldPassword.Size = new Size(245, 23);
            txtOldPassword.TabIndex = 19;
            // 
            // label5
            // 
            label5.AutoSize = true;
            label5.Font = new Font("Montserrat Black", 20.25F, FontStyle.Bold, GraphicsUnit.Point);
            label5.ForeColor = Color.FromArgb(192, 0, 0);
            label5.Location = new Point(279, 52);
            label5.Name = "label5";
            label5.Size = new Size(278, 37);
            label5.TabIndex = 18;
            label5.Text = "Change password";
            label5.TextAlign = ContentAlignment.TopCenter;
            // 
            // label2
            // 
            label2.AutoSize = true;
            label2.Font = new Font("Montserrat", 11.25F, FontStyle.Bold, GraphicsUnit.Point);
            label2.Location = new Point(293, 191);
            label2.Name = "label2";
            label2.Size = new Size(124, 21);
            label2.TabIndex = 25;
            label2.Text = "New password";
            // 
            // txtNewPassword
            // 
            txtNewPassword.Location = new Point(293, 226);
            txtNewPassword.Name = "txtNewPassword";
            txtNewPassword.PasswordChar = '*';
            txtNewPassword.Size = new Size(245, 23);
            txtNewPassword.TabIndex = 24;
            // 
            // btnUserProfile
            // 
            btnUserProfile.Font = new Font("Montserrat Medium", 15.75F, FontStyle.Bold, GraphicsUnit.Point);
            btnUserProfile.Location = new Point(12, 12);
            btnUserProfile.Name = "btnUserProfile";
            btnUserProfile.Size = new Size(151, 51);
            btnUserProfile.TabIndex = 26;
            btnUserProfile.Text = "User profile";
            btnUserProfile.UseVisualStyleBackColor = true;
            btnUserProfile.Click += btnUserProfile_Click;
            // 
            // labelcuibap
            // 
            labelcuibap.AutoSize = true;
            labelcuibap.Font = new Font("Montserrat", 11.25F, FontStyle.Bold, GraphicsUnit.Point);
            labelcuibap.Location = new Point(293, 267);
            labelcuibap.Name = "labelcuibap";
            labelcuibap.Size = new Size(154, 21);
            labelcuibap.TabIndex = 28;
            labelcuibap.Text = "Confirm password";
            // 
            // txtConfirm
            // 
            txtConfirm.Location = new Point(293, 291);
            txtConfirm.Name = "txtConfirm";
            txtConfirm.PasswordChar = '*';
            txtConfirm.Size = new Size(245, 23);
            txtConfirm.TabIndex = 27;
            // 
            // ChangePassword
            // 
            AutoScaleDimensions = new SizeF(7F, 15F);
            AutoScaleMode = AutoScaleMode.Font;
            ClientSize = new Size(800, 450);
            Controls.Add(labelcuibap);
            Controls.Add(txtConfirm);
            Controls.Add(btnUserProfile);
            Controls.Add(label2);
            Controls.Add(txtNewPassword);
            Controls.Add(label3);
            Controls.Add(btnChangePassword);
            Controls.Add(txtOldPassword);
            Controls.Add(label5);
            Name = "ChangePassword";
            Text = "ChangePassword";
            ResumeLayout(false);
            PerformLayout();
        }

        #endregion

        private Label label3;
        private Button btnChangePassword;
        private TextBox txtOldPassword;
        private Label label5;
        private Label label2;
        private TextBox txtNewPassword;
        private Button btnUserProfile;
        private Label labelcuibap;
        private TextBox txtConfirm;
    }
}