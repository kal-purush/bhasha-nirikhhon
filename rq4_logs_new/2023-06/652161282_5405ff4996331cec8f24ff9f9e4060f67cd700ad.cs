namespace Recipe_Organizer_PRN211.Authentication
{
	partial class ChangePasswordAdmin
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
			labelcuibap = new Label();
			txtConfirm = new TextBox();
			btnUserProfile = new Button();
			label2 = new Label();
			txtNewPassword = new TextBox();
			label3 = new Label();
			btnChangePassword = new Button();
			txtOldPassword = new TextBox();
			label5 = new Label();
			SuspendLayout();
			// 
			// labelcuibap
			// 
			labelcuibap.AutoSize = true;
			labelcuibap.Font = new Font("Montserrat", 11.25F, FontStyle.Bold, GraphicsUnit.Point);
			labelcuibap.Location = new Point(296, 274);
			labelcuibap.Name = "labelcuibap";
			labelcuibap.Size = new Size(154, 21);
			labelcuibap.TabIndex = 37;
			labelcuibap.Text = "Confirm password";
			// 
			// txtConfirm
			// 
			txtConfirm.Location = new Point(296, 298);
			txtConfirm.Name = "txtConfirm";
			txtConfirm.PasswordChar = '*';
			txtConfirm.Size = new Size(245, 23);
			txtConfirm.TabIndex = 36;
			// 
			// btnUserProfile
			// 
			btnUserProfile.BackgroundImageLayout = ImageLayout.Stretch;
			btnUserProfile.Font = new Font("Montserrat Medium", 15.75F, FontStyle.Bold, GraphicsUnit.Point);
			btnUserProfile.ForeColor = Color.Black;
			btnUserProfile.Location = new Point(15, 19);
			btnUserProfile.Name = "btnUserProfile";
			btnUserProfile.Size = new Size(205, 51);
			btnUserProfile.TabIndex = 35;
			btnUserProfile.Text = "Admin profile";
			btnUserProfile.UseVisualStyleBackColor = true;
			btnUserProfile.Click += btnUserProfile_Click_1;
			// 
			// label2
			// 
			label2.AutoSize = true;
			label2.Font = new Font("Montserrat", 11.25F, FontStyle.Bold, GraphicsUnit.Point);
			label2.Location = new Point(296, 198);
			label2.Name = "label2";
			label2.Size = new Size(124, 21);
			label2.TabIndex = 34;
			label2.Text = "New password";
			// 
			// txtNewPassword
			// 
			txtNewPassword.Location = new Point(296, 233);
			txtNewPassword.Name = "txtNewPassword";
			txtNewPassword.PasswordChar = '*';
			txtNewPassword.Size = new Size(245, 23);
			txtNewPassword.TabIndex = 33;
			// 
			// label3
			// 
			label3.AutoSize = true;
			label3.Font = new Font("Montserrat", 11.25F, FontStyle.Bold, GraphicsUnit.Point);
			label3.Location = new Point(296, 136);
			label3.Name = "label3";
			label3.Size = new Size(117, 21);
			label3.TabIndex = 32;
			label3.Text = "Old password";
			// 
			// btnChangePassword
			// 
			btnChangePassword.Font = new Font("Montserrat", 11.25F, FontStyle.Bold, GraphicsUnit.Point);
			btnChangePassword.Location = new Point(296, 344);
			btnChangePassword.Name = "btnChangePassword";
			btnChangePassword.Size = new Size(245, 36);
			btnChangePassword.TabIndex = 31;
			btnChangePassword.Text = "Change password";
			btnChangePassword.UseVisualStyleBackColor = true;
			btnChangePassword.Click += btnChangePassword_Click_1;
			// 
			// txtOldPassword
			// 
			txtOldPassword.Location = new Point(296, 160);
			txtOldPassword.Name = "txtOldPassword";
			txtOldPassword.PasswordChar = '*';
			txtOldPassword.Size = new Size(245, 23);
			txtOldPassword.TabIndex = 30;
			// 
			// label5
			// 
			label5.AutoSize = true;
			label5.Font = new Font("Montserrat Black", 20.25F, FontStyle.Bold, GraphicsUnit.Point);
			label5.ForeColor = Color.FromArgb(192, 0, 0);
			label5.Location = new Point(282, 59);
			label5.Name = "label5";
			label5.Size = new Size(278, 37);
			label5.TabIndex = 29;
			label5.Text = "Change password";
			label5.TextAlign = ContentAlignment.TopCenter;
			// 
			// ChangePasswordAdmin
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
			Name = "ChangePasswordAdmin";
			Text = "ChangePasswordAdmin";
			Load += ChangePasswordAdmin_Load;
			ResumeLayout(false);
			PerformLayout();
		}

		#endregion

		private Label labelcuibap;
		private TextBox txtConfirm;
		private Button btnUserProfile;
		private Label label2;
		private TextBox txtNewPassword;
		private Label label3;
		private Button btnChangePassword;
		private TextBox txtOldPassword;
		private Label label5;
	}
}