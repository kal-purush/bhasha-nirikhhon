namespace Recipe_Organizer_PRN211.Authentication
{
	partial class UserProfile
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
			label5 = new Label();
			button1 = new Button();
			button2 = new Button();
			button3 = new Button();
			button4 = new Button();
			txtHello = new Label();
			SuspendLayout();
			// 
			// label5
			// 
			label5.AutoSize = true;
			label5.Font = new Font("Montserrat Black", 20.25F, FontStyle.Bold, GraphicsUnit.Point);
			label5.ForeColor = Color.FromArgb(192, 0, 0);
			label5.Location = new Point(318, 34);
			label5.Name = "label5";
			label5.Size = new Size(190, 37);
			label5.TabIndex = 10;
			label5.Text = "User Profile";
			// 
			// button1
			// 
			button1.Location = new Point(49, 125);
			button1.Name = "button1";
			button1.Size = new Size(159, 37);
			button1.TabIndex = 11;
			button1.Text = "button1";
			button1.UseVisualStyleBackColor = true;
			// 
			// button2
			// 
			button2.Location = new Point(49, 184);
			button2.Name = "button2";
			button2.Size = new Size(159, 37);
			button2.TabIndex = 12;
			button2.Text = "button2";
			button2.UseVisualStyleBackColor = true;
			// 
			// button3
			// 
			button3.Location = new Point(49, 245);
			button3.Name = "button3";
			button3.Size = new Size(159, 37);
			button3.TabIndex = 13;
			button3.Text = "button3";
			button3.UseVisualStyleBackColor = true;
			// 
			// button4
			// 
			button4.Location = new Point(49, 303);
			button4.Name = "button4";
			button4.Size = new Size(159, 37);
			button4.TabIndex = 14;
			button4.Text = "button4";
			button4.UseVisualStyleBackColor = true;
			// 
			// txtHello
			// 
			txtHello.AutoSize = true;
			txtHello.Font = new Font("Montserrat", 11.25F, FontStyle.Bold, GraphicsUnit.Point);
			txtHello.Location = new Point(49, 74);
			txtHello.Name = "txtHello";
			txtHello.Size = new Size(51, 21);
			txtHello.TabIndex = 15;
			txtHello.Text = "Hello";
			// 
			// UserProfile
			// 
			AutoScaleDimensions = new SizeF(7F, 15F);
			AutoScaleMode = AutoScaleMode.Font;
			ClientSize = new Size(800, 450);
			Controls.Add(txtHello);
			Controls.Add(button4);
			Controls.Add(button3);
			Controls.Add(button2);
			Controls.Add(button1);
			Controls.Add(label5);
			Name = "UserProfile";
			Text = "UserProfile";
			ResumeLayout(false);
			PerformLayout();
		}

		#endregion

		private Label label5;
		private Button button1;
		private Button button2;
		private Button button3;
		private Button button4;
		private Label txtHello;
	}
}