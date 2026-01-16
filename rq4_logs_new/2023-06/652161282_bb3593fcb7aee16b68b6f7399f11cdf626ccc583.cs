namespace Recipe_Organizer_PRN211.Authentication
{
	partial class Search
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
			label1 = new Label();
			label2 = new Label();
			BtnSearchPage = new Button();
			textBox1 = new TextBox();
			SuspendLayout();
			// 
			// label1
			// 
			label1.AutoSize = true;
			label1.Location = new Point(216, 56);
			label1.Name = "label1";
			label1.Size = new Size(50, 20);
			label1.TabIndex = 0;
			label1.Text = "label1";
			label1.Click += label1_Click;
			// 
			// label2
			// 
			label2.AutoSize = true;
			label2.Location = new Point(323, 56);
			label2.Name = "label2";
			label2.Size = new Size(50, 20);
			label2.TabIndex = 1;
			label2.Text = "label2";
			// 
			// BtnSearchPage
			// 
			BtnSearchPage.Location = new Point(152, 118);
			BtnSearchPage.Name = "BtnSearchPage";
			BtnSearchPage.Size = new Size(94, 29);
			BtnSearchPage.TabIndex = 2;
			BtnSearchPage.Text = "Search";
			BtnSearchPage.UseVisualStyleBackColor = true;
			// 
			// textBox1
			// 
			textBox1.Location = new Point(279, 118);
			textBox1.Name = "textBox1";
			textBox1.Size = new Size(324, 27);
			textBox1.TabIndex = 3;
			// 
			// Search
			// 
			AutoScaleDimensions = new SizeF(8F, 20F);
			AutoScaleMode = AutoScaleMode.Font;
			ClientSize = new Size(800, 450);
			Controls.Add(textBox1);
			Controls.Add(BtnSearchPage);
			Controls.Add(label2);
			Controls.Add(label1);
			Name = "Search";
			Text = "Search";
			ResumeLayout(false);
			PerformLayout();
		}

		#endregion

		private Label label1;
		private Label label2;
		private Button BtnSearchPage;
		private TextBox textBox1;
	}
}