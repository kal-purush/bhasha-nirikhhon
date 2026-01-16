namespace Recipe_Organizer_PRN211.Recipe
{
	partial class RecipeDetail
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
			lbTitle = new Label();
			label1 = new Label();
			lbIngredient = new Label();
			label2 = new Label();
			lbDescription = new Label();
			lbDate = new Label();
			pictureBox1 = new PictureBox();
			((System.ComponentModel.ISupportInitialize)pictureBox1).BeginInit();
			SuspendLayout();
			// 
			// label5
			// 
			label5.AutoSize = true;
			label5.Font = new Font("Montserrat Black", 20.25F, FontStyle.Bold, GraphicsUnit.Point);
			label5.ForeColor = Color.FromArgb(192, 0, 0);
			label5.Location = new Point(27, 25);
			label5.Name = "label5";
			label5.Size = new Size(145, 47);
			label5.TabIndex = 13;
			label5.Text = "Recipe";
			// 
			// lbTitle
			// 
			lbTitle.AutoSize = true;
			lbTitle.Font = new Font("Montserrat Black", 18F, FontStyle.Regular, GraphicsUnit.Point);
			lbTitle.ForeColor = Color.FromArgb(255, 128, 0);
			lbTitle.Location = new Point(315, 65);
			lbTitle.Name = "lbTitle";
			lbTitle.Size = new Size(283, 41);
			lbTitle.TabIndex = 14;
			lbTitle.Text = "TITLE HEREEEEE";
			lbTitle.TextAlign = ContentAlignment.TopCenter;
			// 
			// label1
			// 
			label1.AutoSize = true;
			label1.Font = new Font("Segoe UI", 11F, FontStyle.Bold, GraphicsUnit.Point);
			label1.Location = new Point(27, 439);
			label1.Name = "label1";
			label1.Size = new Size(114, 25);
			label1.TabIndex = 15;
			label1.Text = "Ingredients";
			// 
			// lbIngredient
			// 
			lbIngredient.AutoSize = true;
			lbIngredient.Location = new Point(66, 476);
			lbIngredient.Name = "lbIngredient";
			lbIngredient.Size = new Size(50, 20);
			lbIngredient.TabIndex = 16;
			lbIngredient.Text = "label2";
			// 
			// label2
			// 
			label2.AutoSize = true;
			label2.Font = new Font("Segoe UI", 11F, FontStyle.Bold, GraphicsUnit.Point);
			label2.Location = new Point(27, 706);
			label2.Name = "label2";
			label2.Size = new Size(102, 25);
			label2.TabIndex = 17;
			label2.Text = "Directions";
			// 
			// lbDescription
			// 
			lbDescription.AutoSize = true;
			lbDescription.Location = new Point(66, 748);
			lbDescription.Name = "lbDescription";
			lbDescription.Size = new Size(50, 20);
			lbDescription.TabIndex = 20;
			lbDescription.Text = "label4";
			// 
			// lbDate
			// 
			lbDate.AutoSize = true;
			lbDate.Location = new Point(674, 99);
			lbDate.Name = "lbDate";
			lbDate.Size = new Size(50, 20);
			lbDate.TabIndex = 21;
			lbDate.Text = "label4";
			// 
			// pictureBox1
			// 
			pictureBox1.Location = new Point(197, 141);
			pictureBox1.Name = "pictureBox1";
			pictureBox1.Size = new Size(493, 274);
			pictureBox1.TabIndex = 22;
			pictureBox1.TabStop = false;
			// 
			// RecipeDetail
			// 
			AutoScaleDimensions = new SizeF(8F, 20F);
			AutoScaleMode = AutoScaleMode.Font;
			ClientSize = new Size(914, 1175);
			Controls.Add(pictureBox1);
			Controls.Add(lbDate);
			Controls.Add(lbDescription);
			Controls.Add(label2);
			Controls.Add(lbIngredient);
			Controls.Add(label1);
			Controls.Add(lbTitle);
			Controls.Add(label5);
			Margin = new Padding(3, 4, 3, 4);
			Name = "RecipeDetail";
			Text = "RecipeDetail";
			((System.ComponentModel.ISupportInitialize)pictureBox1).EndInit();
			ResumeLayout(false);
			PerformLayout();
		}

		#endregion

		private Label label5;
		private Label lbTitle;
		private Label label1;
		private Label lbIngredient;
		private Label label2;
		private Label lbDescription;
		private Label lbDate;
		private PictureBox pictureBox1;
	}
}