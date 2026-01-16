namespace Recipe_Organizer_PRN211.Manage
{
	partial class RecipeDetailPending
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
			btnBack = new Button();
			tlpDirection = new TableLayoutPanel();
			tlpIngredients = new TableLayoutPanel();
			pictureBox1 = new PictureBox();
			lbDate = new Label();
			label2 = new Label();
			label1 = new Label();
			lbTitle = new Label();
			label5 = new Label();
			((System.ComponentModel.ISupportInitialize)pictureBox1).BeginInit();
			SuspendLayout();
			// 
			// btnBack
			// 
			btnBack.BackColor = Color.Transparent;
			btnBack.BackgroundImage = Properties.Resources.left_arrow;
			btnBack.BackgroundImageLayout = ImageLayout.Zoom;
			btnBack.Location = new Point(10, 5);
			btnBack.Name = "btnBack";
			btnBack.Size = new Size(76, 38);
			btnBack.TabIndex = 35;
			btnBack.TextAlign = ContentAlignment.MiddleLeft;
			btnBack.UseVisualStyleBackColor = false;
			// 
			// tlpDirection
			// 
			tlpDirection.AutoScroll = true;
			tlpDirection.AutoSize = true;
			tlpDirection.ColumnCount = 2;
			tlpDirection.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 50F));
			tlpDirection.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 50F));
			tlpDirection.GrowStyle = TableLayoutPanelGrowStyle.FixedSize;
			tlpDirection.Location = new Point(56, 376);
			tlpDirection.Margin = new Padding(3, 2, 3, 2);
			tlpDirection.Name = "tlpDirection";
			tlpDirection.RowCount = 2;
			tlpDirection.RowStyles.Add(new RowStyle(SizeType.Percent, 50F));
			tlpDirection.RowStyles.Add(new RowStyle(SizeType.Percent, 50F));
			tlpDirection.Size = new Size(900, 119);
			tlpDirection.TabIndex = 34;
			// 
			// tlpIngredients
			// 
			tlpIngredients.ColumnCount = 2;
			tlpIngredients.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 50F));
			tlpIngredients.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 50F));
			tlpIngredients.Location = new Point(56, 164);
			tlpIngredients.Margin = new Padding(3, 2, 3, 2);
			tlpIngredients.Name = "tlpIngredients";
			tlpIngredients.RowCount = 2;
			tlpIngredients.RowStyles.Add(new RowStyle(SizeType.Percent, 50F));
			tlpIngredients.RowStyles.Add(new RowStyle(SizeType.Percent, 50F));
			tlpIngredients.Size = new Size(387, 153);
			tlpIngredients.TabIndex = 33;
			// 
			// pictureBox1
			// 
			pictureBox1.Location = new Point(525, 108);
			pictureBox1.Margin = new Padding(3, 2, 3, 2);
			pictureBox1.Name = "pictureBox1";
			pictureBox1.Size = new Size(431, 209);
			pictureBox1.TabIndex = 32;
			pictureBox1.TabStop = false;
			// 
			// lbDate
			// 
			lbDate.AutoSize = true;
			lbDate.Location = new Point(35, 78);
			lbDate.Name = "lbDate";
			lbDate.Size = new Size(38, 15);
			lbDate.TabIndex = 31;
			lbDate.Text = "label4";
			// 
			// label2
			// 
			label2.AutoSize = true;
			label2.Font = new Font("Segoe UI", 11F, FontStyle.Bold, GraphicsUnit.Point);
			label2.Location = new Point(35, 341);
			label2.Name = "label2";
			label2.Size = new Size(80, 20);
			label2.TabIndex = 30;
			label2.Text = "Directions";
			// 
			// label1
			// 
			label1.AutoSize = true;
			label1.Font = new Font("Segoe UI", 11F, FontStyle.Bold, GraphicsUnit.Point);
			label1.Location = new Point(35, 122);
			label1.Name = "label1";
			label1.Size = new Size(89, 20);
			label1.TabIndex = 29;
			label1.Text = "Ingredients";
			// 
			// lbTitle
			// 
			lbTitle.AutoSize = true;
			lbTitle.Font = new Font("Montserrat Black", 18F, FontStyle.Regular, GraphicsUnit.Point);
			lbTitle.ForeColor = Color.FromArgb(255, 128, 0);
			lbTitle.Location = new Point(395, 40);
			lbTitle.Name = "lbTitle";
			lbTitle.Size = new Size(226, 33);
			lbTitle.TabIndex = 28;
			lbTitle.Text = "TITLE HEREEEEE";
			lbTitle.TextAlign = ContentAlignment.TopCenter;
			// 
			// label5
			// 
			label5.AutoSize = true;
			label5.Font = new Font("Montserrat Black", 20.25F, FontStyle.Bold, GraphicsUnit.Point);
			label5.ForeColor = Color.FromArgb(192, 0, 0);
			label5.Location = new Point(791, 23);
			label5.Name = "label5";
			label5.Size = new Size(210, 37);
			label5.TabIndex = 27;
			label5.Text = "Recipe detail";
			// 
			// RecipeDetailPending
			// 
			AutoScaleDimensions = new SizeF(7F, 15F);
			AutoScaleMode = AutoScaleMode.Font;
			ClientSize = new Size(1032, 526);
			Controls.Add(btnBack);
			Controls.Add(tlpDirection);
			Controls.Add(tlpIngredients);
			Controls.Add(pictureBox1);
			Controls.Add(lbDate);
			Controls.Add(label2);
			Controls.Add(label1);
			Controls.Add(lbTitle);
			Controls.Add(label5);
			Name = "RecipeDetailPending";
			Text = "RecipeDetailPending";
			((System.ComponentModel.ISupportInitialize)pictureBox1).EndInit();
			ResumeLayout(false);
			PerformLayout();
		}

		#endregion

		private Button btnBack;
		private TableLayoutPanel tlpDirection;
		private TableLayoutPanel tlpIngredients;
		private PictureBox pictureBox1;
		private Label lbDate;
		private Label label2;
		private Label label1;
		private Label lbTitle;
		private Label label5;
	}
}