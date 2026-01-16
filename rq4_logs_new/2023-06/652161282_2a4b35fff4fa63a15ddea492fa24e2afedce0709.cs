using Recipe_Organizer_PRN211.Recipe;

namespace Recipe_Organizer_PRN211.Plan
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
			System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(SearchRecipe));
			btnRefresh = new Button();
			btnSearch = new Button();
			txtSearch = new TextBox();
			lbTitle = new Label();
			lstRecipes = new ListBox();
			btnLogout = new Button();
			btnUserProfile = new Button();
			txtWelcome = new Label();
			btnAddRecipe = new PictureBox();
			btnMealPlanning = new Button();
			((System.ComponentModel.ISupportInitialize)btnAddRecipe).BeginInit();
			SuspendLayout();
			// 
			// btnRefresh
			// 
			btnRefresh.Font = new Font("Montserrat Medium", 8.249999F, FontStyle.Bold, GraphicsUnit.Point);
			btnRefresh.Location = new Point(445, 104);
			btnRefresh.Name = "btnRefresh";
			btnRefresh.Size = new Size(75, 23);
			btnRefresh.TabIndex = 20;
			btnRefresh.Text = "Refresh";
			btnRefresh.UseVisualStyleBackColor = true;
			btnRefresh.Click += btnRefresh_Click;
			// 
			// btnSearch
			// 
			btnSearch.Font = new Font("Montserrat Medium", 8.249999F, FontStyle.Bold, GraphicsUnit.Point);
			btnSearch.Location = new Point(364, 104);
			btnSearch.Name = "btnSearch";
			btnSearch.Size = new Size(75, 23);
			btnSearch.TabIndex = 19;
			btnSearch.Text = "Search";
			btnSearch.UseVisualStyleBackColor = true;
			btnSearch.Click += btnSearch_Click;
			// 
			// txtSearch
			// 
			txtSearch.Location = new Point(53, 104);
			txtSearch.Name = "txtSearch";
			txtSearch.Size = new Size(305, 23);
			txtSearch.TabIndex = 18;
			// 
			// lbTitle
			// 
			lbTitle.AutoSize = true;
			lbTitle.Font = new Font("Montserrat Black", 18F, FontStyle.Regular, GraphicsUnit.Point);
			lbTitle.ForeColor = Color.FromArgb(255, 128, 0);
			lbTitle.Location = new Point(386, 55);
			lbTitle.Name = "lbTitle";
			lbTitle.Size = new Size(223, 33);
			lbTitle.TabIndex = 22;
			lbTitle.Text = "ALL OF RECIPES";
			lbTitle.TextAlign = ContentAlignment.TopCenter;
			// 
			// lstRecipes
			// 
			lstRecipes.FormattingEnabled = true;
			lstRecipes.ItemHeight = 15;
			lstRecipes.Location = new Point(53, 144);
			lstRecipes.Margin = new Padding(3, 2, 3, 2);
			lstRecipes.Name = "lstRecipes";
			lstRecipes.Size = new Size(875, 409);
			lstRecipes.TabIndex = 23;
			lstRecipes.SelectedIndexChanged += lstRecipes_SelectedIndexChanged;
			// 
			// btnLogout
			// 
			btnLogout.Font = new Font("Montserrat", 8.999999F, FontStyle.Regular, GraphicsUnit.Point);
			btnLogout.Location = new Point(839, 12);
			btnLogout.Name = "btnLogout";
			btnLogout.Size = new Size(89, 38);
			btnLogout.TabIndex = 24;
			btnLogout.Text = "Logout";
			btnLogout.UseVisualStyleBackColor = true;
			btnLogout.Click += btnLogout_Click;
			// 
			// btnUserProfile
			// 
			btnUserProfile.FlatStyle = FlatStyle.System;
			btnUserProfile.Font = new Font("Montserrat", 8.999999F, FontStyle.Regular, GraphicsUnit.Point);
			btnUserProfile.ForeColor = Color.Red;
			btnUserProfile.Location = new Point(744, 12);
			btnUserProfile.Name = "btnUserProfile";
			btnUserProfile.Size = new Size(89, 38);
			btnUserProfile.TabIndex = 26;
			btnUserProfile.Text = "User profile";
			btnUserProfile.UseVisualStyleBackColor = true;
			btnUserProfile.Click += btnUserProfile_Click;
			// 
			// txtWelcome
			// 
			txtWelcome.AutoSize = true;
			txtWelcome.Font = new Font("Montserrat", 17.9999981F, FontStyle.Bold, GraphicsUnit.Point);
			txtWelcome.Location = new Point(24, 17);
			txtWelcome.Name = "txtWelcome";
			txtWelcome.Size = new Size(79, 33);
			txtWelcome.TabIndex = 57;
			txtWelcome.Text = "Hello";
			// 
			// btnAddRecipe
			// 
			btnAddRecipe.BackgroundImageLayout = ImageLayout.None;
			btnAddRecipe.Image = Properties.Resources.add_button;
			btnAddRecipe.Location = new Point(683, 12);
			btnAddRecipe.Name = "btnAddRecipe";
			btnAddRecipe.Size = new Size(42, 38);
			btnAddRecipe.SizeMode = PictureBoxSizeMode.StretchImage;
			btnAddRecipe.TabIndex = 58;
			btnAddRecipe.TabStop = false;
			btnAddRecipe.Click += btnAddRecipe_Click;
			// 
			// btnMealPlanning
			// 
			btnMealPlanning.Font = new Font("Arial", 9.75F, FontStyle.Bold, GraphicsUnit.Point);
			btnMealPlanning.Location = new Point(552, 10);
			btnMealPlanning.Name = "btnMealPlanning";
			btnMealPlanning.Size = new Size(114, 40);
			btnMealPlanning.TabIndex = 59;
			btnMealPlanning.Text = "Meal planning";
			btnMealPlanning.UseVisualStyleBackColor = true;
			btnMealPlanning.Click += btnMealPlanning_Click;
			// 
			// SearchRecipe
			// 
			AutoScaleDimensions = new SizeF(7F, 15F);
			AutoScaleMode = AutoScaleMode.Font;
			BackgroundImageLayout = ImageLayout.Center;
			ClientSize = new Size(974, 608);
			Controls.Add(btnMealPlanning);
			Controls.Add(btnAddRecipe);
			Controls.Add(txtWelcome);
			Controls.Add(btnUserProfile);
			Controls.Add(btnLogout);
			Controls.Add(lstRecipes);
			Controls.Add(lbTitle);
			Controls.Add(btnRefresh);
			Controls.Add(btnSearch);
			Controls.Add(txtSearch);
			Icon = (Icon)resources.GetObject("$this.Icon");
			Margin = new Padding(3, 2, 3, 2);
			Name = "SearchRecipe";
			StartPosition = FormStartPosition.CenterScreen;
			Text = "RecipeList";
			Load += SearchRecipe_Load;
			((System.ComponentModel.ISupportInitialize)btnAddRecipe).EndInit();
			ResumeLayout(false);
			PerformLayout();
		}

		#endregion
		private Button btnRefresh;
		private Button btnSearch;
		private TextBox txtSearch;
		private Label lbTitle;
		private ListBox lstRecipes;
		private Button btnLogout;
		private Button btnUserProfile;
		private Label txtWelcome;
		private PictureBox btnAddRecipe;
		private Button btnMealPlanning;
	}
}