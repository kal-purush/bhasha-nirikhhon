namespace Recipe_Organizer_PRN211.Recipe
{
    partial class SearchRecipe
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
            btnRefresh = new Button();
            btnSearch = new Button();
            txtSearch = new TextBox();
            dgvRecipeList = new DataGridView();
            lbTitle = new Label();
            label5 = new Label();
            ((System.ComponentModel.ISupportInitialize)dgvRecipeList).BeginInit();
            SuspendLayout();
            // 
            // btnRefresh
            // 
            btnRefresh.Font = new Font("Montserrat Medium", 8.249999F, FontStyle.Bold, GraphicsUnit.Point);
            btnRefresh.Location = new Point(509, 138);
            btnRefresh.Margin = new Padding(3, 4, 3, 4);
            btnRefresh.Name = "btnRefresh";
            btnRefresh.Size = new Size(86, 31);
            btnRefresh.TabIndex = 20;
            btnRefresh.Text = "Refresh";
            btnRefresh.UseVisualStyleBackColor = true;
            btnRefresh.Click += btnRefresh_Click;
            // 
            // btnSearch
            // 
            btnSearch.Font = new Font("Montserrat Medium", 8.249999F, FontStyle.Bold, GraphicsUnit.Point);
            btnSearch.Location = new Point(416, 138);
            btnSearch.Margin = new Padding(3, 4, 3, 4);
            btnSearch.Name = "btnSearch";
            btnSearch.Size = new Size(86, 31);
            btnSearch.TabIndex = 19;
            btnSearch.Text = "Search";
            btnSearch.UseVisualStyleBackColor = true;
            btnSearch.Click += btnSearch_Click;
            // 
            // txtSearch
            // 
            txtSearch.Location = new Point(61, 138);
            txtSearch.Margin = new Padding(3, 4, 3, 4);
            txtSearch.Name = "txtSearch";
            txtSearch.Size = new Size(348, 27);
            txtSearch.TabIndex = 18;
            // 
            // dgvRecipeList
            // 
            dgvRecipeList.ColumnHeadersHeightSizeMode = DataGridViewColumnHeadersHeightSizeMode.AutoSize;
            dgvRecipeList.Location = new Point(61, 192);
            dgvRecipeList.Margin = new Padding(3, 4, 3, 4);
            dgvRecipeList.Name = "dgvRecipeList";
            dgvRecipeList.RowHeadersWidth = 51;
            dgvRecipeList.RowTemplate.Height = 25;
            dgvRecipeList.Size = new Size(987, 458);
            dgvRecipeList.TabIndex = 17;
            dgvRecipeList.CellDoubleClick += dgvRecipeList_CellDoubleClick;
            // 
            // lbTitle
            // 
            lbTitle.AutoSize = true;
            lbTitle.Font = new Font("Montserrat Black", 18F, FontStyle.Regular, GraphicsUnit.Point);
            lbTitle.ForeColor = Color.FromArgb(255, 128, 0);
            lbTitle.Location = new Point(452, 69);
            lbTitle.Name = "lbTitle";
            lbTitle.Size = new Size(279, 41);
            lbTitle.TabIndex = 22;
            lbTitle.Text = "ALL OF RECIPES";
            lbTitle.TextAlign = ContentAlignment.TopCenter;
            // 
            // label5
            // 
            label5.AutoSize = true;
            label5.Font = new Font("Montserrat Black", 20.25F, FontStyle.Bold, GraphicsUnit.Point);
            label5.ForeColor = Color.FromArgb(192, 0, 0);
            label5.Location = new Point(43, 38);
            label5.Name = "label5";
            label5.Size = new Size(145, 47);
            label5.TabIndex = 21;
            label5.Text = "Recipe";
            // 
            // SearchRecipe
            // 
            AutoScaleDimensions = new SizeF(8F, 20F);
            AutoScaleMode = AutoScaleMode.Font;
            ClientSize = new Size(1113, 810);
            Controls.Add(lbTitle);
            Controls.Add(label5);
            Controls.Add(btnRefresh);
            Controls.Add(btnSearch);
            Controls.Add(txtSearch);
            Controls.Add(dgvRecipeList);
            Name = "SearchRecipe";
            Text = "RecipeList";
            ((System.ComponentModel.ISupportInitialize)dgvRecipeList).EndInit();
            ResumeLayout(false);
            PerformLayout();
        }

        #endregion
        private Button btnRefresh;
        private Button btnSearch;
        private TextBox txtSearch;
        private DataGridView dgvRecipeList;
        private Label lbTitle;
        private Label label5;
    }
}