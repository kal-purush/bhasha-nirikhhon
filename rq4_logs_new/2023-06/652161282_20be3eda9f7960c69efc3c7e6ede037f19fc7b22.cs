namespace Recipe_Organizer_PRN211.Authentication
{
    partial class CategoryCRUD
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
            btnDelete = new Button();
            btnUpdate = new Button();
            btnCreate = new Button();
            label7 = new Label();
            txtPassword = new TextBox();
            txtUsername = new TextBox();
            empty = new Label();
            btnRefresh = new Button();
            btnSearch = new Button();
            txtSearch = new TextBox();
            dgvListUser = new DataGridView();
            label5 = new Label();
            ((System.ComponentModel.ISupportInitialize)dgvListUser).BeginInit();
            SuspendLayout();
            // 
            // btnDelete
            // 
            btnDelete.Location = new Point(821, 540);
            btnDelete.Margin = new Padding(3, 4, 3, 4);
            btnDelete.Name = "btnDelete";
            btnDelete.Size = new Size(79, 39);
            btnDelete.TabIndex = 56;
            btnDelete.Text = "Delete";
            btnDelete.UseVisualStyleBackColor = true;
            // 
            // btnUpdate
            // 
            btnUpdate.Location = new Point(728, 540);
            btnUpdate.Margin = new Padding(3, 4, 3, 4);
            btnUpdate.Name = "btnUpdate";
            btnUpdate.Size = new Size(79, 39);
            btnUpdate.TabIndex = 55;
            btnUpdate.Text = "Update";
            btnUpdate.UseVisualStyleBackColor = true;
            // 
            // btnCreate
            // 
            btnCreate.Location = new Point(635, 540);
            btnCreate.Margin = new Padding(3, 4, 3, 4);
            btnCreate.Name = "btnCreate";
            btnCreate.Size = new Size(79, 39);
            btnCreate.TabIndex = 54;
            btnCreate.Text = "Create";
            btnCreate.UseVisualStyleBackColor = true;
            // 
            // label7
            // 
            label7.AutoSize = true;
            label7.Font = new Font("Microsoft Sans Serif", 9.749999F, FontStyle.Bold, GraphicsUnit.Point);
            label7.Location = new Point(520, 391);
            label7.Name = "label7";
            label7.Size = new Size(91, 20);
            label7.TabIndex = 52;
            label7.Text = "Password";
            // 
            // txtPassword
            // 
            txtPassword.Location = new Point(635, 391);
            txtPassword.Margin = new Padding(3, 4, 3, 4);
            txtPassword.Name = "txtPassword";
            txtPassword.Size = new Size(254, 27);
            txtPassword.TabIndex = 51;
            // 
            // txtUsername
            // 
            txtUsername.Location = new Point(243, 391);
            txtUsername.Margin = new Padding(3, 4, 3, 4);
            txtUsername.Name = "txtUsername";
            txtUsername.Size = new Size(254, 27);
            txtUsername.TabIndex = 47;
            // 
            // empty
            // 
            empty.AutoSize = true;
            empty.Font = new Font("Microsoft Sans Serif", 9.749999F, FontStyle.Bold, GraphicsUnit.Point);
            empty.Location = new Point(78, 389);
            empty.Name = "empty";
            empty.Size = new Size(138, 20);
            empty.TabIndex = 42;
            empty.Text = "Category Name";
            // 
            // btnRefresh
            // 
            btnRefresh.Font = new Font("Microsoft Sans Serif", 8.249999F, FontStyle.Bold, GraphicsUnit.Point);
            btnRefresh.Location = new Point(526, 101);
            btnRefresh.Margin = new Padding(3, 4, 3, 4);
            btnRefresh.Name = "btnRefresh";
            btnRefresh.Size = new Size(79, 31);
            btnRefresh.TabIndex = 41;
            btnRefresh.Text = "Refresh";
            btnRefresh.UseVisualStyleBackColor = true;
            // 
            // btnSearch
            // 
            btnSearch.Font = new Font("Microsoft Sans Serif", 8.249999F, FontStyle.Bold, GraphicsUnit.Point);
            btnSearch.Location = new Point(433, 101);
            btnSearch.Margin = new Padding(3, 4, 3, 4);
            btnSearch.Name = "btnSearch";
            btnSearch.Size = new Size(79, 31);
            btnSearch.TabIndex = 40;
            btnSearch.Text = "Search";
            btnSearch.UseVisualStyleBackColor = true;
            // 
            // txtSearch
            // 
            txtSearch.Location = new Point(78, 101);
            txtSearch.Margin = new Padding(3, 4, 3, 4);
            txtSearch.Name = "txtSearch";
            txtSearch.Size = new Size(341, 27);
            txtSearch.TabIndex = 39;
            // 
            // dgvListUser
            // 
            dgvListUser.ColumnHeadersHeightSizeMode = DataGridViewColumnHeadersHeightSizeMode.AutoSize;
            dgvListUser.Location = new Point(78, 155);
            dgvListUser.Margin = new Padding(3, 4, 3, 4);
            dgvListUser.Name = "dgvListUser";
            dgvListUser.RowHeadersWidth = 51;
            dgvListUser.RowTemplate.Height = 25;
            dgvListUser.Size = new Size(887, 193);
            dgvListUser.TabIndex = 38;
            dgvListUser.CellContentClick += dgvListUser_CellContentClick;
            // 
            // label5
            // 
            label5.AutoSize = true;
            label5.Font = new Font("Microsoft Sans Serif", 20.25F, FontStyle.Bold, GraphicsUnit.Point);
            label5.ForeColor = Color.FromArgb(192, 0, 0);
            label5.Location = new Point(415, 23);
            label5.Name = "label5";
            label5.Size = new Size(230, 39);
            label5.TabIndex = 37;
            label5.Text = "Category List";
            // 
            // Category
            // 
            AutoScaleDimensions = new SizeF(8F, 20F);
            AutoScaleMode = AutoScaleMode.Font;
            ClientSize = new Size(1043, 660);
            Controls.Add(btnDelete);
            Controls.Add(btnUpdate);
            Controls.Add(btnCreate);
            Controls.Add(label7);
            Controls.Add(txtPassword);
            Controls.Add(txtUsername);
            Controls.Add(empty);
            Controls.Add(btnRefresh);
            Controls.Add(btnSearch);
            Controls.Add(txtSearch);
            Controls.Add(dgvListUser);
            Controls.Add(label5);
            Name = "Category";
            Text = "Category";
            ((System.ComponentModel.ISupportInitialize)dgvListUser).EndInit();
            ResumeLayout(false);
            PerformLayout();
        }

        #endregion
        private Button btnDelete;
        private Button btnUpdate;
        private Button btnCreate;
        private Label label7;
        private TextBox txtPassword;
        private TextBox txtUsername;
        private Label empty;
        private Button btnRefresh;
        private Button btnSearch;
        private TextBox txtSearch;
        private DataGridView dgvListUser;
        private Label label5;
    }
}