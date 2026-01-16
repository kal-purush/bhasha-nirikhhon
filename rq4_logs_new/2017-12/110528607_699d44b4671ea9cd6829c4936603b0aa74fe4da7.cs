namespace CRM_GTMK.Visual
{
	partial class MainScreenForm
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
			System.Windows.Forms.TreeNode treeNode7 = new System.Windows.Forms.TreeNode("Компании");
			System.Windows.Forms.TreeNode treeNode8 = new System.Windows.Forms.TreeNode("Физ. лица");
			System.Windows.Forms.TreeNode treeNode9 = new System.Windows.Forms.TreeNode("Благотворительность");
			System.Windows.Forms.TreeNode treeNode10 = new System.Windows.Forms.TreeNode("Клиенты", new System.Windows.Forms.TreeNode[] {
            treeNode7,
            treeNode8,
            treeNode9});
			System.Windows.Forms.TreeNode treeNode11 = new System.Windows.Forms.TreeNode("Проекты");
			System.Windows.Forms.TreeNode treeNode12 = new System.Windows.Forms.TreeNode("Задачи");
			this.treeView1 = new System.Windows.Forms.TreeView();
			this.panel1 = new System.Windows.Forms.Panel();
			this.addNewCompanyButton = new System.Windows.Forms.Button();
			this.companiesDefaultListDataGridView = new System.Windows.Forms.DataGridView();
			this.CompanyNameColumn = new System.Windows.Forms.DataGridViewTextBoxColumn();
			this.CompanyCityColumn = new System.Windows.Forms.DataGridViewTextBoxColumn();
			this.CompanyStatusColumn = new System.Windows.Forms.DataGridViewTextBoxColumn();
			this.LastContactDateColumn = new System.Windows.Forms.DataGridViewTextBoxColumn();
			this.AddingDateColumn = new System.Windows.Forms.DataGridViewTextBoxColumn();
			this.panel1.SuspendLayout();
			((System.ComponentModel.ISupportInitialize)(this.companiesDefaultListDataGridView)).BeginInit();
			this.SuspendLayout();
			// 
			// treeView1
			// 
			this.treeView1.Location = new System.Drawing.Point(12, 57);
			this.treeView1.Name = "treeView1";
			treeNode7.Name = "CompaniesNode";
			treeNode7.Text = "Компании";
			treeNode8.Name = "PhisicalPersonsNode";
			treeNode8.Text = "Физ. лица";
			treeNode9.Name = "CharityNode";
			treeNode9.Text = "Благотворительность";
			treeNode10.Checked = true;
			treeNode10.Name = "ClientsRoot";
			treeNode10.Text = "Клиенты";
			treeNode11.Name = "ProjectsRoot";
			treeNode11.Text = "Проекты";
			treeNode12.Name = "TasksRoot";
			treeNode12.Text = "Задачи";
			this.treeView1.Nodes.AddRange(new System.Windows.Forms.TreeNode[] {
            treeNode10,
            treeNode11,
            treeNode12});
			this.treeView1.Size = new System.Drawing.Size(165, 350);
			this.treeView1.TabIndex = 0;
			// 
			// panel1
			// 
			this.panel1.Controls.Add(this.companiesDefaultListDataGridView);
			this.panel1.Controls.Add(this.addNewCompanyButton);
			this.panel1.Location = new System.Drawing.Point(211, 57);
			this.panel1.Name = "panel1";
			this.panel1.Size = new System.Drawing.Size(555, 350);
			this.panel1.TabIndex = 1;
			// 
			// addNewCompanyButton
			// 
			this.addNewCompanyButton.Location = new System.Drawing.Point(210, 22);
			this.addNewCompanyButton.Name = "addNewCompanyButton";
			this.addNewCompanyButton.Size = new System.Drawing.Size(135, 23);
			this.addNewCompanyButton.TabIndex = 0;
			this.addNewCompanyButton.Text = "Добавить компанию";
			this.addNewCompanyButton.UseVisualStyleBackColor = true;
			this.addNewCompanyButton.Click += new System.EventHandler(this.addNewCompanyButton_Click);
			// 
			// companiesDefaultListDataGridView
			// 
			this.companiesDefaultListDataGridView.ColumnHeadersHeightSizeMode = System.Windows.Forms.DataGridViewColumnHeadersHeightSizeMode.AutoSize;
			this.companiesDefaultListDataGridView.Columns.AddRange(new System.Windows.Forms.DataGridViewColumn[] {
            this.CompanyNameColumn,
            this.CompanyCityColumn,
            this.CompanyStatusColumn,
            this.LastContactDateColumn,
            this.AddingDateColumn});
			this.companiesDefaultListDataGridView.Location = new System.Drawing.Point(18, 62);
			this.companiesDefaultListDataGridView.Name = "companiesDefaultListDataGridView";
			this.companiesDefaultListDataGridView.Size = new System.Drawing.Size(525, 275);
			this.companiesDefaultListDataGridView.TabIndex = 1;
			// 
			// CompanyNameColumn
			// 
			this.CompanyNameColumn.HeaderText = "Название";
			this.CompanyNameColumn.Name = "CompanyNameColumn";
			// 
			// CompanyCityColumn
			// 
			this.CompanyCityColumn.HeaderText = "Город";
			this.CompanyCityColumn.Name = "CompanyCityColumn";
			// 
			// CompanyStatusColumn
			// 
			this.CompanyStatusColumn.HeaderText = "Статус";
			this.CompanyStatusColumn.Name = "CompanyStatusColumn";
			// 
			// LastContactDateColumn
			// 
			this.LastContactDateColumn.HeaderText = "Дата крайнего контакта";
			this.LastContactDateColumn.Name = "LastContactDateColumn";
			// 
			// AddingDateColumn
			// 
			this.AddingDateColumn.HeaderText = "Дата внесения в базу";
			this.AddingDateColumn.Name = "AddingDateColumn";
			// 
			// MainScreenForm
			// 
			this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
			this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
			this.ClientSize = new System.Drawing.Size(789, 434);
			this.Controls.Add(this.panel1);
			this.Controls.Add(this.treeView1);
			this.Name = "MainScreenForm";
			this.Text = "MainScreenForm";
			this.panel1.ResumeLayout(false);
			((System.ComponentModel.ISupportInitialize)(this.companiesDefaultListDataGridView)).EndInit();
			this.ResumeLayout(false);

		}

		#endregion

		private System.Windows.Forms.TreeView treeView1;
		private System.Windows.Forms.Panel panel1;
		private System.Windows.Forms.DataGridView companiesDefaultListDataGridView;
		private System.Windows.Forms.DataGridViewTextBoxColumn CompanyNameColumn;
		private System.Windows.Forms.DataGridViewTextBoxColumn CompanyCityColumn;
		private System.Windows.Forms.DataGridViewTextBoxColumn CompanyStatusColumn;
		private System.Windows.Forms.DataGridViewTextBoxColumn LastContactDateColumn;
		private System.Windows.Forms.DataGridViewTextBoxColumn AddingDateColumn;
		private System.Windows.Forms.Button addNewCompanyButton;
	}
}