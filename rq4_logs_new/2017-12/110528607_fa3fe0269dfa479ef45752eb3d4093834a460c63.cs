using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace CRM_GTMK.Visual.MainScreenPanels
{
	public class MyClientsPanel :  Panel
	{
		public MyClientsPanel(MainScreenForm form) : base()
		{
			Controls.Add(new MyCompaniesDefaultListDataGridView());
			Controls.Add(new MyAddNewCompanyButton(form));
			Location = new System.Drawing.Point(211, 57);
			Name = "clientsPanel";
			Size = new System.Drawing.Size(1075, 517);
			TabIndex = 1;

		}
	}

	public class MyCompaniesDefaultListDataGridView : DataGridView
	{
		private DataGridViewTextBoxColumn CompanyNameColumn;
		private DataGridViewTextBoxColumn CompanyCityColumn;
		private DataGridViewTextBoxColumn CompanyStatusColumn;
		private DataGridViewTextBoxColumn LastContactDateColumn;
		private DataGridViewTextBoxColumn AddingDateColumn;

		public MyCompaniesDefaultListDataGridView()
		{
			this.CompanyNameColumn = new DataGridViewTextBoxColumn();
			this.CompanyCityColumn = new DataGridViewTextBoxColumn();
			this.CompanyStatusColumn = new DataGridViewTextBoxColumn();
			this.LastContactDateColumn = new DataGridViewTextBoxColumn();
			this.AddingDateColumn = new DataGridViewTextBoxColumn();

			ColumnHeadersHeightSizeMode = System.Windows.Forms.DataGridViewColumnHeadersHeightSizeMode.AutoSize;
			Columns.AddRange(new System.Windows.Forms.DataGridViewColumn[] {
			this.CompanyNameColumn,
			this.CompanyCityColumn,
			this.CompanyStatusColumn,
			this.LastContactDateColumn,
			this.AddingDateColumn});
			Location = new System.Drawing.Point(18, 62);
			Name = "companiesDefaultListDataGridView";
			Size = new System.Drawing.Size(525, 275);
			TabIndex = 1;
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
		}
	}

	public class MyAddNewCompanyButton : Button
	{
		private MainScreenForm _form;
		public MyAddNewCompanyButton(MainScreenForm form) : base()
		{
			_form = form;

			Location = new System.Drawing.Point(210, 22);
			Name = "addNewCompanyButton";
			Size = new System.Drawing.Size(135, 23);
			TabIndex = 0;
			Text = "Добавить компанию";
			UseVisualStyleBackColor = true;
			Click += new System.EventHandler(IsClicked);
		}

		private void IsClicked(object sender, EventArgs e)
		{
			_form.addNewCompanyButton_Click();
		}
	}
		

	/*
	 * this.clientsPanel.Controls.Add(this.companiesDefaultListDataGridView);
			this.clientsPanel.Controls.Add(this.addNewCompanyButton);
			this.clientsPanel.Location = new System.Drawing.Point(211, 57);
			this.clientsPanel.Name = "clientsPanel";
			this.clientsPanel.Size = new System.Drawing.Size(1075, 517);
			this.clientsPanel.TabIndex = 1;

	// MyCompaniesDefaultListDataGridView
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
			// addNewCompanyButton
			// 
			this.addNewCompanyButton.Location = new System.Drawing.Point(210, 22);
			this.addNewCompanyButton.Name = "addNewCompanyButton";
			this.addNewCompanyButton.Size = new System.Drawing.Size(135, 23);
			this.addNewCompanyButton.TabIndex = 0;
			this.addNewCompanyButton.Text = "Добавить компанию";
			this.addNewCompanyButton.UseVisualStyleBackColor = true;
			this.addNewCompanyButton.Click += new System.EventHandler(this.addNewCompanyButton_Click);
	 */
	}