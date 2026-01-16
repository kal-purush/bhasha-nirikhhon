namespace CRM_GTMK.Visual
{
	partial class ClientsListForm
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
			this.ClientsDataGridView = new System.Windows.Forms.DataGridView();
			this.CompanyNameColumn = new System.Windows.Forms.DataGridViewTextBoxColumn();
			this.CompanySiteColumn = new System.Windows.Forms.DataGridViewTextBoxColumn();
			((System.ComponentModel.ISupportInitialize)(this.ClientsDataGridView)).BeginInit();
			this.SuspendLayout();
			// 
			// ClientsDataGridView
			// 
			this.ClientsDataGridView.ColumnHeadersHeightSizeMode = System.Windows.Forms.DataGridViewColumnHeadersHeightSizeMode.AutoSize;
			this.ClientsDataGridView.Columns.AddRange(new System.Windows.Forms.DataGridViewColumn[] {
            this.CompanyNameColumn,
            this.CompanySiteColumn});
			this.ClientsDataGridView.Location = new System.Drawing.Point(12, 12);
			this.ClientsDataGridView.Name = "ClientsDataGridView";
			this.ClientsDataGridView.Size = new System.Drawing.Size(382, 150);
			this.ClientsDataGridView.TabIndex = 1;
			this.ClientsDataGridView.CellContentClick += new System.Windows.Forms.DataGridViewCellEventHandler(this.dataGridView1_CellContentClick);
			// 
			// CompanyNameColumn
			// 
			this.CompanyNameColumn.HeaderText = "Название компании";
			this.CompanyNameColumn.Name = "CompanyNameColumn";
			// 
			// CompanySiteColumn
			// 
			this.CompanySiteColumn.HeaderText = "Сайт";
			this.CompanySiteColumn.Name = "CompanySiteColumn";
			// 
			// ClientsListForm
			// 
			this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
			this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
			this.ClientSize = new System.Drawing.Size(476, 428);
			this.Controls.Add(this.ClientsDataGridView);
			this.Name = "ClientsListForm";
			this.Text = "ClientsListForm";
			((System.ComponentModel.ISupportInitialize)(this.ClientsDataGridView)).EndInit();
			this.ResumeLayout(false);

		}

		#endregion
		private System.Windows.Forms.DataGridView ClientsDataGridView;
		private System.Windows.Forms.DataGridViewTextBoxColumn CompanyNameColumn;
		private System.Windows.Forms.DataGridViewTextBoxColumn CompanySiteColumn;
	}
}