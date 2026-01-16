namespace CRM_GTMK.Visual
{
	partial class AddNewClientForm
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
			this.CompanyNameTextBox = new System.Windows.Forms.TextBox();
			this.CompanyNameLabel = new System.Windows.Forms.Label();
			this.CompanyActivityLabel = new System.Windows.Forms.Label();
			this.AddClientDataButton = new System.Windows.Forms.Button();
			this.label1 = new System.Windows.Forms.Label();
			this.CompanySiteTextBox = new System.Windows.Forms.TextBox();
			this.SuspendLayout();
			// 
			// CompanyNameTextBox
			// 
			this.CompanyNameTextBox.Location = new System.Drawing.Point(176, 21);
			this.CompanyNameTextBox.Name = "CompanyNameTextBox";
			this.CompanyNameTextBox.Size = new System.Drawing.Size(165, 20);
			this.CompanyNameTextBox.TabIndex = 0;
			// 
			// CompanyNameLabel
			// 
			this.CompanyNameLabel.AutoSize = true;
			this.CompanyNameLabel.Location = new System.Drawing.Point(38, 24);
			this.CompanyNameLabel.Name = "CompanyNameLabel";
			this.CompanyNameLabel.Size = new System.Drawing.Size(110, 13);
			this.CompanyNameLabel.TabIndex = 1;
			this.CompanyNameLabel.Text = "Название компании";
			// 
			// CompanyActivityLabel
			// 
			this.CompanyActivityLabel.AutoSize = true;
			this.CompanyActivityLabel.Location = new System.Drawing.Point(12, 159);
			this.CompanyActivityLabel.Name = "CompanyActivityLabel";
			this.CompanyActivityLabel.Size = new System.Drawing.Size(148, 13);
			this.CompanyActivityLabel.TabIndex = 2;
			this.CompanyActivityLabel.Text = "Направление деятельности";
			this.CompanyActivityLabel.Click += new System.EventHandler(this.CompanyActivityLabel_Click);
			// 
			// AddClientDataButton
			// 
			this.AddClientDataButton.Location = new System.Drawing.Point(300, 205);
			this.AddClientDataButton.Name = "AddClientDataButton";
			this.AddClientDataButton.Size = new System.Drawing.Size(75, 35);
			this.AddClientDataButton.TabIndex = 3;
			this.AddClientDataButton.Text = "Сохранить данные";
			this.AddClientDataButton.UseVisualStyleBackColor = true;
			this.AddClientDataButton.Click += new System.EventHandler(this.button1_Click);
			// 
			// label1
			// 
			this.label1.AutoSize = true;
			this.label1.Location = new System.Drawing.Point(117, 57);
			this.label1.Name = "label1";
			this.label1.Size = new System.Drawing.Size(31, 13);
			this.label1.TabIndex = 4;
			this.label1.Text = "Сайт";
			// 
			// CompanySiteTextBox
			// 
			this.CompanySiteTextBox.Location = new System.Drawing.Point(176, 57);
			this.CompanySiteTextBox.Name = "CompanySiteTextBox";
			this.CompanySiteTextBox.Size = new System.Drawing.Size(165, 20);
			this.CompanySiteTextBox.TabIndex = 5;
			// 
			// AddNewClientForm
			// 
			this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
			this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
			this.ClientSize = new System.Drawing.Size(724, 261);
			this.Controls.Add(this.CompanySiteTextBox);
			this.Controls.Add(this.label1);
			this.Controls.Add(this.AddClientDataButton);
			this.Controls.Add(this.CompanyActivityLabel);
			this.Controls.Add(this.CompanyNameLabel);
			this.Controls.Add(this.CompanyNameTextBox);
			this.Name = "AddNewClientForm";
			this.Text = "AddNewClientForm";
			this.Load += new System.EventHandler(this.AddNewClientForm_Load);
			this.ResumeLayout(false);
			this.PerformLayout();

		}

		#endregion

		private System.Windows.Forms.TextBox CompanyNameTextBox;
		private System.Windows.Forms.Label CompanyNameLabel;
		private System.Windows.Forms.Label CompanyActivityLabel;
		private System.Windows.Forms.Button AddClientDataButton;
		private System.Windows.Forms.Label label1;
		private System.Windows.Forms.TextBox CompanySiteTextBox;
	}
}