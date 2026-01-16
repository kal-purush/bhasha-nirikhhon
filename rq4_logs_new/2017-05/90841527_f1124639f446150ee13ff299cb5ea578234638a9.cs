namespace GUI
{
    partial class AddInvestor
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
            this.components = new System.ComponentModel.Container();
            System.Windows.Forms.Label New_company;
            System.Windows.Forms.Label investorLabel;
            System.Windows.Forms.Label label1;
            this.name_add = new System.Windows.Forms.TextBox();
            this.Country_comboBox = new System.Windows.Forms.ComboBox();
            this.type_comboBox = new System.Windows.Forms.ComboBox();
            this.bankDataBase = new GUI.BankDataBase();
            this.countryBindingSource = new System.Windows.Forms.BindingSource(this.components);
            this.countryTableAdapter = new GUI.BankDataBaseTableAdapters.CountryTableAdapter();
            this.tipeBindingSource = new System.Windows.Forms.BindingSource(this.components);
            this.tipeTableAdapter = new GUI.BankDataBaseTableAdapters.TipeTableAdapter();
            this.add_comand = new System.Windows.Forms.Button();
            this.investorBindingSource = new System.Windows.Forms.BindingSource(this.components);
            this.investorTableAdapter = new GUI.BankDataBaseTableAdapters.InvestorTableAdapter();
            New_company = new System.Windows.Forms.Label();
            investorLabel = new System.Windows.Forms.Label();
            label1 = new System.Windows.Forms.Label();
            ((System.ComponentModel.ISupportInitialize)(this.bankDataBase)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.countryBindingSource)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.tipeBindingSource)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.investorBindingSource)).BeginInit();
            this.SuspendLayout();
            // 
            // name_add
            // 
            this.name_add.Location = new System.Drawing.Point(108, 43);
            this.name_add.Name = "name_add";
            this.name_add.Size = new System.Drawing.Size(121, 20);
            this.name_add.TabIndex = 15;
            // 
            // New_company
            // 
            New_company.AutoSize = true;
            New_company.Font = new System.Drawing.Font("Microsoft Sans Serif", 9.75F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(204)));
            New_company.Location = new System.Drawing.Point(34, 44);
            New_company.Name = "New_company";
            New_company.Size = new System.Drawing.Size(68, 16);
            New_company.TabIndex = 14;
            New_company.Text = "Comany:";
            // 
            // investorLabel
            // 
            investorLabel.AutoSize = true;
            investorLabel.Font = new System.Drawing.Font("Microsoft Sans Serif", 9.75F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(204)));
            investorLabel.Location = new System.Drawing.Point(38, 70);
            investorLabel.Name = "investorLabel";
            investorLabel.Size = new System.Drawing.Size(64, 16);
            investorLabel.TabIndex = 16;
            investorLabel.Text = "Country:";
            // 
            // Country_comboBox
            // 
            this.Country_comboBox.DataSource = this.countryBindingSource;
            this.Country_comboBox.DisplayMember = "Name";
            this.Country_comboBox.FormattingEnabled = true;
            this.Country_comboBox.Location = new System.Drawing.Point(108, 69);
            this.Country_comboBox.Name = "Country_comboBox";
            this.Country_comboBox.Size = new System.Drawing.Size(121, 21);
            this.Country_comboBox.TabIndex = 17;
            this.Country_comboBox.ValueMember = "id";
            // 
            // label1
            // 
            label1.AutoSize = true;
            label1.Font = new System.Drawing.Font("Microsoft Sans Serif", 9.75F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(204)));
            label1.Location = new System.Drawing.Point(53, 97);
            label1.Name = "label1";
            label1.Size = new System.Drawing.Size(48, 16);
            label1.TabIndex = 18;
            label1.Text = "Type:";
            // 
            // type_comboBox
            // 
            this.type_comboBox.DataSource = this.tipeBindingSource;
            this.type_comboBox.DisplayMember = "Name";
            this.type_comboBox.FormattingEnabled = true;
            this.type_comboBox.Location = new System.Drawing.Point(108, 96);
            this.type_comboBox.Name = "type_comboBox";
            this.type_comboBox.Size = new System.Drawing.Size(121, 21);
            this.type_comboBox.TabIndex = 19;
            this.type_comboBox.ValueMember = "id";
            // 
            // bankDataBase
            // 
            this.bankDataBase.DataSetName = "BankDataBase";
            this.bankDataBase.SchemaSerializationMode = System.Data.SchemaSerializationMode.IncludeSchema;
            // 
            // countryBindingSource
            // 
            this.countryBindingSource.DataMember = "Country";
            this.countryBindingSource.DataSource = this.bankDataBase;
            // 
            // countryTableAdapter
            // 
            this.countryTableAdapter.ClearBeforeFill = true;
            // 
            // tipeBindingSource
            // 
            this.tipeBindingSource.DataMember = "Tipe";
            this.tipeBindingSource.DataSource = this.bankDataBase;
            // 
            // tipeTableAdapter
            // 
            this.tipeTableAdapter.ClearBeforeFill = true;
            // 
            // add_comand
            // 
            this.add_comand.Location = new System.Drawing.Point(37, 168);
            this.add_comand.Name = "add_comand";
            this.add_comand.Size = new System.Drawing.Size(75, 23);
            this.add_comand.TabIndex = 20;
            this.add_comand.Text = "ADD";
            this.add_comand.UseVisualStyleBackColor = true;
            this.add_comand.Click += new System.EventHandler(this.add_comand_Click);
            // 
            // investorBindingSource
            // 
            this.investorBindingSource.DataMember = "Investor";
            this.investorBindingSource.DataSource = this.bankDataBase;
            // 
            // investorTableAdapter
            // 
            this.investorTableAdapter.ClearBeforeFill = true;
            // 
            // AddInvestor
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(284, 262);
            this.Controls.Add(this.add_comand);
            this.Controls.Add(label1);
            this.Controls.Add(this.type_comboBox);
            this.Controls.Add(investorLabel);
            this.Controls.Add(this.Country_comboBox);
            this.Controls.Add(this.name_add);
            this.Controls.Add(New_company);
            this.Name = "AddInvestor";
            this.Text = "AddInvestor";
            this.Load += new System.EventHandler(this.AddInvestor_Load);
            ((System.ComponentModel.ISupportInitialize)(this.bankDataBase)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.countryBindingSource)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.tipeBindingSource)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.investorBindingSource)).EndInit();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.TextBox name_add;
        private System.Windows.Forms.ComboBox Country_comboBox;
        private System.Windows.Forms.ComboBox type_comboBox;
        private BankDataBase bankDataBase;
        private System.Windows.Forms.BindingSource countryBindingSource;
        private BankDataBaseTableAdapters.CountryTableAdapter countryTableAdapter;
        private System.Windows.Forms.BindingSource tipeBindingSource;
        private BankDataBaseTableAdapters.TipeTableAdapter tipeTableAdapter;
        private System.Windows.Forms.Button add_comand;
        private System.Windows.Forms.BindingSource investorBindingSource;
        private BankDataBaseTableAdapters.InvestorTableAdapter investorTableAdapter;
    }
}