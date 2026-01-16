namespace MANUUFinance
{
    partial class frmAccount
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
            this.groupBox2 = new System.Windows.Forms.GroupBox();
            this.btnClose = new System.Windows.Forms.Button();
            this.btnClear = new System.Windows.Forms.Button();
            this.btnDelete = new System.Windows.Forms.Button();
            this.btnUpdate = new System.Windows.Forms.Button();
            this.btnAdd = new System.Windows.Forms.Button();
            this.groupBox1 = new System.Windows.Forms.GroupBox();
            this.label3 = new System.Windows.Forms.Label();
            this.comboBankAccount = new System.Windows.Forms.ComboBox();
            this.comboSL3 = new System.Windows.Forms.ComboBox();
            this.label2 = new System.Windows.Forms.Label();
            this.comboSL2 = new System.Windows.Forms.ComboBox();
            this.comboSL1 = new System.Windows.Forms.ComboBox();
            this.txtAccountOrder = new System.Windows.Forms.TextBox();
            this.label1 = new System.Windows.Forms.Label();
            this.label8 = new System.Windows.Forms.Label();
            this.label6 = new System.Windows.Forms.Label();
            this.groupBox3 = new System.Windows.Forms.GroupBox();
            this.radioBtnAccountInActive = new System.Windows.Forms.RadioButton();
            this.radioBtnAccountActive = new System.Windows.Forms.RadioButton();
            this.label7 = new System.Windows.Forms.Label();
            this.label4 = new System.Windows.Forms.Label();
            this.txtPKACID = new System.Windows.Forms.TextBox();
            this.groupBox4 = new System.Windows.Forms.GroupBox();
            this.label12 = new System.Windows.Forms.Label();
            this.txtAccountNameSearch = new System.Windows.Forms.TextBox();
            this.label11 = new System.Windows.Forms.Label();
            this.txtSL3Search = new System.Windows.Forms.TextBox();
            this.txtSL3SearchLabel = new System.Windows.Forms.Label();
            this.txtBankAccSearch = new System.Windows.Forms.TextBox();
            this.btnSearch = new System.Windows.Forms.Button();
            this.btnClearSearch = new System.Windows.Forms.Button();
            this.label10 = new System.Windows.Forms.Label();
            this.txtSL1Seacrh = new System.Windows.Forms.TextBox();
            this.label9 = new System.Windows.Forms.Label();
            this.txtSL2Search = new System.Windows.Forms.TextBox();
            this.label5 = new System.Windows.Forms.Label();
            this.DGVAccounts = new System.Windows.Forms.DataGridView();
            this.txtAccountName = new System.Windows.Forms.TextBox();
            this.label13 = new System.Windows.Forms.Label();
            this.comboAccountType = new System.Windows.Forms.ComboBox();
            this.SL1Name = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.SL2Name = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.SL3Name = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.BankAccountType = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.FKBankAccountID = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.pKACIDDataGridViewTextBoxColumn = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.fKSL3IDDataGridViewTextBoxColumn = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.accountNameDataGridViewTextBoxColumn = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.acOrderDataGridViewTextBoxColumn = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.acActiveDataGridViewCheckBoxColumn = new System.Windows.Forms.DataGridViewCheckBoxColumn();
            this.bankNameDataGridViewTextBoxColumn = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.accountNumberDataGridViewTextBoxColumn = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.sL1IDDataGridViewTextBoxColumn = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.pKSL2DataGridViewTextBoxColumn = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.accountTypeDataGridViewTextBoxColumn = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.accountsViewBindingSource = new System.Windows.Forms.BindingSource(this.components);
            this.financeDataSet = new MANUUFinance.FinanceDataSet();
            this.accountsViewTableAdapter = new MANUUFinance.FinanceDataSetTableAdapters.AccountsViewTableAdapter();
            this.groupBox2.SuspendLayout();
            this.groupBox1.SuspendLayout();
            this.groupBox3.SuspendLayout();
            this.groupBox4.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)(this.DGVAccounts)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.accountsViewBindingSource)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.financeDataSet)).BeginInit();
            this.SuspendLayout();
            // 
            // groupBox2
            // 
            this.groupBox2.Controls.Add(this.btnClose);
            this.groupBox2.Controls.Add(this.btnClear);
            this.groupBox2.Controls.Add(this.btnDelete);
            this.groupBox2.Controls.Add(this.btnUpdate);
            this.groupBox2.Controls.Add(this.btnAdd);
            this.groupBox2.Location = new System.Drawing.Point(190, 297);
            this.groupBox2.Name = "groupBox2";
            this.groupBox2.Size = new System.Drawing.Size(722, 70);
            this.groupBox2.TabIndex = 24;
            this.groupBox2.TabStop = false;
            // 
            // btnClose
            // 
            this.btnClose.Location = new System.Drawing.Point(568, 19);
            this.btnClose.Name = "btnClose";
            this.btnClose.Size = new System.Drawing.Size(133, 38);
            this.btnClose.TabIndex = 4;
            this.btnClose.Text = "Close Form";
            this.btnClose.UseVisualStyleBackColor = true;
            this.btnClose.Click += new System.EventHandler(this.btnClose_Click);
            // 
            // btnClear
            // 
            this.btnClear.Location = new System.Drawing.Point(429, 19);
            this.btnClear.Name = "btnClear";
            this.btnClear.Size = new System.Drawing.Size(133, 38);
            this.btnClear.TabIndex = 3;
            this.btnClear.Text = "Clear Controls";
            this.btnClear.UseVisualStyleBackColor = true;
            this.btnClear.Click += new System.EventHandler(this.btnClear_Click);
            // 
            // btnDelete
            // 
            this.btnDelete.Location = new System.Drawing.Point(290, 19);
            this.btnDelete.Name = "btnDelete";
            this.btnDelete.Size = new System.Drawing.Size(133, 38);
            this.btnDelete.TabIndex = 2;
            this.btnDelete.Text = "Delete Record";
            this.btnDelete.UseVisualStyleBackColor = true;
            this.btnDelete.Click += new System.EventHandler(this.btnDelete_Click);
            // 
            // btnUpdate
            // 
            this.btnUpdate.Location = new System.Drawing.Point(151, 19);
            this.btnUpdate.Name = "btnUpdate";
            this.btnUpdate.Size = new System.Drawing.Size(133, 38);
            this.btnUpdate.TabIndex = 1;
            this.btnUpdate.Text = "Update Record";
            this.btnUpdate.UseVisualStyleBackColor = true;
            this.btnUpdate.Click += new System.EventHandler(this.btnUpdate_Click);
            // 
            // btnAdd
            // 
            this.btnAdd.Location = new System.Drawing.Point(12, 19);
            this.btnAdd.Name = "btnAdd";
            this.btnAdd.Size = new System.Drawing.Size(133, 38);
            this.btnAdd.TabIndex = 0;
            this.btnAdd.Text = "Add Record";
            this.btnAdd.UseVisualStyleBackColor = true;
            this.btnAdd.Click += new System.EventHandler(this.btnAdd_Click);
            // 
            // groupBox1
            // 
            this.groupBox1.Controls.Add(this.comboAccountType);
            this.groupBox1.Controls.Add(this.txtAccountName);
            this.groupBox1.Controls.Add(this.label13);
            this.groupBox1.Controls.Add(this.label3);
            this.groupBox1.Controls.Add(this.comboBankAccount);
            this.groupBox1.Controls.Add(this.comboSL3);
            this.groupBox1.Controls.Add(this.label2);
            this.groupBox1.Controls.Add(this.comboSL2);
            this.groupBox1.Controls.Add(this.comboSL1);
            this.groupBox1.Controls.Add(this.txtAccountOrder);
            this.groupBox1.Controls.Add(this.label1);
            this.groupBox1.Controls.Add(this.label8);
            this.groupBox1.Controls.Add(this.label6);
            this.groupBox1.Controls.Add(this.groupBox3);
            this.groupBox1.Controls.Add(this.label7);
            this.groupBox1.Controls.Add(this.label4);
            this.groupBox1.Controls.Add(this.txtPKACID);
            this.groupBox1.ForeColor = System.Drawing.Color.Maroon;
            this.groupBox1.Location = new System.Drawing.Point(33, 37);
            this.groupBox1.Name = "groupBox1";
            this.groupBox1.Size = new System.Drawing.Size(1411, 235);
            this.groupBox1.TabIndex = 23;
            this.groupBox1.TabStop = false;
            this.groupBox1.Text = "Account";
            // 
            // label3
            // 
            this.label3.AutoSize = true;
            this.label3.Location = new System.Drawing.Point(795, 125);
            this.label3.Name = "label3";
            this.label3.Size = new System.Drawing.Size(109, 20);
            this.label3.TabIndex = 26;
            this.label3.Text = "Bank Account";
            // 
            // comboBankAccount
            // 
            this.comboBankAccount.AutoCompleteMode = System.Windows.Forms.AutoCompleteMode.SuggestAppend;
            this.comboBankAccount.AutoCompleteSource = System.Windows.Forms.AutoCompleteSource.ListItems;
            this.comboBankAccount.FormattingEnabled = true;
            this.comboBankAccount.Location = new System.Drawing.Point(926, 122);
            this.comboBankAccount.Name = "comboBankAccount";
            this.comboBankAccount.Size = new System.Drawing.Size(472, 28);
            this.comboBankAccount.TabIndex = 4;
            // 
            // comboSL3
            // 
            this.comboSL3.AutoCompleteMode = System.Windows.Forms.AutoCompleteMode.SuggestAppend;
            this.comboSL3.AutoCompleteSource = System.Windows.Forms.AutoCompleteSource.ListItems;
            this.comboSL3.FormattingEnabled = true;
            this.comboSL3.Location = new System.Drawing.Point(217, 78);
            this.comboSL3.Name = "comboSL3";
            this.comboSL3.Size = new System.Drawing.Size(472, 28);
            this.comboSL3.TabIndex = 2;
            // 
            // label2
            // 
            this.label2.AutoSize = true;
            this.label2.Location = new System.Drawing.Point(86, 81);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(84, 20);
            this.label2.TabIndex = 25;
            this.label2.Text = "SL3 Name";
            // 
            // comboSL2
            // 
            this.comboSL2.AutoCompleteMode = System.Windows.Forms.AutoCompleteMode.SuggestAppend;
            this.comboSL2.AutoCompleteSource = System.Windows.Forms.AutoCompleteSource.ListItems;
            this.comboSL2.FormattingEnabled = true;
            this.comboSL2.Location = new System.Drawing.Point(926, 37);
            this.comboSL2.Name = "comboSL2";
            this.comboSL2.Size = new System.Drawing.Size(472, 28);
            this.comboSL2.TabIndex = 1;
            this.comboSL2.SelectedIndexChanged += new System.EventHandler(this.comboSL2_SelectedIndexChanged);
            // 
            // comboSL1
            // 
            this.comboSL1.AutoCompleteMode = System.Windows.Forms.AutoCompleteMode.SuggestAppend;
            this.comboSL1.AutoCompleteSource = System.Windows.Forms.AutoCompleteSource.ListItems;
            this.comboSL1.FormattingEnabled = true;
            this.comboSL1.Location = new System.Drawing.Point(217, 37);
            this.comboSL1.Name = "comboSL1";
            this.comboSL1.Size = new System.Drawing.Size(472, 28);
            this.comboSL1.TabIndex = 0;
            this.comboSL1.SelectedIndexChanged += new System.EventHandler(this.comboSL1_SelectedIndexChanged);
            // 
            // txtAccountOrder
            // 
            this.txtAccountOrder.Location = new System.Drawing.Point(926, 167);
            this.txtAccountOrder.Name = "txtAccountOrder";
            this.txtAccountOrder.Size = new System.Drawing.Size(100, 26);
            this.txtAccountOrder.TabIndex = 5;
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Location = new System.Drawing.Point(795, 168);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(112, 20);
            this.label1.TabIndex = 20;
            this.label1.Text = "Account Order";
            // 
            // label8
            // 
            this.label8.AutoSize = true;
            this.label8.Location = new System.Drawing.Point(86, 171);
            this.label8.Name = "label8";
            this.label8.Size = new System.Drawing.Size(115, 20);
            this.label8.TabIndex = 19;
            this.label8.Text = "Account Active";
            // 
            // label6
            // 
            this.label6.AutoSize = true;
            this.label6.Location = new System.Drawing.Point(86, 40);
            this.label6.Name = "label6";
            this.label6.Size = new System.Drawing.Size(84, 20);
            this.label6.TabIndex = 13;
            this.label6.Text = "SL1 Name";
            // 
            // groupBox3
            // 
            this.groupBox3.Controls.Add(this.radioBtnAccountInActive);
            this.groupBox3.Controls.Add(this.radioBtnAccountActive);
            this.groupBox3.Location = new System.Drawing.Point(217, 153);
            this.groupBox3.Name = "groupBox3";
            this.groupBox3.Size = new System.Drawing.Size(273, 45);
            this.groupBox3.TabIndex = 19;
            this.groupBox3.TabStop = false;
            // 
            // radioBtnAccountInActive
            // 
            this.radioBtnAccountInActive.AutoSize = true;
            this.radioBtnAccountInActive.Location = new System.Drawing.Point(148, 15);
            this.radioBtnAccountInActive.Name = "radioBtnAccountInActive";
            this.radioBtnAccountInActive.Size = new System.Drawing.Size(89, 24);
            this.radioBtnAccountInActive.TabIndex = 1;
            this.radioBtnAccountInActive.TabStop = true;
            this.radioBtnAccountInActive.Text = "Inactive";
            this.radioBtnAccountInActive.UseVisualStyleBackColor = true;
            // 
            // radioBtnAccountActive
            // 
            this.radioBtnAccountActive.AutoSize = true;
            this.radioBtnAccountActive.Location = new System.Drawing.Point(40, 15);
            this.radioBtnAccountActive.Name = "radioBtnAccountActive";
            this.radioBtnAccountActive.Size = new System.Drawing.Size(77, 24);
            this.radioBtnAccountActive.TabIndex = 0;
            this.radioBtnAccountActive.TabStop = true;
            this.radioBtnAccountActive.Text = "Active";
            this.radioBtnAccountActive.UseVisualStyleBackColor = true;
            // 
            // label7
            // 
            this.label7.AutoSize = true;
            this.label7.Location = new System.Drawing.Point(795, 40);
            this.label7.Name = "label7";
            this.label7.Size = new System.Drawing.Size(84, 20);
            this.label7.TabIndex = 16;
            this.label7.Text = "SL2 Name";
            // 
            // label4
            // 
            this.label4.AutoSize = true;
            this.label4.Location = new System.Drawing.Point(795, 80);
            this.label4.Name = "label4";
            this.label4.Size = new System.Drawing.Size(106, 20);
            this.label4.TabIndex = 11;
            this.label4.Text = "Account Type";
            // 
            // txtPKACID
            // 
            this.txtPKACID.Location = new System.Drawing.Point(1282, 184);
            this.txtPKACID.Name = "txtPKACID";
            this.txtPKACID.Size = new System.Drawing.Size(100, 26);
            this.txtPKACID.TabIndex = 1;
            // 
            // groupBox4
            // 
            this.groupBox4.Controls.Add(this.label12);
            this.groupBox4.Controls.Add(this.txtAccountNameSearch);
            this.groupBox4.Controls.Add(this.label11);
            this.groupBox4.Controls.Add(this.txtSL3Search);
            this.groupBox4.Controls.Add(this.txtSL3SearchLabel);
            this.groupBox4.Controls.Add(this.txtBankAccSearch);
            this.groupBox4.Controls.Add(this.btnSearch);
            this.groupBox4.Controls.Add(this.btnClearSearch);
            this.groupBox4.Controls.Add(this.label10);
            this.groupBox4.Controls.Add(this.txtSL1Seacrh);
            this.groupBox4.Controls.Add(this.label9);
            this.groupBox4.Controls.Add(this.txtSL2Search);
            this.groupBox4.Controls.Add(this.label5);
            this.groupBox4.Location = new System.Drawing.Point(1088, 382);
            this.groupBox4.Name = "groupBox4";
            this.groupBox4.Size = new System.Drawing.Size(356, 402);
            this.groupBox4.TabIndex = 25;
            this.groupBox4.TabStop = false;
            this.groupBox4.Text = "Grid Filter";
            // 
            // label12
            // 
            this.label12.AutoSize = true;
            this.label12.Location = new System.Drawing.Point(14, 262);
            this.label12.Name = "label12";
            this.label12.Size = new System.Drawing.Size(114, 20);
            this.label12.TabIndex = 26;
            this.label12.Text = "Account Name";
            // 
            // txtAccountNameSearch
            // 
            this.txtAccountNameSearch.Location = new System.Drawing.Point(18, 287);
            this.txtAccountNameSearch.Name = "txtAccountNameSearch";
            this.txtAccountNameSearch.Size = new System.Drawing.Size(324, 26);
            this.txtAccountNameSearch.TabIndex = 4;
            // 
            // label11
            // 
            this.label11.AutoSize = true;
            this.label11.Location = new System.Drawing.Point(14, 205);
            this.label11.Name = "label11";
            this.label11.Size = new System.Drawing.Size(109, 20);
            this.label11.TabIndex = 24;
            this.label11.Text = "Bank Account";
            // 
            // txtSL3Search
            // 
            this.txtSL3Search.Location = new System.Drawing.Point(18, 171);
            this.txtSL3Search.Name = "txtSL3Search";
            this.txtSL3Search.Size = new System.Drawing.Size(324, 26);
            this.txtSL3Search.TabIndex = 2;
            // 
            // txtSL3SearchLabel
            // 
            this.txtSL3SearchLabel.AutoSize = true;
            this.txtSL3SearchLabel.Location = new System.Drawing.Point(16, 148);
            this.txtSL3SearchLabel.Name = "txtSL3SearchLabel";
            this.txtSL3SearchLabel.Size = new System.Drawing.Size(84, 20);
            this.txtSL3SearchLabel.TabIndex = 23;
            this.txtSL3SearchLabel.Text = "SL3 Name";
            // 
            // txtBankAccSearch
            // 
            this.txtBankAccSearch.Location = new System.Drawing.Point(18, 228);
            this.txtBankAccSearch.Name = "txtBankAccSearch";
            this.txtBankAccSearch.Size = new System.Drawing.Size(324, 26);
            this.txtBankAccSearch.TabIndex = 3;
            // 
            // btnSearch
            // 
            this.btnSearch.Location = new System.Drawing.Point(178, 342);
            this.btnSearch.Name = "btnSearch";
            this.btnSearch.Size = new System.Drawing.Size(133, 38);
            this.btnSearch.TabIndex = 6;
            this.btnSearch.Text = "Search";
            this.btnSearch.UseVisualStyleBackColor = true;
            this.btnSearch.Click += new System.EventHandler(this.btnSearch_Click);
            // 
            // btnClearSearch
            // 
            this.btnClearSearch.Location = new System.Drawing.Point(33, 342);
            this.btnClearSearch.Name = "btnClearSearch";
            this.btnClearSearch.Size = new System.Drawing.Size(133, 38);
            this.btnClearSearch.TabIndex = 5;
            this.btnClearSearch.Text = "Clear Controls";
            this.btnClearSearch.UseVisualStyleBackColor = true;
            this.btnClearSearch.Click += new System.EventHandler(this.btnClearSearch_Click);
            // 
            // label10
            // 
            this.label10.AutoSize = true;
            this.label10.Location = new System.Drawing.Point(15, 91);
            this.label10.Name = "label10";
            this.label10.Size = new System.Drawing.Size(84, 20);
            this.label10.TabIndex = 18;
            this.label10.Text = "SL2 Name";
            // 
            // txtSL1Seacrh
            // 
            this.txtSL1Seacrh.Location = new System.Drawing.Point(19, 57);
            this.txtSL1Seacrh.Name = "txtSL1Seacrh";
            this.txtSL1Seacrh.Size = new System.Drawing.Size(324, 26);
            this.txtSL1Seacrh.TabIndex = 0;
            // 
            // label9
            // 
            this.label9.AutoSize = true;
            this.label9.Location = new System.Drawing.Point(17, 34);
            this.label9.Name = "label9";
            this.label9.Size = new System.Drawing.Size(84, 20);
            this.label9.TabIndex = 17;
            this.label9.Text = "SL1 Name";
            // 
            // txtSL2Search
            // 
            this.txtSL2Search.Location = new System.Drawing.Point(19, 114);
            this.txtSL2Search.Name = "txtSL2Search";
            this.txtSL2Search.Size = new System.Drawing.Size(324, 26);
            this.txtSL2Search.TabIndex = 1;
            // 
            // label5
            // 
            this.label5.AutoSize = true;
            this.label5.Location = new System.Drawing.Point(-123, 136);
            this.label5.Name = "label5";
            this.label5.Size = new System.Drawing.Size(114, 20);
            this.label5.TabIndex = 13;
            this.label5.Text = "Account Name";
            // 
            // DGVAccounts
            // 
            this.DGVAccounts.AllowUserToAddRows = false;
            this.DGVAccounts.AllowUserToDeleteRows = false;
            this.DGVAccounts.AllowUserToResizeRows = false;
            this.DGVAccounts.AutoGenerateColumns = false;
            this.DGVAccounts.AutoSizeRowsMode = System.Windows.Forms.DataGridViewAutoSizeRowsMode.AllCells;
            this.DGVAccounts.ColumnHeadersHeightSizeMode = System.Windows.Forms.DataGridViewColumnHeadersHeightSizeMode.AutoSize;
            this.DGVAccounts.Columns.AddRange(new System.Windows.Forms.DataGridViewColumn[] {
            this.pKACIDDataGridViewTextBoxColumn,
            this.SL1Name,
            this.SL2Name,
            this.SL3Name,
            this.fKSL3IDDataGridViewTextBoxColumn,
            this.accountNameDataGridViewTextBoxColumn,
            this.acOrderDataGridViewTextBoxColumn,
            this.acActiveDataGridViewCheckBoxColumn,
            this.bankNameDataGridViewTextBoxColumn,
            this.BankAccountType,
            this.accountNumberDataGridViewTextBoxColumn,
            this.sL1IDDataGridViewTextBoxColumn,
            this.pKSL2DataGridViewTextBoxColumn,
            this.FKBankAccountID,
            this.accountTypeDataGridViewTextBoxColumn});
            this.DGVAccounts.DataSource = this.accountsViewBindingSource;
            this.DGVAccounts.Location = new System.Drawing.Point(33, 396);
            this.DGVAccounts.MultiSelect = false;
            this.DGVAccounts.Name = "DGVAccounts";
            this.DGVAccounts.ReadOnly = true;
            this.DGVAccounts.RowTemplate.Height = 28;
            this.DGVAccounts.Size = new System.Drawing.Size(1005, 388);
            this.DGVAccounts.TabIndex = 26;
            this.DGVAccounts.CellClick += new System.Windows.Forms.DataGridViewCellEventHandler(this.DGVAccounts_CellClick);
            // 
            // txtAccountName
            // 
            this.txtAccountName.Location = new System.Drawing.Point(217, 119);
            this.txtAccountName.Name = "txtAccountName";
            this.txtAccountName.Size = new System.Drawing.Size(472, 26);
            this.txtAccountName.TabIndex = 27;
            // 
            // label13
            // 
            this.label13.AutoSize = true;
            this.label13.Location = new System.Drawing.Point(86, 119);
            this.label13.Name = "label13";
            this.label13.Size = new System.Drawing.Size(114, 20);
            this.label13.TabIndex = 28;
            this.label13.Text = "Account Name";
            // 
            // comboAccountType
            // 
            this.comboAccountType.AutoCompleteMode = System.Windows.Forms.AutoCompleteMode.SuggestAppend;
            this.comboAccountType.AutoCompleteSource = System.Windows.Forms.AutoCompleteSource.ListItems;
            this.comboAccountType.FormattingEnabled = true;
            this.comboAccountType.Location = new System.Drawing.Point(926, 78);
            this.comboAccountType.Name = "comboAccountType";
            this.comboAccountType.Size = new System.Drawing.Size(472, 28);
            this.comboAccountType.TabIndex = 29;
            // 
            // SL1Name
            // 
            this.SL1Name.DataPropertyName = "SL1Name";
            this.SL1Name.HeaderText = "SL1Name";
            this.SL1Name.Name = "SL1Name";
            this.SL1Name.ReadOnly = true;
            // 
            // SL2Name
            // 
            this.SL2Name.DataPropertyName = "SL2Name";
            this.SL2Name.HeaderText = "SL2Name";
            this.SL2Name.Name = "SL2Name";
            this.SL2Name.ReadOnly = true;
            // 
            // SL3Name
            // 
            this.SL3Name.DataPropertyName = "SL3Name";
            this.SL3Name.HeaderText = "SL3Name";
            this.SL3Name.Name = "SL3Name";
            this.SL3Name.ReadOnly = true;
            // 
            // BankAccountType
            // 
            this.BankAccountType.DataPropertyName = "BankAccountType";
            this.BankAccountType.HeaderText = "BankAccountType";
            this.BankAccountType.Name = "BankAccountType";
            this.BankAccountType.ReadOnly = true;
            // 
            // FKBankAccountID
            // 
            this.FKBankAccountID.DataPropertyName = "FKBankAccountID";
            this.FKBankAccountID.HeaderText = "FKBankAccountID";
            this.FKBankAccountID.Name = "FKBankAccountID";
            this.FKBankAccountID.ReadOnly = true;
            this.FKBankAccountID.Visible = false;
            // 
            // pKACIDDataGridViewTextBoxColumn
            // 
            this.pKACIDDataGridViewTextBoxColumn.DataPropertyName = "PKACID";
            this.pKACIDDataGridViewTextBoxColumn.HeaderText = "PKACID";
            this.pKACIDDataGridViewTextBoxColumn.Name = "pKACIDDataGridViewTextBoxColumn";
            this.pKACIDDataGridViewTextBoxColumn.ReadOnly = true;
            this.pKACIDDataGridViewTextBoxColumn.Visible = false;
            // 
            // fKSL3IDDataGridViewTextBoxColumn
            // 
            this.fKSL3IDDataGridViewTextBoxColumn.DataPropertyName = "FKSL3ID";
            this.fKSL3IDDataGridViewTextBoxColumn.HeaderText = "FKSL3ID";
            this.fKSL3IDDataGridViewTextBoxColumn.Name = "fKSL3IDDataGridViewTextBoxColumn";
            this.fKSL3IDDataGridViewTextBoxColumn.ReadOnly = true;
            this.fKSL3IDDataGridViewTextBoxColumn.Visible = false;
            // 
            // accountNameDataGridViewTextBoxColumn
            // 
            this.accountNameDataGridViewTextBoxColumn.DataPropertyName = "AccountName";
            this.accountNameDataGridViewTextBoxColumn.HeaderText = "AccountName";
            this.accountNameDataGridViewTextBoxColumn.Name = "accountNameDataGridViewTextBoxColumn";
            this.accountNameDataGridViewTextBoxColumn.ReadOnly = true;
            // 
            // acOrderDataGridViewTextBoxColumn
            // 
            this.acOrderDataGridViewTextBoxColumn.DataPropertyName = "AcOrder";
            this.acOrderDataGridViewTextBoxColumn.HeaderText = "Order";
            this.acOrderDataGridViewTextBoxColumn.Name = "acOrderDataGridViewTextBoxColumn";
            this.acOrderDataGridViewTextBoxColumn.ReadOnly = true;
            // 
            // acActiveDataGridViewCheckBoxColumn
            // 
            this.acActiveDataGridViewCheckBoxColumn.DataPropertyName = "AcActive";
            this.acActiveDataGridViewCheckBoxColumn.HeaderText = "Active";
            this.acActiveDataGridViewCheckBoxColumn.Name = "acActiveDataGridViewCheckBoxColumn";
            this.acActiveDataGridViewCheckBoxColumn.ReadOnly = true;
            // 
            // bankNameDataGridViewTextBoxColumn
            // 
            this.bankNameDataGridViewTextBoxColumn.DataPropertyName = "BankName";
            this.bankNameDataGridViewTextBoxColumn.HeaderText = "BankName";
            this.bankNameDataGridViewTextBoxColumn.Name = "bankNameDataGridViewTextBoxColumn";
            this.bankNameDataGridViewTextBoxColumn.ReadOnly = true;
            // 
            // accountNumberDataGridViewTextBoxColumn
            // 
            this.accountNumberDataGridViewTextBoxColumn.DataPropertyName = "AccountNumber";
            this.accountNumberDataGridViewTextBoxColumn.HeaderText = "AccountNumber";
            this.accountNumberDataGridViewTextBoxColumn.Name = "accountNumberDataGridViewTextBoxColumn";
            this.accountNumberDataGridViewTextBoxColumn.ReadOnly = true;
            // 
            // sL1IDDataGridViewTextBoxColumn
            // 
            this.sL1IDDataGridViewTextBoxColumn.DataPropertyName = "SL1ID";
            this.sL1IDDataGridViewTextBoxColumn.HeaderText = "SL1ID";
            this.sL1IDDataGridViewTextBoxColumn.Name = "sL1IDDataGridViewTextBoxColumn";
            this.sL1IDDataGridViewTextBoxColumn.ReadOnly = true;
            this.sL1IDDataGridViewTextBoxColumn.Visible = false;
            // 
            // pKSL2DataGridViewTextBoxColumn
            // 
            this.pKSL2DataGridViewTextBoxColumn.DataPropertyName = "PKSL2";
            this.pKSL2DataGridViewTextBoxColumn.HeaderText = "PKSL2";
            this.pKSL2DataGridViewTextBoxColumn.Name = "pKSL2DataGridViewTextBoxColumn";
            this.pKSL2DataGridViewTextBoxColumn.ReadOnly = true;
            this.pKSL2DataGridViewTextBoxColumn.Visible = false;
            // 
            // accountTypeDataGridViewTextBoxColumn
            // 
            this.accountTypeDataGridViewTextBoxColumn.DataPropertyName = "AccountType";
            this.accountTypeDataGridViewTextBoxColumn.HeaderText = "AccountType";
            this.accountTypeDataGridViewTextBoxColumn.Name = "accountTypeDataGridViewTextBoxColumn";
            this.accountTypeDataGridViewTextBoxColumn.ReadOnly = true;
            this.accountTypeDataGridViewTextBoxColumn.Visible = false;
            // 
            // accountsViewBindingSource
            // 
            this.accountsViewBindingSource.DataMember = "AccountsView";
            this.accountsViewBindingSource.DataSource = this.financeDataSet;
            // 
            // financeDataSet
            // 
            this.financeDataSet.DataSetName = "FinanceDataSet";
            this.financeDataSet.SchemaSerializationMode = System.Data.SchemaSerializationMode.IncludeSchema;
            // 
            // accountsViewTableAdapter
            // 
            this.accountsViewTableAdapter.ClearBeforeFill = true;
            // 
            // frmAccount
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(9F, 20F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(1578, 844);
            this.Controls.Add(this.DGVAccounts);
            this.Controls.Add(this.groupBox4);
            this.Controls.Add(this.groupBox2);
            this.Controls.Add(this.groupBox1);
            this.Name = "frmAccount";
            this.Text = "Account";
            this.Load += new System.EventHandler(this.frmAccount_Load);
            this.groupBox2.ResumeLayout(false);
            this.groupBox1.ResumeLayout(false);
            this.groupBox1.PerformLayout();
            this.groupBox3.ResumeLayout(false);
            this.groupBox3.PerformLayout();
            this.groupBox4.ResumeLayout(false);
            this.groupBox4.PerformLayout();
            ((System.ComponentModel.ISupportInitialize)(this.DGVAccounts)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.accountsViewBindingSource)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.financeDataSet)).EndInit();
            this.ResumeLayout(false);

        }

        #endregion

        private System.Windows.Forms.GroupBox groupBox2;
        private System.Windows.Forms.Button btnClose;
        private System.Windows.Forms.Button btnClear;
        private System.Windows.Forms.Button btnDelete;
        private System.Windows.Forms.Button btnUpdate;
        private System.Windows.Forms.Button btnAdd;
        private System.Windows.Forms.GroupBox groupBox1;
        private System.Windows.Forms.ComboBox comboSL3;
        private System.Windows.Forms.Label label2;
        private System.Windows.Forms.ComboBox comboSL2;
        private System.Windows.Forms.ComboBox comboSL1;
        private System.Windows.Forms.TextBox txtAccountOrder;
        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.Label label8;
        private System.Windows.Forms.Label label6;
        private System.Windows.Forms.GroupBox groupBox3;
        private System.Windows.Forms.RadioButton radioBtnAccountInActive;
        private System.Windows.Forms.RadioButton radioBtnAccountActive;
        private System.Windows.Forms.Label label7;
        private System.Windows.Forms.Label label4;
        private System.Windows.Forms.TextBox txtPKACID;
        private System.Windows.Forms.GroupBox groupBox4;
        private System.Windows.Forms.Label label3;
        private System.Windows.Forms.ComboBox comboBankAccount;
        private System.Windows.Forms.DataGridView DGVAccounts;
        private FinanceDataSet financeDataSet;
        private System.Windows.Forms.BindingSource accountsViewBindingSource;
        private FinanceDataSetTableAdapters.AccountsViewTableAdapter accountsViewTableAdapter;
        private System.Windows.Forms.Button btnSearch;
        private System.Windows.Forms.Button btnClearSearch;
        private System.Windows.Forms.Label label10;
        private System.Windows.Forms.TextBox txtSL1Seacrh;
        private System.Windows.Forms.Label label9;
        private System.Windows.Forms.TextBox txtSL2Search;
        private System.Windows.Forms.Label label5;
        private System.Windows.Forms.Label label11;
        private System.Windows.Forms.TextBox txtSL3Search;
        private System.Windows.Forms.Label txtSL3SearchLabel;
        private System.Windows.Forms.TextBox txtBankAccSearch;
        private System.Windows.Forms.Label label12;
        private System.Windows.Forms.TextBox txtAccountNameSearch;
        private System.Windows.Forms.ComboBox comboAccountType;
        private System.Windows.Forms.TextBox txtAccountName;
        private System.Windows.Forms.Label label13;
        private System.Windows.Forms.DataGridViewTextBoxColumn pKACIDDataGridViewTextBoxColumn;
        private System.Windows.Forms.DataGridViewTextBoxColumn SL1Name;
        private System.Windows.Forms.DataGridViewTextBoxColumn SL2Name;
        private System.Windows.Forms.DataGridViewTextBoxColumn SL3Name;
        private System.Windows.Forms.DataGridViewTextBoxColumn fKSL3IDDataGridViewTextBoxColumn;
        private System.Windows.Forms.DataGridViewTextBoxColumn accountNameDataGridViewTextBoxColumn;
        private System.Windows.Forms.DataGridViewTextBoxColumn acOrderDataGridViewTextBoxColumn;
        private System.Windows.Forms.DataGridViewCheckBoxColumn acActiveDataGridViewCheckBoxColumn;
        private System.Windows.Forms.DataGridViewTextBoxColumn bankNameDataGridViewTextBoxColumn;
        private System.Windows.Forms.DataGridViewTextBoxColumn BankAccountType;
        private System.Windows.Forms.DataGridViewTextBoxColumn accountNumberDataGridViewTextBoxColumn;
        private System.Windows.Forms.DataGridViewTextBoxColumn sL1IDDataGridViewTextBoxColumn;
        private System.Windows.Forms.DataGridViewTextBoxColumn pKSL2DataGridViewTextBoxColumn;
        private System.Windows.Forms.DataGridViewTextBoxColumn FKBankAccountID;
        private System.Windows.Forms.DataGridViewTextBoxColumn accountTypeDataGridViewTextBoxColumn;
    }
}