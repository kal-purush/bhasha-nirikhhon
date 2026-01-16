namespace SDP
{
    partial class Account
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
            System.Windows.Forms.DataGridViewCellStyle dataGridViewCellStyle1 = new System.Windows.Forms.DataGridViewCellStyle();
            System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(Account));
            this.AccountView = new System.Windows.Forms.DataGridView();
            this.staff_id = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.staff_password = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.staff_name = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.staff_email = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.staff_access = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.retail = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.active = new System.Windows.Forms.DataGridViewCheckBoxColumn();
            this.imageList1 = new System.Windows.Forms.ImageList(this.components);
            this.staff_name_text = new System.Windows.Forms.TextBox();
            this.staff_name_label = new System.Windows.Forms.Label();
            this.staff_pwd_text = new System.Windows.Forms.TextBox();
            this.staff_pwd_label = new System.Windows.Forms.Label();
            this.staff_email_label = new System.Windows.Forms.Label();
            this.staff_email_text = new System.Windows.Forms.TextBox();
            this.staff_role_label = new System.Windows.Forms.Label();
            this.staff_role_text = new System.Windows.Forms.ComboBox();
            this.dataAddBtn = new FontAwesome.Sharp.IconButton();
            this.searchTxt = new System.Windows.Forms.TextBox();
            this.search_name_label = new System.Windows.Forms.Label();
            this.searchBtn = new FontAwesome.Sharp.IconButton();
            this.accountTitle = new System.Windows.Forms.Label();
            this.createAccountPanel = new System.Windows.Forms.Panel();
            this.closeAccountMenu = new System.Windows.Forms.PictureBox();
            this.accountCreate = new System.Windows.Forms.Button();
            this.panel2 = new System.Windows.Forms.Panel();
            this.staff_retail_text = new System.Windows.Forms.ComboBox();
            this.staff_retail_label = new System.Windows.Forms.Label();
            this.menuTitle = new System.Windows.Forms.Label();
            this.createAccountMenu = new System.Windows.Forms.Timer(this.components);
            this.panel1 = new System.Windows.Forms.Panel();
            this.editAccountPanel = new System.Windows.Forms.Panel();
            this.edit_id = new System.Windows.Forms.NumericUpDown();
            this.disableAccount = new System.Windows.Forms.Button();
            this.pictureBox1 = new System.Windows.Forms.PictureBox();
            this.saveBtn = new System.Windows.Forms.Button();
            this.panel4 = new System.Windows.Forms.Panel();
            this.edit_retail_text = new System.Windows.Forms.ComboBox();
            this.label1 = new System.Windows.Forms.Label();
            this.label2 = new System.Windows.Forms.Label();
            this.label3 = new System.Windows.Forms.Label();
            this.edit_name_text = new System.Windows.Forms.TextBox();
            this.label4 = new System.Windows.Forms.Label();
            this.edit_password_text = new System.Windows.Forms.TextBox();
            this.label5 = new System.Windows.Forms.Label();
            this.edit_access_text = new System.Windows.Forms.ComboBox();
            this.edit_email_text = new System.Windows.Forms.TextBox();
            this.label6 = new System.Windows.Forms.Label();
            this.editAccountMenu = new System.Windows.Forms.Timer(this.components);
            this.staffBindingSource = new System.Windows.Forms.BindingSource(this.components);
            ((System.ComponentModel.ISupportInitialize)(this.AccountView)).BeginInit();
            this.createAccountPanel.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)(this.closeAccountMenu)).BeginInit();
            this.panel1.SuspendLayout();
            this.editAccountPanel.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)(this.edit_id)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.pictureBox1)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.staffBindingSource)).BeginInit();
            this.SuspendLayout();
            // 
            // AccountView
            // 
            this.AccountView.AllowUserToAddRows = false;
            this.AccountView.AllowUserToDeleteRows = false;
            this.AccountView.Anchor = ((System.Windows.Forms.AnchorStyles)((((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom) 
            | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.AccountView.AutoSizeColumnsMode = System.Windows.Forms.DataGridViewAutoSizeColumnsMode.Fill;
            this.AccountView.BorderStyle = System.Windows.Forms.BorderStyle.None;
            this.AccountView.CellBorderStyle = System.Windows.Forms.DataGridViewCellBorderStyle.None;
            this.AccountView.ColumnHeadersBorderStyle = System.Windows.Forms.DataGridViewHeaderBorderStyle.Single;
            dataGridViewCellStyle1.Alignment = System.Windows.Forms.DataGridViewContentAlignment.MiddleLeft;
            dataGridViewCellStyle1.BackColor = System.Drawing.SystemColors.Control;
            dataGridViewCellStyle1.Font = new System.Drawing.Font("Impact", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            dataGridViewCellStyle1.ForeColor = System.Drawing.SystemColors.WindowText;
            dataGridViewCellStyle1.SelectionBackColor = System.Drawing.SystemColors.Highlight;
            dataGridViewCellStyle1.SelectionForeColor = System.Drawing.SystemColors.HighlightText;
            dataGridViewCellStyle1.WrapMode = System.Windows.Forms.DataGridViewTriState.True;
            this.AccountView.ColumnHeadersDefaultCellStyle = dataGridViewCellStyle1;
            this.AccountView.ColumnHeadersHeightSizeMode = System.Windows.Forms.DataGridViewColumnHeadersHeightSizeMode.AutoSize;
            this.AccountView.Columns.AddRange(new System.Windows.Forms.DataGridViewColumn[] {
            this.staff_id,
            this.staff_password,
            this.staff_name,
            this.staff_email,
            this.staff_access,
            this.retail,
            this.active});
            this.AccountView.Cursor = System.Windows.Forms.Cursors.Default;
            this.AccountView.Location = new System.Drawing.Point(24, 168);
            this.AccountView.Name = "AccountView";
            this.AccountView.ReadOnly = true;
            this.AccountView.RowTemplate.Height = 30;
            this.AccountView.Size = new System.Drawing.Size(1321, 460);
            this.AccountView.TabIndex = 0;
            this.AccountView.CellDoubleClick += new System.Windows.Forms.DataGridViewCellEventHandler(this.AccountView_CellDoubleClick);
            // 
            // staff_id
            // 
            this.staff_id.HeaderText = "Staff ID";
            this.staff_id.Name = "staff_id";
            this.staff_id.ReadOnly = true;
            // 
            // staff_password
            // 
            this.staff_password.HeaderText = "Staff Password";
            this.staff_password.Name = "staff_password";
            this.staff_password.ReadOnly = true;
            this.staff_password.Visible = false;
            // 
            // staff_name
            // 
            this.staff_name.HeaderText = "Staff Name";
            this.staff_name.Name = "staff_name";
            this.staff_name.ReadOnly = true;
            // 
            // staff_email
            // 
            this.staff_email.HeaderText = "Staff Email";
            this.staff_email.Name = "staff_email";
            this.staff_email.ReadOnly = true;
            // 
            // staff_access
            // 
            this.staff_access.HeaderText = "Access Right";
            this.staff_access.Name = "staff_access";
            this.staff_access.ReadOnly = true;
            // 
            // retail
            // 
            this.retail.HeaderText = "Retail Store";
            this.retail.Name = "retail";
            this.retail.ReadOnly = true;
            // 
            // active
            // 
            this.active.HeaderText = "Active";
            this.active.Name = "active";
            this.active.ReadOnly = true;
            this.active.Resizable = System.Windows.Forms.DataGridViewTriState.True;
            this.active.SortMode = System.Windows.Forms.DataGridViewColumnSortMode.Automatic;
            // 
            // imageList1
            // 
            this.imageList1.ColorDepth = System.Windows.Forms.ColorDepth.Depth8Bit;
            this.imageList1.ImageSize = new System.Drawing.Size(16, 16);
            this.imageList1.TransparentColor = System.Drawing.Color.Transparent;
            // 
            // staff_name_text
            // 
            this.staff_name_text.BorderStyle = System.Windows.Forms.BorderStyle.FixedSingle;
            this.staff_name_text.Font = new System.Drawing.Font("PMingLiU", 13F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(136)));
            this.staff_name_text.Location = new System.Drawing.Point(35, 150);
            this.staff_name_text.Margin = new System.Windows.Forms.Padding(3, 3, 40, 3);
            this.staff_name_text.MaximumSize = new System.Drawing.Size(300, 33);
            this.staff_name_text.MinimumSize = new System.Drawing.Size(196, 33);
            this.staff_name_text.Name = "staff_name_text";
            this.staff_name_text.Size = new System.Drawing.Size(196, 33);
            this.staff_name_text.TabIndex = 2;
            this.staff_name_text.TextChanged += new System.EventHandler(this.textBox1_TextChanged_2);
            // 
            // staff_name_label
            // 
            this.staff_name_label.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.staff_name_label.AutoSize = true;
            this.staff_name_label.Font = new System.Drawing.Font("Impact", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.staff_name_label.ForeColor = System.Drawing.Color.White;
            this.staff_name_label.Location = new System.Drawing.Point(30, 114);
            this.staff_name_label.Name = "staff_name_label";
            this.staff_name_label.Size = new System.Drawing.Size(100, 25);
            this.staff_name_label.TabIndex = 3;
            this.staff_name_label.Text = "Staff Name";
            // 
            // staff_pwd_text
            // 
            this.staff_pwd_text.BorderStyle = System.Windows.Forms.BorderStyle.FixedSingle;
            this.staff_pwd_text.Font = new System.Drawing.Font("PMingLiU", 13F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(136)));
            this.staff_pwd_text.Location = new System.Drawing.Point(35, 243);
            this.staff_pwd_text.Margin = new System.Windows.Forms.Padding(3, 3, 40, 3);
            this.staff_pwd_text.MaximumSize = new System.Drawing.Size(300, 33);
            this.staff_pwd_text.MinimumSize = new System.Drawing.Size(196, 33);
            this.staff_pwd_text.Name = "staff_pwd_text";
            this.staff_pwd_text.Size = new System.Drawing.Size(196, 33);
            this.staff_pwd_text.TabIndex = 4;
            this.staff_pwd_text.TextChanged += new System.EventHandler(this.staff_pwd_text_TextChanged);
            // 
            // staff_pwd_label
            // 
            this.staff_pwd_label.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.staff_pwd_label.AutoSize = true;
            this.staff_pwd_label.Font = new System.Drawing.Font("Impact", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.staff_pwd_label.ForeColor = System.Drawing.Color.White;
            this.staff_pwd_label.Location = new System.Drawing.Point(30, 207);
            this.staff_pwd_label.Name = "staff_pwd_label";
            this.staff_pwd_label.Size = new System.Drawing.Size(132, 25);
            this.staff_pwd_label.TabIndex = 5;
            this.staff_pwd_label.Text = "Staff Password";
            // 
            // staff_email_label
            // 
            this.staff_email_label.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.staff_email_label.AutoSize = true;
            this.staff_email_label.Font = new System.Drawing.Font("Impact", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.staff_email_label.ForeColor = System.Drawing.Color.White;
            this.staff_email_label.Location = new System.Drawing.Point(30, 300);
            this.staff_email_label.Name = "staff_email_label";
            this.staff_email_label.Size = new System.Drawing.Size(97, 25);
            this.staff_email_label.TabIndex = 6;
            this.staff_email_label.Text = "Staff Email";
            // 
            // staff_email_text
            // 
            this.staff_email_text.BackColor = System.Drawing.SystemColors.Window;
            this.staff_email_text.BorderStyle = System.Windows.Forms.BorderStyle.FixedSingle;
            this.staff_email_text.Font = new System.Drawing.Font("PMingLiU", 13F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(136)));
            this.staff_email_text.ForeColor = System.Drawing.Color.Black;
            this.staff_email_text.Location = new System.Drawing.Point(35, 336);
            this.staff_email_text.Margin = new System.Windows.Forms.Padding(3, 3, 40, 3);
            this.staff_email_text.MaximumSize = new System.Drawing.Size(300, 33);
            this.staff_email_text.MinimumSize = new System.Drawing.Size(196, 33);
            this.staff_email_text.Name = "staff_email_text";
            this.staff_email_text.Size = new System.Drawing.Size(223, 33);
            this.staff_email_text.TabIndex = 7;
            this.staff_email_text.TextChanged += new System.EventHandler(this.staff_email_text_TextChanged);
            // 
            // staff_role_label
            // 
            this.staff_role_label.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.staff_role_label.AutoSize = true;
            this.staff_role_label.Font = new System.Drawing.Font("Impact", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.staff_role_label.ForeColor = System.Drawing.Color.White;
            this.staff_role_label.Location = new System.Drawing.Point(30, 394);
            this.staff_role_label.Name = "staff_role_label";
            this.staff_role_label.Size = new System.Drawing.Size(90, 25);
            this.staff_role_label.TabIndex = 8;
            this.staff_role_label.Text = "Staff Role";
            // 
            // staff_role_text
            // 
            this.staff_role_text.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.staff_role_text.Font = new System.Drawing.Font("Impact", 12F);
            this.staff_role_text.FormattingEnabled = true;
            this.staff_role_text.Items.AddRange(new object[] {
            "Admin",
            "Manager",
            "Sales",
            "Delivery Worker",
            "Van Driver",
            "Installation Worker"});
            this.staff_role_text.Location = new System.Drawing.Point(35, 429);
            this.staff_role_text.Margin = new System.Windows.Forms.Padding(3, 3, 40, 3);
            this.staff_role_text.MaximumSize = new System.Drawing.Size(196, 0);
            this.staff_role_text.MinimumSize = new System.Drawing.Size(196, 0);
            this.staff_role_text.Name = "staff_role_text";
            this.staff_role_text.Size = new System.Drawing.Size(196, 33);
            this.staff_role_text.TabIndex = 9;
            this.staff_role_text.SelectedIndexChanged += new System.EventHandler(this.staff_role_text_SelectedIndexChanged);
            // 
            // dataAddBtn
            // 
            this.dataAddBtn.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Right)));
            this.dataAddBtn.BackColor = System.Drawing.Color.White;
            this.dataAddBtn.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.dataAddBtn.Font = new System.Drawing.Font("Impact", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.dataAddBtn.ForeColor = System.Drawing.Color.Black;
            this.dataAddBtn.IconChar = FontAwesome.Sharp.IconChar.Plus;
            this.dataAddBtn.IconColor = System.Drawing.Color.Black;
            this.dataAddBtn.IconFont = FontAwesome.Sharp.IconFont.Auto;
            this.dataAddBtn.IconSize = 30;
            this.dataAddBtn.ImageAlign = System.Drawing.ContentAlignment.MiddleLeft;
            this.dataAddBtn.Location = new System.Drawing.Point(1211, 100);
            this.dataAddBtn.Margin = new System.Windows.Forms.Padding(10);
            this.dataAddBtn.Name = "dataAddBtn";
            this.dataAddBtn.Padding = new System.Windows.Forms.Padding(5);
            this.dataAddBtn.Size = new System.Drawing.Size(134, 55);
            this.dataAddBtn.TabIndex = 10;
            this.dataAddBtn.Text = "Add";
            this.dataAddBtn.TextAlign = System.Drawing.ContentAlignment.MiddleLeft;
            this.dataAddBtn.TextImageRelation = System.Windows.Forms.TextImageRelation.ImageBeforeText;
            this.dataAddBtn.UseVisualStyleBackColor = false;
            this.dataAddBtn.Click += new System.EventHandler(this.dataAddBtn_Click);
            // 
            // searchTxt
            // 
            this.searchTxt.BorderStyle = System.Windows.Forms.BorderStyle.FixedSingle;
            this.searchTxt.Font = new System.Drawing.Font("PMingLiU", 13F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(136)));
            this.searchTxt.Location = new System.Drawing.Point(244, 16);
            this.searchTxt.Margin = new System.Windows.Forms.Padding(3, 3, 40, 3);
            this.searchTxt.MaximumSize = new System.Drawing.Size(300, 33);
            this.searchTxt.MinimumSize = new System.Drawing.Size(196, 33);
            this.searchTxt.Name = "searchTxt";
            this.searchTxt.Size = new System.Drawing.Size(196, 33);
            this.searchTxt.TabIndex = 11;
            this.searchTxt.TextChanged += new System.EventHandler(this.search_name_text_TextChanged);
            this.searchTxt.KeyDown += new System.Windows.Forms.KeyEventHandler(this.search_name_text_KeyDown);
            // 
            // search_name_label
            // 
            this.search_name_label.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.search_name_label.AutoSize = true;
            this.search_name_label.Font = new System.Drawing.Font("Impact", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.search_name_label.ForeColor = System.Drawing.Color.White;
            this.search_name_label.Location = new System.Drawing.Point(4, 18);
            this.search_name_label.Name = "search_name_label";
            this.search_name_label.Size = new System.Drawing.Size(182, 25);
            this.search_name_label.TabIndex = 12;
            this.search_name_label.Text = "Search Name / Email:";
            // 
            // searchBtn
            // 
            this.searchBtn.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Right)));
            this.searchBtn.BackColor = System.Drawing.Color.White;
            this.searchBtn.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.searchBtn.Font = new System.Drawing.Font("Impact", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.searchBtn.ForeColor = System.Drawing.Color.Black;
            this.searchBtn.IconChar = FontAwesome.Sharp.IconChar.Search;
            this.searchBtn.IconColor = System.Drawing.Color.Black;
            this.searchBtn.IconFont = FontAwesome.Sharp.IconFont.Auto;
            this.searchBtn.IconSize = 30;
            this.searchBtn.ImageAlign = System.Drawing.ContentAlignment.MiddleLeft;
            this.searchBtn.Location = new System.Drawing.Point(1025, 100);
            this.searchBtn.Margin = new System.Windows.Forms.Padding(10);
            this.searchBtn.Name = "searchBtn";
            this.searchBtn.Padding = new System.Windows.Forms.Padding(5);
            this.searchBtn.Size = new System.Drawing.Size(166, 55);
            this.searchBtn.TabIndex = 16;
            this.searchBtn.Text = "Search";
            this.searchBtn.TextAlign = System.Drawing.ContentAlignment.MiddleLeft;
            this.searchBtn.TextImageRelation = System.Windows.Forms.TextImageRelation.ImageBeforeText;
            this.searchBtn.UseVisualStyleBackColor = false;
            this.searchBtn.Click += new System.EventHandler(this.iconButton1_Click_1);
            // 
            // accountTitle
            // 
            this.accountTitle.AutoSize = true;
            this.accountTitle.Font = new System.Drawing.Font("Impact", 24F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.accountTitle.ForeColor = System.Drawing.Color.White;
            this.accountTitle.Location = new System.Drawing.Point(25, 32);
            this.accountTitle.Name = "accountTitle";
            this.accountTitle.Size = new System.Drawing.Size(391, 48);
            this.accountTitle.TabIndex = 17;
            this.accountTitle.Text = "ACCOUNT MANAGEMENT";
            // 
            // createAccountPanel
            // 
            this.createAccountPanel.BackColor = System.Drawing.Color.FromArgb(((int)(((byte)(32)))), ((int)(((byte)(33)))), ((int)(((byte)(53)))));
            this.createAccountPanel.Controls.Add(this.closeAccountMenu);
            this.createAccountPanel.Controls.Add(this.accountCreate);
            this.createAccountPanel.Controls.Add(this.panel2);
            this.createAccountPanel.Controls.Add(this.staff_retail_text);
            this.createAccountPanel.Controls.Add(this.staff_retail_label);
            this.createAccountPanel.Controls.Add(this.menuTitle);
            this.createAccountPanel.Controls.Add(this.staff_name_label);
            this.createAccountPanel.Controls.Add(this.staff_name_text);
            this.createAccountPanel.Controls.Add(this.staff_pwd_label);
            this.createAccountPanel.Controls.Add(this.staff_pwd_text);
            this.createAccountPanel.Controls.Add(this.staff_email_label);
            this.createAccountPanel.Controls.Add(this.staff_role_text);
            this.createAccountPanel.Controls.Add(this.staff_email_text);
            this.createAccountPanel.Controls.Add(this.staff_role_label);
            this.createAccountPanel.Dock = System.Windows.Forms.DockStyle.Right;
            this.createAccountPanel.Location = new System.Drawing.Point(1381, 0);
            this.createAccountPanel.MaximumSize = new System.Drawing.Size(500, 5000);
            this.createAccountPanel.MinimumSize = new System.Drawing.Size(0, 5000);
            this.createAccountPanel.Name = "createAccountPanel";
            this.createAccountPanel.Size = new System.Drawing.Size(1, 5000);
            this.createAccountPanel.TabIndex = 18;
            // 
            // closeAccountMenu
            // 
            this.closeAccountMenu.BackColor = System.Drawing.Color.FromArgb(((int)(((byte)(32)))), ((int)(((byte)(33)))), ((int)(((byte)(53)))));
            this.closeAccountMenu.Cursor = System.Windows.Forms.Cursors.Hand;
            this.closeAccountMenu.Image = ((System.Drawing.Image)(resources.GetObject("closeAccountMenu.Image")));
            this.closeAccountMenu.Location = new System.Drawing.Point(449, 47);
            this.closeAccountMenu.Name = "closeAccountMenu";
            this.closeAccountMenu.Size = new System.Drawing.Size(28, 26);
            this.closeAccountMenu.SizeMode = System.Windows.Forms.PictureBoxSizeMode.Zoom;
            this.closeAccountMenu.TabIndex = 19;
            this.closeAccountMenu.TabStop = false;
            this.closeAccountMenu.Click += new System.EventHandler(this.pictureBox6_Click);
            // 
            // accountCreate
            // 
            this.accountCreate.BackColor = System.Drawing.Color.White;
            this.accountCreate.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.accountCreate.Font = new System.Drawing.Font("Impact", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.accountCreate.Location = new System.Drawing.Point(151, 592);
            this.accountCreate.Name = "accountCreate";
            this.accountCreate.Size = new System.Drawing.Size(219, 43);
            this.accountCreate.TabIndex = 22;
            this.accountCreate.Text = "Create Account";
            this.accountCreate.UseVisualStyleBackColor = false;
            this.accountCreate.Click += new System.EventHandler(this.accountCreate_Click);
            // 
            // panel2
            // 
            this.panel2.BackColor = System.Drawing.Color.White;
            this.panel2.Location = new System.Drawing.Point(37, 89);
            this.panel2.Name = "panel2";
            this.panel2.Size = new System.Drawing.Size(384, 5);
            this.panel2.TabIndex = 21;
            // 
            // staff_retail_text
            // 
            this.staff_retail_text.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.staff_retail_text.Font = new System.Drawing.Font("Impact", 12F);
            this.staff_retail_text.FormattingEnabled = true;
            this.staff_retail_text.Location = new System.Drawing.Point(35, 521);
            this.staff_retail_text.Margin = new System.Windows.Forms.Padding(3, 3, 40, 3);
            this.staff_retail_text.MaximumSize = new System.Drawing.Size(196, 0);
            this.staff_retail_text.MinimumSize = new System.Drawing.Size(196, 0);
            this.staff_retail_text.Name = "staff_retail_text";
            this.staff_retail_text.Size = new System.Drawing.Size(196, 33);
            this.staff_retail_text.TabIndex = 20;
            // 
            // staff_retail_label
            // 
            this.staff_retail_label.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.staff_retail_label.AutoSize = true;
            this.staff_retail_label.Font = new System.Drawing.Font("Impact", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.staff_retail_label.ForeColor = System.Drawing.Color.White;
            this.staff_retail_label.Location = new System.Drawing.Point(30, 486);
            this.staff_retail_label.Name = "staff_retail_label";
            this.staff_retail_label.Size = new System.Drawing.Size(148, 25);
            this.staff_retail_label.TabIndex = 19;
            this.staff_retail_label.Text = "Staff Retail Store";
            // 
            // menuTitle
            // 
            this.menuTitle.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.menuTitle.AutoSize = true;
            this.menuTitle.Font = new System.Drawing.Font("Impact", 20F);
            this.menuTitle.ForeColor = System.Drawing.Color.White;
            this.menuTitle.Location = new System.Drawing.Point(28, 35);
            this.menuTitle.Name = "menuTitle";
            this.menuTitle.Size = new System.Drawing.Size(271, 42);
            this.menuTitle.TabIndex = 19;
            this.menuTitle.Text = "Create An Account";
            // 
            // createAccountMenu
            // 
            this.createAccountMenu.Interval = 1;
            this.createAccountMenu.Tick += new System.EventHandler(this.createAccountMenu_Tick);
            // 
            // panel1
            // 
            this.panel1.Controls.Add(this.searchTxt);
            this.panel1.Controls.Add(this.search_name_label);
            this.panel1.Location = new System.Drawing.Point(24, 97);
            this.panel1.Name = "panel1";
            this.panel1.Size = new System.Drawing.Size(902, 62);
            this.panel1.TabIndex = 15;
            // 
            // editAccountPanel
            // 
            this.editAccountPanel.BackColor = System.Drawing.Color.FromArgb(((int)(((byte)(32)))), ((int)(((byte)(33)))), ((int)(((byte)(53)))));
            this.editAccountPanel.Controls.Add(this.edit_id);
            this.editAccountPanel.Controls.Add(this.disableAccount);
            this.editAccountPanel.Controls.Add(this.pictureBox1);
            this.editAccountPanel.Controls.Add(this.saveBtn);
            this.editAccountPanel.Controls.Add(this.panel4);
            this.editAccountPanel.Controls.Add(this.edit_retail_text);
            this.editAccountPanel.Controls.Add(this.label1);
            this.editAccountPanel.Controls.Add(this.label2);
            this.editAccountPanel.Controls.Add(this.label3);
            this.editAccountPanel.Controls.Add(this.edit_name_text);
            this.editAccountPanel.Controls.Add(this.label4);
            this.editAccountPanel.Controls.Add(this.edit_password_text);
            this.editAccountPanel.Controls.Add(this.label5);
            this.editAccountPanel.Controls.Add(this.edit_access_text);
            this.editAccountPanel.Controls.Add(this.edit_email_text);
            this.editAccountPanel.Controls.Add(this.label6);
            this.editAccountPanel.Dock = System.Windows.Forms.DockStyle.Right;
            this.editAccountPanel.Location = new System.Drawing.Point(1380, 0);
            this.editAccountPanel.MaximumSize = new System.Drawing.Size(500, 5000);
            this.editAccountPanel.MinimumSize = new System.Drawing.Size(0, 5000);
            this.editAccountPanel.Name = "editAccountPanel";
            this.editAccountPanel.Size = new System.Drawing.Size(1, 5000);
            this.editAccountPanel.TabIndex = 19;
            // 
            // edit_id
            // 
            this.edit_id.Location = new System.Drawing.Point(273, 48);
            this.edit_id.Name = "edit_id";
            this.edit_id.Size = new System.Drawing.Size(120, 25);
            this.edit_id.TabIndex = 24;
            this.edit_id.Visible = false;
            // 
            // disableAccount
            // 
            this.disableAccount.BackColor = System.Drawing.Color.White;
            this.disableAccount.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.disableAccount.Font = new System.Drawing.Font("Impact", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.disableAccount.Location = new System.Drawing.Point(260, 592);
            this.disableAccount.Name = "disableAccount";
            this.disableAccount.Size = new System.Drawing.Size(219, 43);
            this.disableAccount.TabIndex = 23;
            this.disableAccount.Text = "Disable Account";
            this.disableAccount.UseVisualStyleBackColor = false;
            this.disableAccount.Click += new System.EventHandler(this.disableAccount_Click);
            // 
            // pictureBox1
            // 
            this.pictureBox1.BackColor = System.Drawing.Color.FromArgb(((int)(((byte)(32)))), ((int)(((byte)(33)))), ((int)(((byte)(53)))));
            this.pictureBox1.Cursor = System.Windows.Forms.Cursors.Hand;
            this.pictureBox1.Image = ((System.Drawing.Image)(resources.GetObject("pictureBox1.Image")));
            this.pictureBox1.Location = new System.Drawing.Point(449, 47);
            this.pictureBox1.Name = "pictureBox1";
            this.pictureBox1.Size = new System.Drawing.Size(28, 26);
            this.pictureBox1.SizeMode = System.Windows.Forms.PictureBoxSizeMode.Zoom;
            this.pictureBox1.TabIndex = 19;
            this.pictureBox1.TabStop = false;
            this.pictureBox1.Click += new System.EventHandler(this.pictureBox1_Click);
            // 
            // saveBtn
            // 
            this.saveBtn.BackColor = System.Drawing.Color.White;
            this.saveBtn.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.saveBtn.Font = new System.Drawing.Font("Impact", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.saveBtn.Location = new System.Drawing.Point(35, 592);
            this.saveBtn.Name = "saveBtn";
            this.saveBtn.Size = new System.Drawing.Size(219, 43);
            this.saveBtn.TabIndex = 22;
            this.saveBtn.Text = "Update Account";
            this.saveBtn.UseVisualStyleBackColor = false;
            this.saveBtn.Click += new System.EventHandler(this.button1_Click);
            // 
            // panel4
            // 
            this.panel4.BackColor = System.Drawing.Color.White;
            this.panel4.Location = new System.Drawing.Point(37, 89);
            this.panel4.Name = "panel4";
            this.panel4.Size = new System.Drawing.Size(384, 5);
            this.panel4.TabIndex = 21;
            // 
            // edit_retail_text
            // 
            this.edit_retail_text.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.edit_retail_text.Font = new System.Drawing.Font("Impact", 12F);
            this.edit_retail_text.FormattingEnabled = true;
            this.edit_retail_text.Location = new System.Drawing.Point(35, 521);
            this.edit_retail_text.Margin = new System.Windows.Forms.Padding(3, 3, 40, 3);
            this.edit_retail_text.MaximumSize = new System.Drawing.Size(196, 0);
            this.edit_retail_text.MinimumSize = new System.Drawing.Size(196, 0);
            this.edit_retail_text.Name = "edit_retail_text";
            this.edit_retail_text.Size = new System.Drawing.Size(196, 33);
            this.edit_retail_text.TabIndex = 20;
            // 
            // label1
            // 
            this.label1.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.label1.AutoSize = true;
            this.label1.Font = new System.Drawing.Font("Impact", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label1.ForeColor = System.Drawing.Color.White;
            this.label1.Location = new System.Drawing.Point(30, 486);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(148, 25);
            this.label1.TabIndex = 19;
            this.label1.Text = "Staff Retail Store";
            // 
            // label2
            // 
            this.label2.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.label2.AutoSize = true;
            this.label2.Font = new System.Drawing.Font("Impact", 20F);
            this.label2.ForeColor = System.Drawing.Color.White;
            this.label2.Location = new System.Drawing.Point(28, 35);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(230, 42);
            this.label2.TabIndex = 19;
            this.label2.Text = "Edit An Account";
            // 
            // label3
            // 
            this.label3.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.label3.AutoSize = true;
            this.label3.Font = new System.Drawing.Font("Impact", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label3.ForeColor = System.Drawing.Color.White;
            this.label3.Location = new System.Drawing.Point(30, 114);
            this.label3.Name = "label3";
            this.label3.Size = new System.Drawing.Size(100, 25);
            this.label3.TabIndex = 3;
            this.label3.Text = "Staff Name";
            // 
            // edit_name_text
            // 
            this.edit_name_text.BorderStyle = System.Windows.Forms.BorderStyle.FixedSingle;
            this.edit_name_text.Font = new System.Drawing.Font("PMingLiU", 13F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(136)));
            this.edit_name_text.Location = new System.Drawing.Point(35, 150);
            this.edit_name_text.Margin = new System.Windows.Forms.Padding(3, 3, 40, 3);
            this.edit_name_text.MaximumSize = new System.Drawing.Size(300, 33);
            this.edit_name_text.MinimumSize = new System.Drawing.Size(196, 33);
            this.edit_name_text.Name = "edit_name_text";
            this.edit_name_text.Size = new System.Drawing.Size(196, 33);
            this.edit_name_text.TabIndex = 2;
            // 
            // label4
            // 
            this.label4.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.label4.AutoSize = true;
            this.label4.Font = new System.Drawing.Font("Impact", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label4.ForeColor = System.Drawing.Color.White;
            this.label4.Location = new System.Drawing.Point(30, 207);
            this.label4.Name = "label4";
            this.label4.Size = new System.Drawing.Size(132, 25);
            this.label4.TabIndex = 5;
            this.label4.Text = "Staff Password";
            // 
            // edit_password_text
            // 
            this.edit_password_text.BorderStyle = System.Windows.Forms.BorderStyle.FixedSingle;
            this.edit_password_text.Font = new System.Drawing.Font("PMingLiU", 13F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(136)));
            this.edit_password_text.Location = new System.Drawing.Point(35, 243);
            this.edit_password_text.Margin = new System.Windows.Forms.Padding(3, 3, 40, 3);
            this.edit_password_text.MaximumSize = new System.Drawing.Size(300, 33);
            this.edit_password_text.MinimumSize = new System.Drawing.Size(196, 33);
            this.edit_password_text.Name = "edit_password_text";
            this.edit_password_text.Size = new System.Drawing.Size(196, 33);
            this.edit_password_text.TabIndex = 4;
            // 
            // label5
            // 
            this.label5.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.label5.AutoSize = true;
            this.label5.Font = new System.Drawing.Font("Impact", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label5.ForeColor = System.Drawing.Color.White;
            this.label5.Location = new System.Drawing.Point(30, 300);
            this.label5.Name = "label5";
            this.label5.Size = new System.Drawing.Size(97, 25);
            this.label5.TabIndex = 6;
            this.label5.Text = "Staff Email";
            // 
            // edit_access_text
            // 
            this.edit_access_text.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.edit_access_text.Font = new System.Drawing.Font("Impact", 12F);
            this.edit_access_text.FormattingEnabled = true;
            this.edit_access_text.Items.AddRange(new object[] {
            "Admin",
            "Manager",
            "Sales",
            "Delivery Worker",
            "Van Driver",
            "Installation Worker"});
            this.edit_access_text.Location = new System.Drawing.Point(35, 429);
            this.edit_access_text.Margin = new System.Windows.Forms.Padding(3, 3, 40, 3);
            this.edit_access_text.MaximumSize = new System.Drawing.Size(196, 0);
            this.edit_access_text.MinimumSize = new System.Drawing.Size(196, 0);
            this.edit_access_text.Name = "edit_access_text";
            this.edit_access_text.Size = new System.Drawing.Size(196, 33);
            this.edit_access_text.TabIndex = 9;
            // 
            // edit_email_text
            // 
            this.edit_email_text.BackColor = System.Drawing.SystemColors.Window;
            this.edit_email_text.BorderStyle = System.Windows.Forms.BorderStyle.FixedSingle;
            this.edit_email_text.Font = new System.Drawing.Font("PMingLiU", 13F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(136)));
            this.edit_email_text.ForeColor = System.Drawing.Color.Black;
            this.edit_email_text.Location = new System.Drawing.Point(35, 336);
            this.edit_email_text.Margin = new System.Windows.Forms.Padding(3, 3, 40, 3);
            this.edit_email_text.MaximumSize = new System.Drawing.Size(300, 33);
            this.edit_email_text.MinimumSize = new System.Drawing.Size(196, 33);
            this.edit_email_text.Name = "edit_email_text";
            this.edit_email_text.Size = new System.Drawing.Size(223, 33);
            this.edit_email_text.TabIndex = 7;
            // 
            // label6
            // 
            this.label6.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.label6.AutoSize = true;
            this.label6.Font = new System.Drawing.Font("Impact", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label6.ForeColor = System.Drawing.Color.White;
            this.label6.Location = new System.Drawing.Point(30, 394);
            this.label6.Name = "label6";
            this.label6.Size = new System.Drawing.Size(90, 25);
            this.label6.TabIndex = 8;
            this.label6.Text = "Staff Role";
            // 
            // editAccountMenu
            // 
            this.editAccountMenu.Interval = 1;
            this.editAccountMenu.Tick += new System.EventHandler(this.editAccountMenu_Tick);
            // 
            // staffBindingSource
            // 
            this.staffBindingSource.DataSource = typeof(SDP.Staff);
            // 
            // Account
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(8F, 15F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.BackColor = System.Drawing.Color.FromArgb(((int)(((byte)(44)))), ((int)(((byte)(46)))), ((int)(((byte)(68)))));
            this.ClientSize = new System.Drawing.Size(1382, 653);
            this.Controls.Add(this.editAccountPanel);
            this.Controls.Add(this.createAccountPanel);
            this.Controls.Add(this.accountTitle);
            this.Controls.Add(this.searchBtn);
            this.Controls.Add(this.panel1);
            this.Controls.Add(this.dataAddBtn);
            this.Controls.Add(this.AccountView);
            this.MaximizeBox = false;
            this.Name = "Account";
            this.Text = "Account";
            ((System.ComponentModel.ISupportInitialize)(this.AccountView)).EndInit();
            this.createAccountPanel.ResumeLayout(false);
            this.createAccountPanel.PerformLayout();
            ((System.ComponentModel.ISupportInitialize)(this.closeAccountMenu)).EndInit();
            this.panel1.ResumeLayout(false);
            this.panel1.PerformLayout();
            this.editAccountPanel.ResumeLayout(false);
            this.editAccountPanel.PerformLayout();
            ((System.ComponentModel.ISupportInitialize)(this.edit_id)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.pictureBox1)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.staffBindingSource)).EndInit();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.DataGridView AccountView;
        private System.Windows.Forms.BindingSource staffBindingSource;
        private System.Windows.Forms.ImageList imageList1;
        private System.Windows.Forms.TextBox staff_name_text;
        private System.Windows.Forms.Label staff_name_label;
        private System.Windows.Forms.TextBox staff_pwd_text;
        private System.Windows.Forms.Label staff_pwd_label;
        private System.Windows.Forms.Label staff_email_label;
        private System.Windows.Forms.TextBox staff_email_text;
        private System.Windows.Forms.Label staff_role_label;
        private System.Windows.Forms.ComboBox staff_role_text;
        private FontAwesome.Sharp.IconButton dataAddBtn;
        private System.Windows.Forms.TextBox searchTxt;
        private System.Windows.Forms.Label search_name_label;
        private FontAwesome.Sharp.IconButton searchBtn;
        private System.Windows.Forms.Label accountTitle;
        private System.Windows.Forms.Panel createAccountPanel;
        private System.Windows.Forms.Timer createAccountMenu;
        private System.Windows.Forms.Label menuTitle;
        private System.Windows.Forms.ComboBox staff_retail_text;
        private System.Windows.Forms.Label staff_retail_label;
        private System.Windows.Forms.Panel panel2;
        private System.Windows.Forms.Button accountCreate;
        private System.Windows.Forms.PictureBox closeAccountMenu;
        private System.Windows.Forms.Panel panel1;
        private System.Windows.Forms.Panel editAccountPanel;
        private System.Windows.Forms.PictureBox pictureBox1;
        private System.Windows.Forms.Button saveBtn;
        private System.Windows.Forms.Panel panel4;
        private System.Windows.Forms.ComboBox edit_retail_text;
        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.Label label2;
        private System.Windows.Forms.Label label3;
        private System.Windows.Forms.TextBox edit_name_text;
        private System.Windows.Forms.Label label4;
        private System.Windows.Forms.TextBox edit_password_text;
        private System.Windows.Forms.Label label5;
        private System.Windows.Forms.ComboBox edit_access_text;
        private System.Windows.Forms.TextBox edit_email_text;
        private System.Windows.Forms.Label label6;
        private System.Windows.Forms.Timer editAccountMenu;
        private System.Windows.Forms.Button disableAccount;
        private System.Windows.Forms.DataGridViewTextBoxColumn staff_id;
        private System.Windows.Forms.DataGridViewTextBoxColumn staff_password;
        private System.Windows.Forms.DataGridViewTextBoxColumn staff_name;
        private System.Windows.Forms.DataGridViewTextBoxColumn staff_email;
        private System.Windows.Forms.DataGridViewTextBoxColumn staff_access;
        private System.Windows.Forms.DataGridViewTextBoxColumn retail;
        private System.Windows.Forms.DataGridViewCheckBoxColumn active;
        private System.Windows.Forms.NumericUpDown edit_id;
    }
}