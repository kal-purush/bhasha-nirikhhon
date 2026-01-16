namespace FMSAPP
{
    partial class AddUserForm
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
            this.label6 = new System.Windows.Forms.Label();
            this.lblemail = new System.Windows.Forms.Label();
            this.txtemail = new System.Windows.Forms.TextBox();
            this.lblavatar = new System.Windows.Forms.Label();
            this.txtavatar = new System.Windows.Forms.TextBox();
            this.lblfullname = new System.Windows.Forms.Label();
            this.txtfullname = new System.Windows.Forms.TextBox();
            this.lblusername = new System.Windows.Forms.Label();
            this.txtusername = new System.Windows.Forms.TextBox();
            this.txtPassword = new System.Windows.Forms.TextBox();
            this.lblpass = new System.Windows.Forms.Label();
            this.lblre_pass = new System.Windows.Forms.Label();
            this.txtre_pass = new System.Windows.Forms.TextBox();
            this.cbbRoleID = new System.Windows.Forms.ComboBox();
            this.roleBindingSource = new System.Windows.Forms.BindingSource(this.components);
            this.animeDataSet1 = new FMSAPP.animeDataSet1();
            this.label3 = new System.Windows.Forms.Label();
            this.cmbgender = new System.Windows.Forms.ComboBox();
            this.label2 = new System.Windows.Forms.Label();
            this.btnAdd = new System.Windows.Forms.Button();
            this.roleTableAdapter = new FMSAPP.animeDataSet1TableAdapters.roleTableAdapter();
            this.dateTimeAdd = new System.Windows.Forms.DateTimePicker();
            this.accountBindingSource = new System.Windows.Forms.BindingSource(this.components);
            this.accountBindingSource1 = new System.Windows.Forms.BindingSource(this.components);
            ((System.ComponentModel.ISupportInitialize)(this.roleBindingSource)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.animeDataSet1)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.accountBindingSource)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.accountBindingSource1)).BeginInit();
            this.SuspendLayout();
            // 
            // label6
            // 
            this.label6.AutoSize = true;
            this.label6.Font = new System.Drawing.Font("Microsoft Sans Serif", 10.8F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label6.Location = new System.Drawing.Point(105, 32);
            this.label6.Name = "label6";
            this.label6.Size = new System.Drawing.Size(176, 24);
            this.label6.TabIndex = 18;
            this.label6.Text = "ADD_USER_FORM";
            // 
            // lblemail
            // 
            this.lblemail.AutoSize = true;
            this.lblemail.Location = new System.Drawing.Point(12, 219);
            this.lblemail.Name = "lblemail";
            this.lblemail.Size = new System.Drawing.Size(45, 17);
            this.lblemail.TabIndex = 26;
            this.lblemail.Text = "email:";
            // 
            // txtemail
            // 
            this.txtemail.DataBindings.Add(new System.Windows.Forms.Binding("Text", this.accountBindingSource, "email", true));
            this.txtemail.Location = new System.Drawing.Point(171, 219);
            this.txtemail.Name = "txtemail";
            this.txtemail.Size = new System.Drawing.Size(174, 22);
            this.txtemail.TabIndex = 25;
            this.txtemail.Validating += new System.ComponentModel.CancelEventHandler(this.txtemail_Validating);
            // 
            // lblavatar
            // 
            this.lblavatar.AutoSize = true;
            this.lblavatar.Location = new System.Drawing.Point(12, 179);
            this.lblavatar.Name = "lblavatar";
            this.lblavatar.Size = new System.Drawing.Size(52, 17);
            this.lblavatar.TabIndex = 24;
            this.lblavatar.Text = "avatar:";
            // 
            // txtavatar
            // 
            this.txtavatar.DataBindings.Add(new System.Windows.Forms.Binding("Text", this.accountBindingSource, "avatar", true));
            this.txtavatar.Location = new System.Drawing.Point(171, 179);
            this.txtavatar.Name = "txtavatar";
            this.txtavatar.Size = new System.Drawing.Size(174, 22);
            this.txtavatar.TabIndex = 23;
            // 
            // lblfullname
            // 
            this.lblfullname.AutoSize = true;
            this.lblfullname.Location = new System.Drawing.Point(12, 137);
            this.lblfullname.Name = "lblfullname";
            this.lblfullname.Size = new System.Drawing.Size(65, 17);
            this.lblfullname.TabIndex = 22;
            this.lblfullname.Text = "fullname:";
            // 
            // txtfullname
            // 
            this.txtfullname.DataBindings.Add(new System.Windows.Forms.Binding("Text", this.accountBindingSource, "fullname", true));
            this.txtfullname.Location = new System.Drawing.Point(171, 137);
            this.txtfullname.Name = "txtfullname";
            this.txtfullname.Size = new System.Drawing.Size(174, 22);
            this.txtfullname.TabIndex = 21;
            // 
            // lblusername
            // 
            this.lblusername.AutoSize = true;
            this.lblusername.Location = new System.Drawing.Point(12, 97);
            this.lblusername.Name = "lblusername";
            this.lblusername.Size = new System.Drawing.Size(138, 17);
            this.lblusername.TabIndex = 28;
            this.lblusername.Text = "username(required):";
            // 
            // txtusername
            // 
            this.txtusername.DataBindings.Add(new System.Windows.Forms.Binding("Text", this.accountBindingSource, "username", true));
            this.txtusername.Location = new System.Drawing.Point(171, 97);
            this.txtusername.Margin = new System.Windows.Forms.Padding(3, 4, 3, 4);
            this.txtusername.Name = "txtusername";
            this.txtusername.Size = new System.Drawing.Size(174, 22);
            this.txtusername.TabIndex = 18;
            this.txtusername.Validating += new System.ComponentModel.CancelEventHandler(this.txtusername_Validating);
            // 
            // txtPassword
            // 
            this.txtPassword.DataBindings.Add(new System.Windows.Forms.Binding("Text", this.accountBindingSource, "password", true, System.Windows.Forms.DataSourceUpdateMode.OnPropertyChanged));
            this.txtPassword.Font = new System.Drawing.Font("Microsoft Sans Serif", 7.8F);
            this.txtPassword.Location = new System.Drawing.Point(171, 292);
            this.txtPassword.Margin = new System.Windows.Forms.Padding(3, 4, 3, 4);
            this.txtPassword.Name = "txtPassword";
            this.txtPassword.PasswordChar = '•';
            this.txtPassword.Size = new System.Drawing.Size(174, 22);
            this.txtPassword.TabIndex = 29;
            this.txtPassword.Validating += new System.ComponentModel.CancelEventHandler(this.txtPassword_Validating);
            // 
            // lblpass
            // 
            this.lblpass.AutoSize = true;
            this.lblpass.Location = new System.Drawing.Point(12, 292);
            this.lblpass.Name = "lblpass";
            this.lblpass.Size = new System.Drawing.Size(135, 17);
            this.lblpass.TabIndex = 30;
            this.lblpass.Text = "password(required):";
            // 
            // lblre_pass
            // 
            this.lblre_pass.AutoSize = true;
            this.lblre_pass.Location = new System.Drawing.Point(12, 331);
            this.lblre_pass.Name = "lblre_pass";
            this.lblre_pass.Size = new System.Drawing.Size(153, 17);
            this.lblre_pass.TabIndex = 32;
            this.lblre_pass.Text = "re-password(required):";
            // 
            // txtre_pass
            // 
            this.txtre_pass.Font = new System.Drawing.Font("Microsoft Sans Serif", 7.8F);
            this.txtre_pass.Location = new System.Drawing.Point(171, 331);
            this.txtre_pass.Margin = new System.Windows.Forms.Padding(3, 4, 3, 4);
            this.txtre_pass.Name = "txtre_pass";
            this.txtre_pass.PasswordChar = '•';
            this.txtre_pass.Size = new System.Drawing.Size(174, 22);
            this.txtre_pass.TabIndex = 31;
            this.txtre_pass.Validating += new System.ComponentModel.CancelEventHandler(this.txtre_pass_Validating);
            // 
            // cbbRoleID
            // 
            this.cbbRoleID.BackColor = System.Drawing.SystemColors.Window;
            this.cbbRoleID.DataBindings.Add(new System.Windows.Forms.Binding("Text", this.accountBindingSource, "RoleID", true));
            this.cbbRoleID.DataBindings.Add(new System.Windows.Forms.Binding("SelectedValue", this.roleBindingSource, "name", true));
            this.cbbRoleID.FormattingEnabled = true;
            this.cbbRoleID.Location = new System.Drawing.Point(171, 371);
            this.cbbRoleID.Name = "cbbRoleID";
            this.cbbRoleID.Size = new System.Drawing.Size(174, 24);
            this.cbbRoleID.TabIndex = 34;
            // 
            // roleBindingSource
            // 
            this.roleBindingSource.DataMember = "role";
            this.roleBindingSource.DataSource = this.animeDataSet1;
            // 
            // animeDataSet1
            // 
            this.animeDataSet1.DataSetName = "animeDataSet1";
            this.animeDataSet1.SchemaSerializationMode = System.Data.SchemaSerializationMode.IncludeSchema;
            // 
            // label3
            // 
            this.label3.AutoSize = true;
            this.label3.Location = new System.Drawing.Point(12, 368);
            this.label3.Name = "label3";
            this.label3.Size = new System.Drawing.Size(112, 17);
            this.label3.TabIndex = 33;
            this.label3.Text = "roleID(required):";
            // 
            // cmbgender
            // 
            this.cmbgender.DataBindings.Add(new System.Windows.Forms.Binding("Text", this.accountBindingSource, "gender", true));
            this.cmbgender.DataBindings.Add(new System.Windows.Forms.Binding("SelectedItem", this.accountBindingSource, "gender", true));
            this.cmbgender.DataBindings.Add(new System.Windows.Forms.Binding("SelectedValue", this.accountBindingSource, "gender", true));
            this.cmbgender.FormattingEnabled = true;
            this.cmbgender.Items.AddRange(new object[] {
            "0",
            "1"});
            this.cmbgender.Location = new System.Drawing.Point(171, 255);
            this.cmbgender.Name = "cmbgender";
            this.cmbgender.Size = new System.Drawing.Size(174, 24);
            this.cmbgender.TabIndex = 36;
            // 
            // label2
            // 
            this.label2.AutoSize = true;
            this.label2.Location = new System.Drawing.Point(12, 252);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(60, 17);
            this.label2.TabIndex = 35;
            this.label2.Text = "Gender:";
            // 
            // btnAdd
            // 
            this.btnAdd.Location = new System.Drawing.Point(124, 447);
            this.btnAdd.Name = "btnAdd";
            this.btnAdd.Size = new System.Drawing.Size(114, 40);
            this.btnAdd.TabIndex = 37;
            this.btnAdd.Text = "ADD";
            this.btnAdd.UseVisualStyleBackColor = true;
            this.btnAdd.Click += new System.EventHandler(this.btnAdd_Click);
            // 
            // roleTableAdapter
            // 
            this.roleTableAdapter.ClearBeforeFill = true;
            // 
            // dateTimeAdd
            // 
            this.dateTimeAdd.DataBindings.Add(new System.Windows.Forms.Binding("Value", this.accountBindingSource, "created_at", true));
            this.dateTimeAdd.Location = new System.Drawing.Point(96, 59);
            this.dateTimeAdd.Name = "dateTimeAdd";
            this.dateTimeAdd.Size = new System.Drawing.Size(200, 22);
            this.dateTimeAdd.TabIndex = 38;
            this.dateTimeAdd.Value = new System.DateTime(2020, 11, 4, 13, 35, 15, 0);
            this.dateTimeAdd.Validating += new System.ComponentModel.CancelEventHandler(this.dateTimeAdd_Validating);
            // 
            // accountBindingSource
            // 
            this.accountBindingSource.DataSource = typeof(FMSAPP.account);
            // 
            // accountBindingSource1
            // 
            this.accountBindingSource1.DataSource = typeof(FMSAPP.account);
            // 
            // AddUserForm
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(8F, 16F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(370, 508);
            this.Controls.Add(this.dateTimeAdd);
            this.Controls.Add(this.btnAdd);
            this.Controls.Add(this.cmbgender);
            this.Controls.Add(this.label2);
            this.Controls.Add(this.cbbRoleID);
            this.Controls.Add(this.label3);
            this.Controls.Add(this.lblre_pass);
            this.Controls.Add(this.txtre_pass);
            this.Controls.Add(this.lblpass);
            this.Controls.Add(this.txtPassword);
            this.Controls.Add(this.lblusername);
            this.Controls.Add(this.txtusername);
            this.Controls.Add(this.lblemail);
            this.Controls.Add(this.txtemail);
            this.Controls.Add(this.lblavatar);
            this.Controls.Add(this.txtavatar);
            this.Controls.Add(this.lblfullname);
            this.Controls.Add(this.txtfullname);
            this.Controls.Add(this.label6);
            this.FormBorderStyle = System.Windows.Forms.FormBorderStyle.FixedSingle;
            this.MaximizeBox = false;
            this.MinimizeBox = false;
            this.Name = "AddUserForm";
            this.Text = "AddUserForm";
            this.Load += new System.EventHandler(this.AddUserForm_Load);
            ((System.ComponentModel.ISupportInitialize)(this.roleBindingSource)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.animeDataSet1)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.accountBindingSource)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.accountBindingSource1)).EndInit();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Label label6;
        private System.Windows.Forms.Label lblemail;
        private System.Windows.Forms.TextBox txtemail;
        private System.Windows.Forms.Label lblavatar;
        private System.Windows.Forms.TextBox txtavatar;
        private System.Windows.Forms.Label lblfullname;
        private System.Windows.Forms.TextBox txtfullname;
        private System.Windows.Forms.Label lblusername;
        private System.Windows.Forms.TextBox txtusername;
        private System.Windows.Forms.TextBox txtPassword;
        private System.Windows.Forms.Label lblpass;
        private System.Windows.Forms.Label lblre_pass;
        private System.Windows.Forms.TextBox txtre_pass;
        private System.Windows.Forms.ComboBox cbbRoleID;
        private System.Windows.Forms.Label label3;
        private System.Windows.Forms.ComboBox cmbgender;
        private System.Windows.Forms.Label label2;
        private System.Windows.Forms.Button btnAdd;
        private System.Windows.Forms.BindingSource accountBindingSource;
        private System.Windows.Forms.BindingSource accountBindingSource1;
        private animeDataSet1 animeDataSet1;
        private System.Windows.Forms.BindingSource roleBindingSource;
        private animeDataSet1TableAdapters.roleTableAdapter roleTableAdapter;
        private System.Windows.Forms.DateTimePicker dateTimeAdd;
    }
}