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
            this.lbAddNewUser = new System.Windows.Forms.Label();
            this.lblemail = new System.Windows.Forms.Label();
            this.txtemail = new System.Windows.Forms.TextBox();
            this.accountBindingSource = new System.Windows.Forms.BindingSource(this.components);
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
            this.roleBindingSource1 = new System.Windows.Forms.BindingSource(this.components);
            this.roleBindingSource = new System.Windows.Forms.BindingSource(this.components);
            this.lblRole = new System.Windows.Forms.Label();
            this.cmbgender = new System.Windows.Forms.ComboBox();
            this.lbGender = new System.Windows.Forms.Label();
            this.btnAdd = new System.Windows.Forms.Button();
            this.currentDate = new System.Windows.Forms.DateTimePicker();
            this.accountBindingSource1 = new System.Windows.Forms.BindingSource(this.components);
            this.btnChoose = new System.Windows.Forms.Button();
            this.label1 = new System.Windows.Forms.Label();
            ((System.ComponentModel.ISupportInitialize)(this.accountBindingSource)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.roleBindingSource1)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.roleBindingSource)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.accountBindingSource1)).BeginInit();
            this.SuspendLayout();
            // 
            // lbAddNewUser
            // 
            this.lbAddNewUser.AutoSize = true;
            this.lbAddNewUser.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.lbAddNewUser.Location = new System.Drawing.Point(135, 26);
            this.lbAddNewUser.Name = "lbAddNewUser";
            this.lbAddNewUser.Size = new System.Drawing.Size(196, 29);
            this.lbAddNewUser.TabIndex = 18;
            this.lbAddNewUser.Text = "ADD NEW USER";
            // 
            // lblemail
            // 
            this.lblemail.AutoSize = true;
            this.lblemail.Location = new System.Drawing.Point(14, 340);
            this.lblemail.Name = "lblemail";
            this.lblemail.Size = new System.Drawing.Size(52, 20);
            this.lblemail.TabIndex = 26;
            this.lblemail.Text = "Email:";
            // 
            // txtemail
            // 
            this.txtemail.DataBindings.Add(new System.Windows.Forms.Binding("Text", this.accountBindingSource, "email", true));
            this.txtemail.Location = new System.Drawing.Point(206, 337);
            this.txtemail.Margin = new System.Windows.Forms.Padding(3, 4, 3, 4);
            this.txtemail.Name = "txtemail";
            this.txtemail.Size = new System.Drawing.Size(235, 26);
            this.txtemail.TabIndex = 25;
            this.txtemail.Validating += new System.ComponentModel.CancelEventHandler(this.txtemail_Validating);
            // 
            // accountBindingSource
            // 
            this.accountBindingSource.DataSource = typeof(FMSAPP.account);
            // 
            // lblavatar
            // 
            this.lblavatar.AutoSize = true;
            this.lblavatar.Location = new System.Drawing.Point(14, 290);
            this.lblavatar.Name = "lblavatar";
            this.lblavatar.Size = new System.Drawing.Size(59, 20);
            this.lblavatar.TabIndex = 24;
            this.lblavatar.Text = "Avatar:";
            // 
            // txtavatar
            // 
            this.txtavatar.DataBindings.Add(new System.Windows.Forms.Binding("Text", this.accountBindingSource, "avatar", true));
            this.txtavatar.Location = new System.Drawing.Point(206, 287);
            this.txtavatar.Margin = new System.Windows.Forms.Padding(3, 4, 3, 4);
            this.txtavatar.Name = "txtavatar";
            this.txtavatar.Size = new System.Drawing.Size(235, 26);
            this.txtavatar.TabIndex = 23;
            // 
            // lblfullname
            // 
            this.lblfullname.AutoSize = true;
            this.lblfullname.Location = new System.Drawing.Point(14, 237);
            this.lblfullname.Name = "lblfullname";
            this.lblfullname.Size = new System.Drawing.Size(84, 20);
            this.lblfullname.TabIndex = 22;
            this.lblfullname.Text = "Full Name:";
            // 
            // txtfullname
            // 
            this.txtfullname.DataBindings.Add(new System.Windows.Forms.Binding("Text", this.accountBindingSource, "fullname", true));
            this.txtfullname.Location = new System.Drawing.Point(206, 234);
            this.txtfullname.Margin = new System.Windows.Forms.Padding(3, 4, 3, 4);
            this.txtfullname.Name = "txtfullname";
            this.txtfullname.Size = new System.Drawing.Size(235, 26);
            this.txtfullname.TabIndex = 21;
            // 
            // lblusername
            // 
            this.lblusername.AutoSize = true;
            this.lblusername.Location = new System.Drawing.Point(14, 187);
            this.lblusername.Name = "lblusername";
            this.lblusername.Size = new System.Drawing.Size(87, 20);
            this.lblusername.TabIndex = 28;
            this.lblusername.Text = "Username:";
            // 
            // txtusername
            // 
            this.txtusername.DataBindings.Add(new System.Windows.Forms.Binding("Text", this.accountBindingSource, "username", true));
            this.txtusername.Location = new System.Drawing.Point(206, 184);
            this.txtusername.Margin = new System.Windows.Forms.Padding(3, 5, 3, 5);
            this.txtusername.Name = "txtusername";
            this.txtusername.Size = new System.Drawing.Size(235, 26);
            this.txtusername.TabIndex = 18;
            this.txtusername.Validating += new System.ComponentModel.CancelEventHandler(this.txtusername_Validating);
            // 
            // txtPassword
            // 
            this.txtPassword.Font = new System.Drawing.Font("Microsoft Sans Serif", 7.8F);
            this.txtPassword.Location = new System.Drawing.Point(206, 428);
            this.txtPassword.Margin = new System.Windows.Forms.Padding(3, 5, 3, 5);
            this.txtPassword.Name = "txtPassword";
            this.txtPassword.PasswordChar = '•';
            this.txtPassword.Size = new System.Drawing.Size(235, 25);
            this.txtPassword.TabIndex = 29;
            this.txtPassword.Validating += new System.ComponentModel.CancelEventHandler(this.txtPassword_Validating);
            // 
            // lblpass
            // 
            this.lblpass.AutoSize = true;
            this.lblpass.Location = new System.Drawing.Point(14, 431);
            this.lblpass.Name = "lblpass";
            this.lblpass.Size = new System.Drawing.Size(154, 20);
            this.lblpass.TabIndex = 30;
            this.lblpass.Text = "Password (required):";
            // 
            // lblre_pass
            // 
            this.lblre_pass.AutoSize = true;
            this.lblre_pass.Location = new System.Drawing.Point(14, 480);
            this.lblre_pass.Name = "lblre_pass";
            this.lblre_pass.Size = new System.Drawing.Size(179, 20);
            this.lblre_pass.TabIndex = 32;
            this.lblre_pass.Text = "Re-password (required):";
            // 
            // txtre_pass
            // 
            this.txtre_pass.Font = new System.Drawing.Font("Microsoft Sans Serif", 7.8F);
            this.txtre_pass.Location = new System.Drawing.Point(206, 477);
            this.txtre_pass.Margin = new System.Windows.Forms.Padding(3, 5, 3, 5);
            this.txtre_pass.Name = "txtre_pass";
            this.txtre_pass.PasswordChar = '•';
            this.txtre_pass.Size = new System.Drawing.Size(235, 25);
            this.txtre_pass.TabIndex = 31;
            this.txtre_pass.Validating += new System.ComponentModel.CancelEventHandler(this.txtre_pass_Validating);
            // 
            // cbbRoleID
            // 
            this.cbbRoleID.BackColor = System.Drawing.SystemColors.Window;
            this.cbbRoleID.DataBindings.Add(new System.Windows.Forms.Binding("SelectedValue", this.accountBindingSource, "RoleID", true));
            this.cbbRoleID.DataSource = this.roleBindingSource1;
            this.cbbRoleID.DisplayMember = "name";
            this.cbbRoleID.FormattingEnabled = true;
            this.cbbRoleID.Location = new System.Drawing.Point(206, 523);
            this.cbbRoleID.Margin = new System.Windows.Forms.Padding(3, 4, 3, 4);
            this.cbbRoleID.Name = "cbbRoleID";
            this.cbbRoleID.Size = new System.Drawing.Size(235, 28);
            this.cbbRoleID.TabIndex = 34;
            this.cbbRoleID.ValueMember = "RoleID";
            // 
            // roleBindingSource1
            // 
            this.roleBindingSource1.DataSource = typeof(FMSAPP.role);
            // 
            // lblRole
            // 
            this.lblRole.AutoSize = true;
            this.lblRole.Location = new System.Drawing.Point(14, 526);
            this.lblRole.Name = "lblRole";
            this.lblRole.Size = new System.Drawing.Size(118, 20);
            this.lblRole.TabIndex = 33;
            this.lblRole.Text = "Role (required):";
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
            this.cmbgender.Location = new System.Drawing.Point(206, 378);
            this.cmbgender.Margin = new System.Windows.Forms.Padding(3, 4, 3, 4);
            this.cmbgender.Name = "cmbgender";
            this.cmbgender.Size = new System.Drawing.Size(235, 28);
            this.cmbgender.TabIndex = 36;
            // 
            // lbGender
            // 
            this.lbGender.AutoSize = true;
            this.lbGender.Location = new System.Drawing.Point(14, 381);
            this.lbGender.Name = "lbGender";
            this.lbGender.Size = new System.Drawing.Size(67, 20);
            this.lbGender.TabIndex = 35;
            this.lbGender.Text = "Gender:";
            // 
            // btnAdd
            // 
            this.btnAdd.Location = new System.Drawing.Point(140, 584);
            this.btnAdd.Margin = new System.Windows.Forms.Padding(3, 4, 3, 4);
            this.btnAdd.Name = "btnAdd";
            this.btnAdd.Size = new System.Drawing.Size(175, 41);
            this.btnAdd.TabIndex = 37;
            this.btnAdd.Text = "ADD USER";
            this.btnAdd.UseVisualStyleBackColor = true;
            this.btnAdd.Click += new System.EventHandler(this.btnAdd_Click);
            // 
            // currentDate
            // 
            this.currentDate.DataBindings.Add(new System.Windows.Forms.Binding("Value", this.accountBindingSource, "created_at", true));
            this.currentDate.Enabled = false;
            this.currentDate.Location = new System.Drawing.Point(99, 116);
            this.currentDate.Margin = new System.Windows.Forms.Padding(3, 4, 3, 4);
            this.currentDate.Name = "currentDate";
            this.currentDate.Size = new System.Drawing.Size(269, 26);
            this.currentDate.TabIndex = 38;
            this.currentDate.Value = new System.DateTime(2020, 11, 4, 13, 35, 15, 0);
            // 
            // accountBindingSource1
            // 
            this.accountBindingSource1.DataSource = typeof(FMSAPP.account);
            // 
            // btnChoose
            // 
            this.btnChoose.Location = new System.Drawing.Point(109, 286);
            this.btnChoose.Margin = new System.Windows.Forms.Padding(3, 4, 3, 4);
            this.btnChoose.Name = "btnChoose";
            this.btnChoose.Size = new System.Drawing.Size(84, 29);
            this.btnChoose.TabIndex = 39;
            this.btnChoose.Text = "Choose";
            this.btnChoose.UseVisualStyleBackColor = true;
            this.btnChoose.Click += new System.EventHandler(this.btnChoose_Click);
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Location = new System.Drawing.Point(179, 76);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(105, 20);
            this.label1.TabIndex = 40;
            this.label1.Text = "Current Date:";
            // 
            // AddUserForm
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(9F, 20F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(463, 650);
            this.Controls.Add(this.label1);
            this.Controls.Add(this.btnChoose);
            this.Controls.Add(this.currentDate);
            this.Controls.Add(this.btnAdd);
            this.Controls.Add(this.cmbgender);
            this.Controls.Add(this.lbGender);
            this.Controls.Add(this.cbbRoleID);
            this.Controls.Add(this.lblRole);
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
            this.Controls.Add(this.lbAddNewUser);
            this.FormBorderStyle = System.Windows.Forms.FormBorderStyle.FixedSingle;
            this.Margin = new System.Windows.Forms.Padding(3, 4, 3, 4);
            this.MaximizeBox = false;
            this.MinimizeBox = false;
            this.Name = "AddUserForm";
            this.Text = "AddUserForm";
            this.Load += new System.EventHandler(this.AddUserForm_Load);
            ((System.ComponentModel.ISupportInitialize)(this.accountBindingSource)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.roleBindingSource1)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.roleBindingSource)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.accountBindingSource1)).EndInit();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Label lbAddNewUser;
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
        private System.Windows.Forms.Label lblRole;
        private System.Windows.Forms.ComboBox cmbgender;
        private System.Windows.Forms.Label lbGender;
        private System.Windows.Forms.Button btnAdd;
        private System.Windows.Forms.BindingSource accountBindingSource;
        private System.Windows.Forms.BindingSource accountBindingSource1;
        private System.Windows.Forms.BindingSource roleBindingSource;
        private System.Windows.Forms.DateTimePicker currentDate;
        private System.Windows.Forms.Button btnChoose;
        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.BindingSource roleBindingSource1;
    }
}