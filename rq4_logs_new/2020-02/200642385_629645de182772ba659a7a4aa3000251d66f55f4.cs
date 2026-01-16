namespace CrmOrganizationParamsTool
{
    partial class CrmOrganizationParamsTool
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
            this.TextBoxOrganizationServiceUrl = new System.Windows.Forms.TextBox();
            this.LabelOrganizationServiceUrl = new System.Windows.Forms.Label();
            this.GroupBoxConnection = new System.Windows.Forms.GroupBox();
            this.LabelPassword = new System.Windows.Forms.Label();
            this.TextBoxPassword = new System.Windows.Forms.TextBox();
            this.LabelUserName = new System.Windows.Forms.Label();
            this.TextBoxUserName = new System.Windows.Forms.TextBox();
            this.ButtonConnect = new System.Windows.Forms.Button();
            this.GroupBoxOrganizationParameters = new System.Windows.Forms.GroupBox();
            this.LabelParameterType = new System.Windows.Forms.Label();
            this.TextBoxParameterType = new System.Windows.Forms.TextBox();
            this.LabelParameterValue = new System.Windows.Forms.Label();
            this.LabelParameterName = new System.Windows.Forms.Label();
            this.TextBoxParameterValue = new System.Windows.Forms.TextBox();
            this.ButtonSetValue = new System.Windows.Forms.Button();
            this.ComboBoxParameter = new System.Windows.Forms.ComboBox();
            this.GroupBoxConnection.SuspendLayout();
            this.GroupBoxOrganizationParameters.SuspendLayout();
            this.SuspendLayout();
            // 
            // TextBoxOrganizationServiceUrl
            // 
            this.TextBoxOrganizationServiceUrl.Location = new System.Drawing.Point(168, 16);
            this.TextBoxOrganizationServiceUrl.Name = "TextBoxOrganizationServiceUrl";
            this.TextBoxOrganizationServiceUrl.Size = new System.Drawing.Size(260, 20);
            this.TextBoxOrganizationServiceUrl.TabIndex = 0;
            this.TextBoxOrganizationServiceUrl.Text = "http://crm2016_demo:5555/test18/XRMServices/2011/Organization.svc";
            this.TextBoxOrganizationServiceUrl.TextChanged += new System.EventHandler(this.TextBoxOrganizationServiceUrl_TextChanged);
            // 
            // LabelOrganizationServiceUrl
            // 
            this.LabelOrganizationServiceUrl.AutoSize = true;
            this.LabelOrganizationServiceUrl.Location = new System.Drawing.Point(17, 19);
            this.LabelOrganizationServiceUrl.Name = "LabelOrganizationServiceUrl";
            this.LabelOrganizationServiceUrl.Size = new System.Drawing.Size(130, 13);
            this.LabelOrganizationServiceUrl.TabIndex = 1;
            this.LabelOrganizationServiceUrl.Text = "Organization Service URL";
            // 
            // GroupBoxConnection
            // 
            this.GroupBoxConnection.Controls.Add(this.LabelPassword);
            this.GroupBoxConnection.Controls.Add(this.TextBoxPassword);
            this.GroupBoxConnection.Controls.Add(this.LabelUserName);
            this.GroupBoxConnection.Controls.Add(this.TextBoxUserName);
            this.GroupBoxConnection.Controls.Add(this.ButtonConnect);
            this.GroupBoxConnection.Controls.Add(this.LabelOrganizationServiceUrl);
            this.GroupBoxConnection.Controls.Add(this.TextBoxOrganizationServiceUrl);
            this.GroupBoxConnection.Location = new System.Drawing.Point(12, 12);
            this.GroupBoxConnection.Name = "GroupBoxConnection";
            this.GroupBoxConnection.Size = new System.Drawing.Size(455, 152);
            this.GroupBoxConnection.TabIndex = 2;
            this.GroupBoxConnection.TabStop = false;
            this.GroupBoxConnection.Text = "Connection";
            // 
            // LabelPassword
            // 
            this.LabelPassword.AutoSize = true;
            this.LabelPassword.Location = new System.Drawing.Point(94, 77);
            this.LabelPassword.Name = "LabelPassword";
            this.LabelPassword.Size = new System.Drawing.Size(53, 13);
            this.LabelPassword.TabIndex = 6;
            this.LabelPassword.Text = "Password";
            // 
            // TextBoxPassword
            // 
            this.TextBoxPassword.Location = new System.Drawing.Point(168, 74);
            this.TextBoxPassword.Name = "TextBoxPassword";
            this.TextBoxPassword.PasswordChar = '●';
            this.TextBoxPassword.Size = new System.Drawing.Size(260, 20);
            this.TextBoxPassword.TabIndex = 5;
            // 
            // LabelUserName
            // 
            this.LabelUserName.AutoSize = true;
            this.LabelUserName.Location = new System.Drawing.Point(87, 48);
            this.LabelUserName.Name = "LabelUserName";
            this.LabelUserName.Size = new System.Drawing.Size(60, 13);
            this.LabelUserName.TabIndex = 4;
            this.LabelUserName.Text = "User Name";
            // 
            // TextBoxUserName
            // 
            this.TextBoxUserName.Location = new System.Drawing.Point(168, 45);
            this.TextBoxUserName.Name = "TextBoxUserName";
            this.TextBoxUserName.Size = new System.Drawing.Size(260, 20);
            this.TextBoxUserName.TabIndex = 3;
            // 
            // ButtonConnect
            // 
            this.ButtonConnect.BackColor = System.Drawing.Color.White;
            this.ButtonConnect.Enabled = false;
            this.ButtonConnect.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.ButtonConnect.Location = new System.Drawing.Point(189, 102);
            this.ButtonConnect.Name = "ButtonConnect";
            this.ButtonConnect.Size = new System.Drawing.Size(128, 35);
            this.ButtonConnect.TabIndex = 2;
            this.ButtonConnect.Text = "Connect";
            this.ButtonConnect.UseVisualStyleBackColor = false;
            this.ButtonConnect.Click += new System.EventHandler(this.ButtonConnect_Click);
            // 
            // GroupBoxOrganizationParameters
            // 
            this.GroupBoxOrganizationParameters.Controls.Add(this.LabelParameterType);
            this.GroupBoxOrganizationParameters.Controls.Add(this.TextBoxParameterType);
            this.GroupBoxOrganizationParameters.Controls.Add(this.LabelParameterValue);
            this.GroupBoxOrganizationParameters.Controls.Add(this.LabelParameterName);
            this.GroupBoxOrganizationParameters.Controls.Add(this.TextBoxParameterValue);
            this.GroupBoxOrganizationParameters.Controls.Add(this.ButtonSetValue);
            this.GroupBoxOrganizationParameters.Controls.Add(this.ComboBoxParameter);
            this.GroupBoxOrganizationParameters.Enabled = false;
            this.GroupBoxOrganizationParameters.Location = new System.Drawing.Point(12, 169);
            this.GroupBoxOrganizationParameters.Name = "GroupBoxOrganizationParameters";
            this.GroupBoxOrganizationParameters.Size = new System.Drawing.Size(455, 151);
            this.GroupBoxOrganizationParameters.TabIndex = 3;
            this.GroupBoxOrganizationParameters.TabStop = false;
            this.GroupBoxOrganizationParameters.Text = "Organization Parameters";
            // 
            // LabelParameterType
            // 
            this.LabelParameterType.AutoSize = true;
            this.LabelParameterType.Location = new System.Drawing.Point(113, 81);
            this.LabelParameterType.Name = "LabelParameterType";
            this.LabelParameterType.Size = new System.Drawing.Size(31, 13);
            this.LabelParameterType.TabIndex = 8;
            this.LabelParameterType.Text = "Type";
            // 
            // TextBoxParameterType
            // 
            this.TextBoxParameterType.Location = new System.Drawing.Point(168, 78);
            this.TextBoxParameterType.Name = "TextBoxParameterType";
            this.TextBoxParameterType.ReadOnly = true;
            this.TextBoxParameterType.Size = new System.Drawing.Size(260, 20);
            this.TextBoxParameterType.TabIndex = 7;
            // 
            // LabelParameterValue
            // 
            this.LabelParameterValue.AutoSize = true;
            this.LabelParameterValue.Location = new System.Drawing.Point(113, 52);
            this.LabelParameterValue.Name = "LabelParameterValue";
            this.LabelParameterValue.Size = new System.Drawing.Size(34, 13);
            this.LabelParameterValue.TabIndex = 6;
            this.LabelParameterValue.Text = "Value";
            // 
            // LabelParameterName
            // 
            this.LabelParameterName.AutoSize = true;
            this.LabelParameterName.Location = new System.Drawing.Point(112, 22);
            this.LabelParameterName.Name = "LabelParameterName";
            this.LabelParameterName.Size = new System.Drawing.Size(35, 13);
            this.LabelParameterName.TabIndex = 5;
            this.LabelParameterName.Text = "Name";
            // 
            // TextBoxParameterValue
            // 
            this.TextBoxParameterValue.Location = new System.Drawing.Point(168, 49);
            this.TextBoxParameterValue.Name = "TextBoxParameterValue";
            this.TextBoxParameterValue.Size = new System.Drawing.Size(260, 20);
            this.TextBoxParameterValue.TabIndex = 4;
            // 
            // ButtonSetValue
            // 
            this.ButtonSetValue.BackColor = System.Drawing.Color.White;
            this.ButtonSetValue.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.ButtonSetValue.Location = new System.Drawing.Point(189, 106);
            this.ButtonSetValue.Name = "ButtonSetValue";
            this.ButtonSetValue.Size = new System.Drawing.Size(128, 32);
            this.ButtonSetValue.TabIndex = 3;
            this.ButtonSetValue.Text = "Set Value";
            this.ButtonSetValue.UseVisualStyleBackColor = false;
            this.ButtonSetValue.Click += new System.EventHandler(this.ButtonSetValue_Click);
            // 
            // ComboBoxParameter
            // 
            this.ComboBoxParameter.DropDownStyle = System.Windows.Forms.ComboBoxStyle.DropDownList;
            this.ComboBoxParameter.FlatStyle = System.Windows.Forms.FlatStyle.Popup;
            this.ComboBoxParameter.FormattingEnabled = true;
            this.ComboBoxParameter.Location = new System.Drawing.Point(168, 19);
            this.ComboBoxParameter.Name = "ComboBoxParameter";
            this.ComboBoxParameter.Size = new System.Drawing.Size(260, 21);
            this.ComboBoxParameter.TabIndex = 0;
            this.ComboBoxParameter.SelectedIndexChanged += new System.EventHandler(this.ComboBoxParameterName_SelectedIndexChanged);
            // 
            // CrmOrganizationParamsTool
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.BackColor = System.Drawing.Color.White;
            this.ClientSize = new System.Drawing.Size(479, 332);
            this.Controls.Add(this.GroupBoxOrganizationParameters);
            this.Controls.Add(this.GroupBoxConnection);
            this.FormBorderStyle = System.Windows.Forms.FormBorderStyle.FixedDialog;
            this.MaximizeBox = false;
            this.Name = "CrmOrganizationParamsTool";
            this.Text = "CRM Organization Params Tool";
            this.GroupBoxConnection.ResumeLayout(false);
            this.GroupBoxConnection.PerformLayout();
            this.GroupBoxOrganizationParameters.ResumeLayout(false);
            this.GroupBoxOrganizationParameters.PerformLayout();
            this.ResumeLayout(false);

        }

        #endregion

        private System.Windows.Forms.TextBox TextBoxOrganizationServiceUrl;
        private System.Windows.Forms.Label LabelOrganizationServiceUrl;
        private System.Windows.Forms.GroupBox GroupBoxConnection;
        private System.Windows.Forms.GroupBox GroupBoxOrganizationParameters;
        private System.Windows.Forms.Button ButtonConnect;
        private System.Windows.Forms.ComboBox ComboBoxParameter;
        private System.Windows.Forms.Button ButtonSetValue;
        private System.Windows.Forms.Label LabelParameterValue;
        private System.Windows.Forms.Label LabelParameterName;
        private System.Windows.Forms.TextBox TextBoxParameterValue;
        private System.Windows.Forms.Label LabelPassword;
        private System.Windows.Forms.TextBox TextBoxPassword;
        private System.Windows.Forms.Label LabelUserName;
        private System.Windows.Forms.TextBox TextBoxUserName;
        private System.Windows.Forms.Label LabelParameterType;
        private System.Windows.Forms.TextBox TextBoxParameterType;
    }
}
