namespace Trivago.Forms
{
    partial class Login
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
            System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(Login));
            Bunifu.UI.WinForms.BunifuButton.BunifuButton.StateProperties stateProperties4 = new Bunifu.UI.WinForms.BunifuButton.BunifuButton.StateProperties();
            this.EmailTxtBox = new Bunifu.Framework.UI.BunifuMaterialTextbox();
            this.PasswordTxtBox = new Bunifu.Framework.UI.BunifuMaterialTextbox();
            this.LoginBtn = new Bunifu.UI.WinForms.BunifuButton.BunifuButton();
            this.SuspendLayout();
            // 
            // EmailTxtBox
            // 
            this.EmailTxtBox.AutoCompleteMode = System.Windows.Forms.AutoCompleteMode.None;
            this.EmailTxtBox.AutoCompleteSource = System.Windows.Forms.AutoCompleteSource.None;
            this.EmailTxtBox.characterCasing = System.Windows.Forms.CharacterCasing.Normal;
            this.EmailTxtBox.Cursor = System.Windows.Forms.Cursors.IBeam;
            this.EmailTxtBox.Font = new System.Drawing.Font("Century Gothic", 9.75F);
            this.EmailTxtBox.ForeColor = System.Drawing.Color.FromArgb(((int)(((byte)(64)))), ((int)(((byte)(64)))), ((int)(((byte)(64)))));
            this.EmailTxtBox.HintForeColor = System.Drawing.Color.Empty;
            this.EmailTxtBox.HintText = "Enter Your Email";
            this.EmailTxtBox.isPassword = false;
            this.EmailTxtBox.LineFocusedColor = System.Drawing.Color.Blue;
            this.EmailTxtBox.LineIdleColor = System.Drawing.Color.Gray;
            this.EmailTxtBox.LineMouseHoverColor = System.Drawing.Color.Blue;
            this.EmailTxtBox.LineThickness = 3;
            this.EmailTxtBox.Location = new System.Drawing.Point(130, 91);
            this.EmailTxtBox.Margin = new System.Windows.Forms.Padding(4);
            this.EmailTxtBox.MaxLength = 32767;
            this.EmailTxtBox.Name = "EmailTxtBox";
            this.EmailTxtBox.Size = new System.Drawing.Size(379, 33);
            this.EmailTxtBox.TabIndex = 0;
            this.EmailTxtBox.TextAlign = System.Windows.Forms.HorizontalAlignment.Left;
            // 
            // PasswordTxtBox
            // 
            this.PasswordTxtBox.AutoCompleteMode = System.Windows.Forms.AutoCompleteMode.None;
            this.PasswordTxtBox.AutoCompleteSource = System.Windows.Forms.AutoCompleteSource.None;
            this.PasswordTxtBox.characterCasing = System.Windows.Forms.CharacterCasing.Normal;
            this.PasswordTxtBox.Cursor = System.Windows.Forms.Cursors.IBeam;
            this.PasswordTxtBox.Font = new System.Drawing.Font("Century Gothic", 9.75F);
            this.PasswordTxtBox.ForeColor = System.Drawing.Color.FromArgb(((int)(((byte)(64)))), ((int)(((byte)(64)))), ((int)(((byte)(64)))));
            this.PasswordTxtBox.HintForeColor = System.Drawing.Color.Empty;
            this.PasswordTxtBox.HintText = "Enter Your Password";
            this.PasswordTxtBox.isPassword = true;
            this.PasswordTxtBox.LineFocusedColor = System.Drawing.Color.Blue;
            this.PasswordTxtBox.LineIdleColor = System.Drawing.Color.Gray;
            this.PasswordTxtBox.LineMouseHoverColor = System.Drawing.Color.Blue;
            this.PasswordTxtBox.LineThickness = 3;
            this.PasswordTxtBox.Location = new System.Drawing.Point(130, 155);
            this.PasswordTxtBox.Margin = new System.Windows.Forms.Padding(4);
            this.PasswordTxtBox.MaxLength = 32767;
            this.PasswordTxtBox.Name = "PasswordTxtBox";
            this.PasswordTxtBox.Size = new System.Drawing.Size(379, 33);
            this.PasswordTxtBox.TabIndex = 1;
            this.PasswordTxtBox.TextAlign = System.Windows.Forms.HorizontalAlignment.Left;
            // 
            // LoginBtn
            // 
            this.LoginBtn.BackColor = System.Drawing.Color.Transparent;
            this.LoginBtn.BackgroundImage = ((System.Drawing.Image)(resources.GetObject("LoginBtn.BackgroundImage")));
            this.LoginBtn.ButtonText = "LOGIN";
            this.LoginBtn.ButtonTextMarginLeft = 0;
            this.LoginBtn.DisabledBorderColor = System.Drawing.Color.FromArgb(((int)(((byte)(161)))), ((int)(((byte)(161)))), ((int)(((byte)(161)))));
            this.LoginBtn.DisabledFillColor = System.Drawing.Color.Gray;
            this.LoginBtn.DisabledForecolor = System.Drawing.Color.White;
            this.LoginBtn.ForeColor = System.Drawing.Color.White;
            this.LoginBtn.IconLeftCursor = System.Windows.Forms.Cursors.Default;
            this.LoginBtn.IconPadding = 10;
            this.LoginBtn.IconRightCursor = System.Windows.Forms.Cursors.Default;
            this.LoginBtn.IdleBorderColor = System.Drawing.Color.FromArgb(((int)(((byte)(51)))), ((int)(((byte)(122)))), ((int)(((byte)(183)))));
            this.LoginBtn.IdleBorderRadius = 1;
            this.LoginBtn.IdleBorderThickness = 0;
            this.LoginBtn.IdleFillColor = System.Drawing.Color.FromArgb(((int)(((byte)(51)))), ((int)(((byte)(122)))), ((int)(((byte)(183)))));
            this.LoginBtn.IdleIconLeftImage = null;
            this.LoginBtn.IdleIconRightImage = null;
            this.LoginBtn.Location = new System.Drawing.Point(180, 228);
            this.LoginBtn.Name = "LoginBtn";
            stateProperties4.BorderColor = System.Drawing.Color.FromArgb(((int)(((byte)(40)))), ((int)(((byte)(96)))), ((int)(((byte)(144)))));
            stateProperties4.BorderRadius = 1;
            stateProperties4.BorderThickness = 1;
            stateProperties4.FillColor = System.Drawing.Color.FromArgb(((int)(((byte)(40)))), ((int)(((byte)(96)))), ((int)(((byte)(144)))));
            stateProperties4.IconLeftImage = null;
            stateProperties4.IconRightImage = null;
            this.LoginBtn.onHoverState = stateProperties4;
            this.LoginBtn.Size = new System.Drawing.Size(210, 45);
            this.LoginBtn.TabIndex = 2;
            this.LoginBtn.TextAlign = System.Drawing.ContentAlignment.MiddleCenter;
            this.LoginBtn.Click += new System.EventHandler(this.LoginBtn_Click);
            // 
            // Login
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(8F, 16F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(624, 352);
            this.Controls.Add(this.LoginBtn);
            this.Controls.Add(this.PasswordTxtBox);
            this.Controls.Add(this.EmailTxtBox);
            this.Name = "Login";
            this.Text = "Login";
            this.FormClosing += new System.Windows.Forms.FormClosingEventHandler(this.Login_FormClosing);
            this.ResumeLayout(false);

        }

        #endregion

        private Bunifu.Framework.UI.BunifuMaterialTextbox EmailTxtBox;
        private Bunifu.Framework.UI.BunifuMaterialTextbox PasswordTxtBox;
        private Bunifu.UI.WinForms.BunifuButton.BunifuButton LoginBtn;
    }
}