namespace Trivago.Forms
{
    partial class Admin
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
            System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(Admin));
            this.bunifuCustomLabel3 = new Bunifu.Framework.UI.BunifuCustomLabel();
            this.bunifuCustomLabel2 = new Bunifu.Framework.UI.BunifuCustomLabel();
            this.bunifuCustomLabel1 = new Bunifu.Framework.UI.BunifuCustomLabel();
            this.id = new Bunifu.UI.WinForms.BunifuDropdown();
            this.bunifuLabel3 = new Bunifu.UI.WinForms.BunifuLabel();
            this.bunifuLabel2 = new Bunifu.UI.WinForms.BunifuLabel();
            this.bunifuLabel1 = new Bunifu.UI.WinForms.BunifuLabel();
            this.submit = new Bunifu.Framework.UI.BunifuThinButton2();
            this.first = new Bunifu.Framework.UI.BunifuMaterialTextbox();
            this.second = new Bunifu.Framework.UI.BunifuMaterialTextbox();
            this.email = new Bunifu.Framework.UI.BunifuMaterialTextbox();
            this.mobile = new Bunifu.Framework.UI.BunifuMaterialTextbox();
            this.gender = new Bunifu.Framework.UI.BunifuMaterialTextbox();
            this.dateofbirth = new Bunifu.Framework.UI.BunifuMaterialTextbox();
            this.SuspendLayout();
            // 
            // bunifuCustomLabel3
            // 
            this.bunifuCustomLabel3.AutoSize = true;
            this.bunifuCustomLabel3.Font = new System.Drawing.Font("Times New Roman", 14.25F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.bunifuCustomLabel3.Location = new System.Drawing.Point(15, 199);
            this.bunifuCustomLabel3.Name = "bunifuCustomLabel3";
            this.bunifuCustomLabel3.Size = new System.Drawing.Size(66, 21);
            this.bunifuCustomLabel3.TabIndex = 13;
            this.bunifuCustomLabel3.Text = "Mobile:";
            // 
            // bunifuCustomLabel2
            // 
            this.bunifuCustomLabel2.AutoSize = true;
            this.bunifuCustomLabel2.Font = new System.Drawing.Font("Times New Roman", 14.25F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.bunifuCustomLabel2.Location = new System.Drawing.Point(13, 250);
            this.bunifuCustomLabel2.Name = "bunifuCustomLabel2";
            this.bunifuCustomLabel2.Size = new System.Drawing.Size(69, 21);
            this.bunifuCustomLabel2.TabIndex = 12;
            this.bunifuCustomLabel2.Text = "Gender:";
            // 
            // bunifuCustomLabel1
            // 
            this.bunifuCustomLabel1.AutoSize = true;
            this.bunifuCustomLabel1.Font = new System.Drawing.Font("Times New Roman", 14.25F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.bunifuCustomLabel1.Location = new System.Drawing.Point(15, 300);
            this.bunifuCustomLabel1.Name = "bunifuCustomLabel1";
            this.bunifuCustomLabel1.Size = new System.Drawing.Size(108, 21);
            this.bunifuCustomLabel1.TabIndex = 11;
            this.bunifuCustomLabel1.Text = "Date of birth:";
            // 
            // id
            // 
            this.id.BackColor = System.Drawing.SystemColors.Control;
            this.id.BorderRadius = 1;
            this.id.Color = System.Drawing.Color.MidnightBlue;
            this.id.Direction = Bunifu.UI.WinForms.BunifuDropdown.Directions.Down;
            this.id.DisabledColor = System.Drawing.Color.Gray;
            this.id.DrawMode = System.Windows.Forms.DrawMode.OwnerDrawFixed;
            this.id.DropdownBorderThickness = Bunifu.UI.WinForms.BunifuDropdown.BorderThickness.Thick;
            this.id.DropDownStyle = System.Windows.Forms.ComboBoxStyle.DropDownList;
            this.id.DropDownTextAlign = Bunifu.UI.WinForms.BunifuDropdown.TextAlign.Right;
            this.id.FillDropDown = false;
            this.id.FillIndicator = false;
            this.id.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.id.Font = new System.Drawing.Font("Times New Roman", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.id.ForeColor = System.Drawing.Color.MidnightBlue;
            this.id.FormattingEnabled = true;
            this.id.Icon = null;
            this.id.IndicatorColor = System.Drawing.Color.MidnightBlue;
            this.id.IndicatorLocation = Bunifu.UI.WinForms.BunifuDropdown.Indicator.Left;
            this.id.ItemBackColor = System.Drawing.Color.White;
            this.id.ItemBorderColor = System.Drawing.Color.White;
            this.id.ItemForeColor = System.Drawing.Color.MidnightBlue;
            this.id.ItemHeight = 26;
            this.id.ItemHighLightColor = System.Drawing.Color.CornflowerBlue;
            this.id.Location = new System.Drawing.Point(147, 37);
            this.id.Name = "id";
            this.id.Size = new System.Drawing.Size(228, 32);
            this.id.TabIndex = 10;
            this.id.Text = "Choose user ID";
            this.id.SelectedIndexChanged += new System.EventHandler(this.id_SelectedIndexChanged);
            // 
            // bunifuLabel3
            // 
            this.bunifuLabel3.AutoEllipsis = false;
            this.bunifuLabel3.CursorType = null;
            this.bunifuLabel3.Font = new System.Drawing.Font("Times New Roman", 14.25F);
            this.bunifuLabel3.Location = new System.Drawing.Point(17, 149);
            this.bunifuLabel3.Name = "bunifuLabel3";
            this.bunifuLabel3.RightToLeft = System.Windows.Forms.RightToLeft.No;
            this.bunifuLabel3.Size = new System.Drawing.Size(54, 23);
            this.bunifuLabel3.TabIndex = 9;
            this.bunifuLabel3.Text = "E-mail:";
            this.bunifuLabel3.TextAlignment = System.Drawing.ContentAlignment.TopLeft;
            this.bunifuLabel3.TextFormat = Bunifu.UI.WinForms.BunifuLabel.TextFormattingOptions.Default;
            // 
            // bunifuLabel2
            // 
            this.bunifuLabel2.AutoEllipsis = false;
            this.bunifuLabel2.CursorType = null;
            this.bunifuLabel2.Font = new System.Drawing.Font("Times New Roman", 14.25F);
            this.bunifuLabel2.Location = new System.Drawing.Point(19, 100);
            this.bunifuLabel2.Name = "bunifuLabel2";
            this.bunifuLabel2.RightToLeft = System.Windows.Forms.RightToLeft.No;
            this.bunifuLabel2.Size = new System.Drawing.Size(50, 23);
            this.bunifuLabel2.TabIndex = 8;
            this.bunifuLabel2.Text = "Name:";
            this.bunifuLabel2.TextAlignment = System.Drawing.ContentAlignment.TopLeft;
            this.bunifuLabel2.TextFormat = Bunifu.UI.WinForms.BunifuLabel.TextFormattingOptions.Default;
            // 
            // bunifuLabel1
            // 
            this.bunifuLabel1.AutoEllipsis = false;
            this.bunifuLabel1.CursorType = null;
            this.bunifuLabel1.Font = new System.Drawing.Font("Times New Roman", 14.25F);
            this.bunifuLabel1.Location = new System.Drawing.Point(19, 46);
            this.bunifuLabel1.Name = "bunifuLabel1";
            this.bunifuLabel1.RightToLeft = System.Windows.Forms.RightToLeft.No;
            this.bunifuLabel1.Size = new System.Drawing.Size(66, 23);
            this.bunifuLabel1.TabIndex = 7;
            this.bunifuLabel1.Text = "User ID:";
            this.bunifuLabel1.TextAlignment = System.Drawing.ContentAlignment.TopLeft;
            this.bunifuLabel1.TextFormat = Bunifu.UI.WinForms.BunifuLabel.TextFormattingOptions.Default;
            // 
            // submit
            // 
            this.submit.ActiveBorderThickness = 1;
            this.submit.ActiveCornerRadius = 20;
            this.submit.ActiveFillColor = System.Drawing.Color.CornflowerBlue;
            this.submit.ActiveForecolor = System.Drawing.Color.White;
            this.submit.ActiveLineColor = System.Drawing.Color.MidnightBlue;
            this.submit.BackColor = System.Drawing.SystemColors.Control;
            this.submit.BackgroundImage = ((System.Drawing.Image)(resources.GetObject("submit.BackgroundImage")));
            this.submit.ButtonText = "Submit";
            this.submit.Cursor = System.Windows.Forms.Cursors.Hand;
            this.submit.Font = new System.Drawing.Font("Times New Roman", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.submit.ForeColor = System.Drawing.Color.MidnightBlue;
            this.submit.IdleBorderThickness = 1;
            this.submit.IdleCornerRadius = 20;
            this.submit.IdleFillColor = System.Drawing.Color.White;
            this.submit.IdleForecolor = System.Drawing.Color.MidnightBlue;
            this.submit.IdleLineColor = System.Drawing.Color.MidnightBlue;
            this.submit.Location = new System.Drawing.Point(168, 347);
            this.submit.Margin = new System.Windows.Forms.Padding(5);
            this.submit.Name = "submit";
            this.submit.Size = new System.Drawing.Size(181, 41);
            this.submit.TabIndex = 15;
            this.submit.TextAlign = System.Drawing.ContentAlignment.MiddleCenter;
            this.submit.Click += new System.EventHandler(this.submit_Click);
            // 
            // first
            // 
            this.first.AutoCompleteMode = System.Windows.Forms.AutoCompleteMode.None;
            this.first.AutoCompleteSource = System.Windows.Forms.AutoCompleteSource.None;
            this.first.characterCasing = System.Windows.Forms.CharacterCasing.Normal;
            this.first.Cursor = System.Windows.Forms.Cursors.IBeam;
            this.first.Font = new System.Drawing.Font("Times New Roman", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.first.ForeColor = System.Drawing.Color.FromArgb(((int)(((byte)(64)))), ((int)(((byte)(64)))), ((int)(((byte)(64)))));
            this.first.HintForeColor = System.Drawing.Color.Empty;
            this.first.HintText = "";
            this.first.isPassword = false;
            this.first.LineFocusedColor = System.Drawing.Color.CornflowerBlue;
            this.first.LineIdleColor = System.Drawing.Color.MidnightBlue;
            this.first.LineMouseHoverColor = System.Drawing.Color.CornflowerBlue;
            this.first.LineThickness = 4;
            this.first.Location = new System.Drawing.Point(147, 84);
            this.first.Margin = new System.Windows.Forms.Padding(4, 5, 4, 5);
            this.first.MaxLength = 32767;
            this.first.Name = "first";
            this.first.Size = new System.Drawing.Size(124, 39);
            this.first.TabIndex = 17;
            this.first.TextAlign = System.Windows.Forms.HorizontalAlignment.Left;
            // 
            // second
            // 
            this.second.AutoCompleteMode = System.Windows.Forms.AutoCompleteMode.None;
            this.second.AutoCompleteSource = System.Windows.Forms.AutoCompleteSource.None;
            this.second.characterCasing = System.Windows.Forms.CharacterCasing.Normal;
            this.second.Cursor = System.Windows.Forms.Cursors.IBeam;
            this.second.Font = new System.Drawing.Font("Times New Roman", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.second.ForeColor = System.Drawing.Color.FromArgb(((int)(((byte)(64)))), ((int)(((byte)(64)))), ((int)(((byte)(64)))));
            this.second.HintForeColor = System.Drawing.Color.Empty;
            this.second.HintText = "";
            this.second.isPassword = false;
            this.second.LineFocusedColor = System.Drawing.Color.CornflowerBlue;
            this.second.LineIdleColor = System.Drawing.Color.MidnightBlue;
            this.second.LineMouseHoverColor = System.Drawing.Color.CornflowerBlue;
            this.second.LineThickness = 4;
            this.second.Location = new System.Drawing.Point(280, 84);
            this.second.Margin = new System.Windows.Forms.Padding(4, 5, 4, 5);
            this.second.MaxLength = 32767;
            this.second.Name = "second";
            this.second.Size = new System.Drawing.Size(124, 39);
            this.second.TabIndex = 18;
            this.second.TextAlign = System.Windows.Forms.HorizontalAlignment.Left;
            // 
            // email
            // 
            this.email.AutoCompleteMode = System.Windows.Forms.AutoCompleteMode.None;
            this.email.AutoCompleteSource = System.Windows.Forms.AutoCompleteSource.None;
            this.email.characterCasing = System.Windows.Forms.CharacterCasing.Normal;
            this.email.Cursor = System.Windows.Forms.Cursors.IBeam;
            this.email.Font = new System.Drawing.Font("Times New Roman", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.email.ForeColor = System.Drawing.Color.FromArgb(((int)(((byte)(64)))), ((int)(((byte)(64)))), ((int)(((byte)(64)))));
            this.email.HintForeColor = System.Drawing.Color.Empty;
            this.email.HintText = "";
            this.email.isPassword = false;
            this.email.LineFocusedColor = System.Drawing.Color.CornflowerBlue;
            this.email.LineIdleColor = System.Drawing.Color.MidnightBlue;
            this.email.LineMouseHoverColor = System.Drawing.Color.CornflowerBlue;
            this.email.LineThickness = 4;
            this.email.Location = new System.Drawing.Point(147, 133);
            this.email.Margin = new System.Windows.Forms.Padding(4, 5, 4, 5);
            this.email.MaxLength = 32767;
            this.email.Name = "email";
            this.email.Size = new System.Drawing.Size(256, 39);
            this.email.TabIndex = 19;
            this.email.TextAlign = System.Windows.Forms.HorizontalAlignment.Left;
            // 
            // mobile
            // 
            this.mobile.AutoCompleteMode = System.Windows.Forms.AutoCompleteMode.None;
            this.mobile.AutoCompleteSource = System.Windows.Forms.AutoCompleteSource.None;
            this.mobile.characterCasing = System.Windows.Forms.CharacterCasing.Normal;
            this.mobile.Cursor = System.Windows.Forms.Cursors.IBeam;
            this.mobile.Font = new System.Drawing.Font("Times New Roman", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.mobile.ForeColor = System.Drawing.Color.FromArgb(((int)(((byte)(64)))), ((int)(((byte)(64)))), ((int)(((byte)(64)))));
            this.mobile.HintForeColor = System.Drawing.Color.Empty;
            this.mobile.HintText = "";
            this.mobile.isPassword = false;
            this.mobile.LineFocusedColor = System.Drawing.Color.CornflowerBlue;
            this.mobile.LineIdleColor = System.Drawing.Color.MidnightBlue;
            this.mobile.LineMouseHoverColor = System.Drawing.Color.CornflowerBlue;
            this.mobile.LineThickness = 4;
            this.mobile.Location = new System.Drawing.Point(147, 181);
            this.mobile.Margin = new System.Windows.Forms.Padding(4, 5, 4, 5);
            this.mobile.MaxLength = 32767;
            this.mobile.Name = "mobile";
            this.mobile.Size = new System.Drawing.Size(256, 39);
            this.mobile.TabIndex = 20;
            this.mobile.TextAlign = System.Windows.Forms.HorizontalAlignment.Left;
            // 
            // gender
            // 
            this.gender.AutoCompleteMode = System.Windows.Forms.AutoCompleteMode.None;
            this.gender.AutoCompleteSource = System.Windows.Forms.AutoCompleteSource.None;
            this.gender.characterCasing = System.Windows.Forms.CharacterCasing.Normal;
            this.gender.Cursor = System.Windows.Forms.Cursors.IBeam;
            this.gender.Font = new System.Drawing.Font("Times New Roman", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.gender.ForeColor = System.Drawing.Color.FromArgb(((int)(((byte)(64)))), ((int)(((byte)(64)))), ((int)(((byte)(64)))));
            this.gender.HintForeColor = System.Drawing.Color.Empty;
            this.gender.HintText = "";
            this.gender.isPassword = false;
            this.gender.LineFocusedColor = System.Drawing.Color.CornflowerBlue;
            this.gender.LineIdleColor = System.Drawing.Color.MidnightBlue;
            this.gender.LineMouseHoverColor = System.Drawing.Color.CornflowerBlue;
            this.gender.LineThickness = 4;
            this.gender.Location = new System.Drawing.Point(147, 230);
            this.gender.Margin = new System.Windows.Forms.Padding(4, 5, 4, 5);
            this.gender.MaxLength = 32767;
            this.gender.Name = "gender";
            this.gender.Size = new System.Drawing.Size(256, 39);
            this.gender.TabIndex = 21;
            this.gender.TextAlign = System.Windows.Forms.HorizontalAlignment.Left;
            // 
            // dateofbirth
            // 
            this.dateofbirth.AutoCompleteMode = System.Windows.Forms.AutoCompleteMode.None;
            this.dateofbirth.AutoCompleteSource = System.Windows.Forms.AutoCompleteSource.None;
            this.dateofbirth.characterCasing = System.Windows.Forms.CharacterCasing.Normal;
            this.dateofbirth.Cursor = System.Windows.Forms.Cursors.IBeam;
            this.dateofbirth.Font = new System.Drawing.Font("Times New Roman", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.dateofbirth.ForeColor = System.Drawing.Color.FromArgb(((int)(((byte)(64)))), ((int)(((byte)(64)))), ((int)(((byte)(64)))));
            this.dateofbirth.HintForeColor = System.Drawing.Color.Empty;
            this.dateofbirth.HintText = "";
            this.dateofbirth.isPassword = false;
            this.dateofbirth.LineFocusedColor = System.Drawing.Color.CornflowerBlue;
            this.dateofbirth.LineIdleColor = System.Drawing.Color.MidnightBlue;
            this.dateofbirth.LineMouseHoverColor = System.Drawing.Color.CornflowerBlue;
            this.dateofbirth.LineThickness = 4;
            this.dateofbirth.Location = new System.Drawing.Point(147, 279);
            this.dateofbirth.Margin = new System.Windows.Forms.Padding(4, 5, 4, 5);
            this.dateofbirth.MaxLength = 32767;
            this.dateofbirth.Name = "dateofbirth";
            this.dateofbirth.Size = new System.Drawing.Size(263, 39);
            this.dateofbirth.TabIndex = 22;
            this.dateofbirth.TextAlign = System.Windows.Forms.HorizontalAlignment.Left;
            // 
            // Admin
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(468, 437);
            this.Controls.Add(this.dateofbirth);
            this.Controls.Add(this.gender);
            this.Controls.Add(this.mobile);
            this.Controls.Add(this.email);
            this.Controls.Add(this.second);
            this.Controls.Add(this.first);
            this.Controls.Add(this.submit);
            this.Controls.Add(this.bunifuCustomLabel3);
            this.Controls.Add(this.bunifuCustomLabel2);
            this.Controls.Add(this.bunifuCustomLabel1);
            this.Controls.Add(this.id);
            this.Controls.Add(this.bunifuLabel3);
            this.Controls.Add(this.bunifuLabel2);
            this.Controls.Add(this.bunifuLabel1);
            this.Name = "Admin";
            this.Text = "Admin";
            this.Load += new System.EventHandler(this.Admin_Load);
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private Bunifu.Framework.UI.BunifuCustomLabel bunifuCustomLabel3;
        private Bunifu.Framework.UI.BunifuCustomLabel bunifuCustomLabel2;
        private Bunifu.Framework.UI.BunifuCustomLabel bunifuCustomLabel1;
        private Bunifu.UI.WinForms.BunifuDropdown id;
        private Bunifu.UI.WinForms.BunifuLabel bunifuLabel3;
        private Bunifu.UI.WinForms.BunifuLabel bunifuLabel2;
        private Bunifu.UI.WinForms.BunifuLabel bunifuLabel1;
        private Bunifu.Framework.UI.BunifuThinButton2 submit;
        private Bunifu.Framework.UI.BunifuMaterialTextbox first;
        private Bunifu.Framework.UI.BunifuMaterialTextbox second;
        private Bunifu.Framework.UI.BunifuMaterialTextbox email;
        private Bunifu.Framework.UI.BunifuMaterialTextbox mobile;
        private Bunifu.Framework.UI.BunifuMaterialTextbox gender;
        private Bunifu.Framework.UI.BunifuMaterialTextbox dateofbirth;
    }
}