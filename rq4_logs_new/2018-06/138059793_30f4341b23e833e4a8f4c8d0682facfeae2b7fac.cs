namespace Traffic
{
    partial class AddForm
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
            this.add = new MetroFramework.Controls.MetroButton();
            this.cancel = new MetroFramework.Controls.MetroButton();
            this.pnumber = new MetroFramework.Controls.MetroTextBox();
            this.metroLabel8 = new MetroFramework.Controls.MetroLabel();
            this.password = new MetroFramework.Controls.MetroTextBox();
            this.metroLabel3 = new MetroFramework.Controls.MetroLabel();
            this.uname = new MetroFramework.Controls.MetroTextBox();
            this.metroLabel4 = new MetroFramework.Controls.MetroLabel();
            this.dname = new MetroFramework.Controls.MetroTextBox();
            this.metroLabel5 = new MetroFramework.Controls.MetroLabel();
            this.sname = new MetroFramework.Controls.MetroTextBox();
            this.metroLabel6 = new MetroFramework.Controls.MetroLabel();
            this.SuspendLayout();
            // 
            // add
            // 
            this.add.Location = new System.Drawing.Point(138, 234);
            this.add.Name = "add";
            this.add.Size = new System.Drawing.Size(75, 23);
            this.add.TabIndex = 8;
            this.add.Text = "Add";
            this.add.Theme = MetroFramework.MetroThemeStyle.Light;
            this.add.UseSelectable = true;
            this.add.Click += new System.EventHandler(this.add_Click);
            // 
            // cancel
            // 
            this.cancel.Location = new System.Drawing.Point(219, 234);
            this.cancel.Name = "cancel";
            this.cancel.Size = new System.Drawing.Size(75, 23);
            this.cancel.TabIndex = 9;
            this.cancel.Text = "Cancel";
            this.cancel.Theme = MetroFramework.MetroThemeStyle.Light;
            this.cancel.UseSelectable = true;
            this.cancel.Click += new System.EventHandler(this.metroButton2_Click);
            // 
            // pnumber
            // 
            // 
            // 
            // 
            this.pnumber.CustomButton.Image = null;
            this.pnumber.CustomButton.Location = new System.Drawing.Point(134, 1);
            this.pnumber.CustomButton.Name = "";
            this.pnumber.CustomButton.Size = new System.Drawing.Size(21, 21);
            this.pnumber.CustomButton.Style = MetroFramework.MetroColorStyle.Blue;
            this.pnumber.CustomButton.TabIndex = 1;
            this.pnumber.CustomButton.Theme = MetroFramework.MetroThemeStyle.Light;
            this.pnumber.CustomButton.UseSelectable = true;
            this.pnumber.CustomButton.Visible = false;
            this.pnumber.Lines = new string[0];
            this.pnumber.Location = new System.Drawing.Point(142, 129);
            this.pnumber.MaxLength = 32767;
            this.pnumber.Name = "pnumber";
            this.pnumber.PasswordChar = '\0';
            this.pnumber.ScrollBars = System.Windows.Forms.ScrollBars.None;
            this.pnumber.SelectedText = "";
            this.pnumber.SelectionLength = 0;
            this.pnumber.SelectionStart = 0;
            this.pnumber.ShortcutsEnabled = true;
            this.pnumber.Size = new System.Drawing.Size(156, 23);
            this.pnumber.TabIndex = 28;
            this.pnumber.Theme = MetroFramework.MetroThemeStyle.Light;
            this.pnumber.UseSelectable = true;
            this.pnumber.WaterMarkColor = System.Drawing.Color.FromArgb(((int)(((byte)(109)))), ((int)(((byte)(109)))), ((int)(((byte)(109)))));
            this.pnumber.WaterMarkFont = new System.Drawing.Font("Segoe UI", 12F, System.Drawing.FontStyle.Italic, System.Drawing.GraphicsUnit.Pixel);
            // 
            // metroLabel8
            // 
            this.metroLabel8.AutoSize = true;
            this.metroLabel8.ForeColor = System.Drawing.Color.White;
            this.metroLabel8.Location = new System.Drawing.Point(25, 133);
            this.metroLabel8.Name = "metroLabel8";
            this.metroLabel8.Size = new System.Drawing.Size(87, 19);
            this.metroLabel8.TabIndex = 27;
            this.metroLabel8.Text = "Port Number";
            this.metroLabel8.Theme = MetroFramework.MetroThemeStyle.Light;
            // 
            // password
            // 
            // 
            // 
            // 
            this.password.CustomButton.Image = null;
            this.password.CustomButton.Location = new System.Drawing.Point(134, 1);
            this.password.CustomButton.Name = "";
            this.password.CustomButton.Size = new System.Drawing.Size(21, 21);
            this.password.CustomButton.Style = MetroFramework.MetroColorStyle.Blue;
            this.password.CustomButton.TabIndex = 1;
            this.password.CustomButton.Theme = MetroFramework.MetroThemeStyle.Light;
            this.password.CustomButton.UseSelectable = true;
            this.password.CustomButton.Visible = false;
            this.password.Lines = new string[0];
            this.password.Location = new System.Drawing.Point(142, 187);
            this.password.MaxLength = 32767;
            this.password.Name = "password";
            this.password.PasswordChar = '\0';
            this.password.ScrollBars = System.Windows.Forms.ScrollBars.None;
            this.password.SelectedText = "";
            this.password.SelectionLength = 0;
            this.password.SelectionStart = 0;
            this.password.ShortcutsEnabled = true;
            this.password.Size = new System.Drawing.Size(156, 23);
            this.password.TabIndex = 26;
            this.password.Theme = MetroFramework.MetroThemeStyle.Light;
            this.password.UseSelectable = true;
            this.password.WaterMarkColor = System.Drawing.Color.FromArgb(((int)(((byte)(109)))), ((int)(((byte)(109)))), ((int)(((byte)(109)))));
            this.password.WaterMarkFont = new System.Drawing.Font("Segoe UI", 12F, System.Drawing.FontStyle.Italic, System.Drawing.GraphicsUnit.Pixel);
            // 
            // metroLabel3
            // 
            this.metroLabel3.AutoSize = true;
            this.metroLabel3.ForeColor = System.Drawing.Color.White;
            this.metroLabel3.Location = new System.Drawing.Point(25, 191);
            this.metroLabel3.Name = "metroLabel3";
            this.metroLabel3.Size = new System.Drawing.Size(63, 19);
            this.metroLabel3.TabIndex = 25;
            this.metroLabel3.Text = "Password";
            this.metroLabel3.Theme = MetroFramework.MetroThemeStyle.Light;
            // 
            // uname
            // 
            // 
            // 
            // 
            this.uname.CustomButton.Image = null;
            this.uname.CustomButton.Location = new System.Drawing.Point(134, 1);
            this.uname.CustomButton.Name = "";
            this.uname.CustomButton.Size = new System.Drawing.Size(21, 21);
            this.uname.CustomButton.Style = MetroFramework.MetroColorStyle.Blue;
            this.uname.CustomButton.TabIndex = 1;
            this.uname.CustomButton.Theme = MetroFramework.MetroThemeStyle.Light;
            this.uname.CustomButton.UseSelectable = true;
            this.uname.CustomButton.Visible = false;
            this.uname.Lines = new string[0];
            this.uname.Location = new System.Drawing.Point(142, 158);
            this.uname.MaxLength = 32767;
            this.uname.Name = "uname";
            this.uname.PasswordChar = '\0';
            this.uname.ScrollBars = System.Windows.Forms.ScrollBars.None;
            this.uname.SelectedText = "";
            this.uname.SelectionLength = 0;
            this.uname.SelectionStart = 0;
            this.uname.ShortcutsEnabled = true;
            this.uname.Size = new System.Drawing.Size(156, 23);
            this.uname.TabIndex = 24;
            this.uname.Theme = MetroFramework.MetroThemeStyle.Light;
            this.uname.UseSelectable = true;
            this.uname.WaterMarkColor = System.Drawing.Color.FromArgb(((int)(((byte)(109)))), ((int)(((byte)(109)))), ((int)(((byte)(109)))));
            this.uname.WaterMarkFont = new System.Drawing.Font("Segoe UI", 12F, System.Drawing.FontStyle.Italic, System.Drawing.GraphicsUnit.Pixel);
            // 
            // metroLabel4
            // 
            this.metroLabel4.AutoSize = true;
            this.metroLabel4.ForeColor = System.Drawing.Color.White;
            this.metroLabel4.Location = new System.Drawing.Point(25, 162);
            this.metroLabel4.Name = "metroLabel4";
            this.metroLabel4.Size = new System.Drawing.Size(75, 19);
            this.metroLabel4.TabIndex = 23;
            this.metroLabel4.Text = "User Name";
            this.metroLabel4.Theme = MetroFramework.MetroThemeStyle.Light;
            // 
            // dname
            // 
            // 
            // 
            // 
            this.dname.CustomButton.Image = null;
            this.dname.CustomButton.Location = new System.Drawing.Point(134, 1);
            this.dname.CustomButton.Name = "";
            this.dname.CustomButton.Size = new System.Drawing.Size(21, 21);
            this.dname.CustomButton.Style = MetroFramework.MetroColorStyle.Blue;
            this.dname.CustomButton.TabIndex = 1;
            this.dname.CustomButton.Theme = MetroFramework.MetroThemeStyle.Light;
            this.dname.CustomButton.UseSelectable = true;
            this.dname.CustomButton.Visible = false;
            this.dname.Lines = new string[0];
            this.dname.Location = new System.Drawing.Point(142, 100);
            this.dname.MaxLength = 32767;
            this.dname.Name = "dname";
            this.dname.PasswordChar = '\0';
            this.dname.ScrollBars = System.Windows.Forms.ScrollBars.None;
            this.dname.SelectedText = "";
            this.dname.SelectionLength = 0;
            this.dname.SelectionStart = 0;
            this.dname.ShortcutsEnabled = true;
            this.dname.Size = new System.Drawing.Size(156, 23);
            this.dname.TabIndex = 22;
            this.dname.Theme = MetroFramework.MetroThemeStyle.Light;
            this.dname.UseSelectable = true;
            this.dname.WaterMarkColor = System.Drawing.Color.FromArgb(((int)(((byte)(109)))), ((int)(((byte)(109)))), ((int)(((byte)(109)))));
            this.dname.WaterMarkFont = new System.Drawing.Font("Segoe UI", 12F, System.Drawing.FontStyle.Italic, System.Drawing.GraphicsUnit.Pixel);
            // 
            // metroLabel5
            // 
            this.metroLabel5.AutoSize = true;
            this.metroLabel5.ForeColor = System.Drawing.Color.White;
            this.metroLabel5.Location = new System.Drawing.Point(25, 104);
            this.metroLabel5.Name = "metroLabel5";
            this.metroLabel5.Size = new System.Drawing.Size(103, 19);
            this.metroLabel5.TabIndex = 21;
            this.metroLabel5.Text = "Database Name";
            this.metroLabel5.Theme = MetroFramework.MetroThemeStyle.Light;
            // 
            // sname
            // 
            // 
            // 
            // 
            this.sname.CustomButton.Image = null;
            this.sname.CustomButton.Location = new System.Drawing.Point(134, 1);
            this.sname.CustomButton.Name = "";
            this.sname.CustomButton.Size = new System.Drawing.Size(21, 21);
            this.sname.CustomButton.Style = MetroFramework.MetroColorStyle.Blue;
            this.sname.CustomButton.TabIndex = 1;
            this.sname.CustomButton.Theme = MetroFramework.MetroThemeStyle.Light;
            this.sname.CustomButton.UseSelectable = true;
            this.sname.CustomButton.Visible = false;
            this.sname.Lines = new string[0];
            this.sname.Location = new System.Drawing.Point(142, 71);
            this.sname.MaxLength = 32767;
            this.sname.Name = "sname";
            this.sname.PasswordChar = '\0';
            this.sname.ScrollBars = System.Windows.Forms.ScrollBars.None;
            this.sname.SelectedText = "";
            this.sname.SelectionLength = 0;
            this.sname.SelectionStart = 0;
            this.sname.ShortcutsEnabled = true;
            this.sname.Size = new System.Drawing.Size(156, 23);
            this.sname.TabIndex = 20;
            this.sname.Theme = MetroFramework.MetroThemeStyle.Light;
            this.sname.UseSelectable = true;
            this.sname.WaterMarkColor = System.Drawing.Color.FromArgb(((int)(((byte)(109)))), ((int)(((byte)(109)))), ((int)(((byte)(109)))));
            this.sname.WaterMarkFont = new System.Drawing.Font("Segoe UI", 12F, System.Drawing.FontStyle.Italic, System.Drawing.GraphicsUnit.Pixel);
            // 
            // metroLabel6
            // 
            this.metroLabel6.AutoSize = true;
            this.metroLabel6.ForeColor = System.Drawing.Color.White;
            this.metroLabel6.Location = new System.Drawing.Point(25, 75);
            this.metroLabel6.Name = "metroLabel6";
            this.metroLabel6.Size = new System.Drawing.Size(87, 19);
            this.metroLabel6.TabIndex = 19;
            this.metroLabel6.Text = "Server Name";
            this.metroLabel6.Theme = MetroFramework.MetroThemeStyle.Light;
            // 
            // AddForm
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(322, 280);
            this.Controls.Add(this.pnumber);
            this.Controls.Add(this.metroLabel8);
            this.Controls.Add(this.password);
            this.Controls.Add(this.metroLabel3);
            this.Controls.Add(this.uname);
            this.Controls.Add(this.metroLabel4);
            this.Controls.Add(this.dname);
            this.Controls.Add(this.metroLabel5);
            this.Controls.Add(this.sname);
            this.Controls.Add(this.metroLabel6);
            this.Controls.Add(this.cancel);
            this.Controls.Add(this.add);
            this.MaximizeBox = false;
            this.MinimizeBox = false;
            this.Name = "AddForm";
            this.Resizable = false;
            this.Text = "Add Database";
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion
        private MetroFramework.Controls.MetroButton add;
        private MetroFramework.Controls.MetroButton cancel;
        private MetroFramework.Controls.MetroTextBox pnumber;
        private MetroFramework.Controls.MetroLabel metroLabel8;
        private MetroFramework.Controls.MetroTextBox password;
        private MetroFramework.Controls.MetroLabel metroLabel3;
        private MetroFramework.Controls.MetroTextBox uname;
        private MetroFramework.Controls.MetroLabel metroLabel4;
        private MetroFramework.Controls.MetroTextBox dname;
        private MetroFramework.Controls.MetroLabel metroLabel5;
        private MetroFramework.Controls.MetroTextBox sname;
        private MetroFramework.Controls.MetroLabel metroLabel6;
    }
}