namespace abc
{
    partial class Form1
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
            System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(Form1));
            this.txt_kelas = new MetroFramework.Controls.MetroTextBox();
            this.txt_phone = new MetroFramework.Controls.MetroTextBox();
            this.txt_sex = new MetroFramework.Controls.MetroComboBox();
            this.txt_date = new MetroFramework.Controls.MetroDateTime();
            this.txt_alamat = new MetroFramework.Controls.MetroTextBox();
            this.txt_angkatan = new MetroFramework.Controls.MetroTextBox();
            this.txt_jurusan = new MetroFramework.Controls.MetroTextBox();
            this.txt_nim = new MetroFramework.Controls.MetroTextBox();
            this.txt_name = new MetroFramework.Controls.MetroTextBox();
            this.txt_password = new MetroFramework.Controls.MetroTextBox();
            this.txt_username = new MetroFramework.Controls.MetroTextBox();
            this.imageUpload = new System.Windows.Forms.PictureBox();
            this.imagePath = new System.Windows.Forms.Label();
            this.metroButton1 = new MetroFramework.Controls.MetroButton();
            ((System.ComponentModel.ISupportInitialize)(this.imageUpload)).BeginInit();
            this.SuspendLayout();
            // 
            // txt_kelas
            // 
            this.txt_kelas.BackColor = System.Drawing.SystemColors.ActiveCaption;
            // 
            // 
            // 
            this.txt_kelas.CustomButton.Image = null;
            this.txt_kelas.CustomButton.Location = new System.Drawing.Point(50, 2);
            this.txt_kelas.CustomButton.Margin = new System.Windows.Forms.Padding(2, 1, 2, 1);
            this.txt_kelas.CustomButton.Name = "";
            this.txt_kelas.CustomButton.Size = new System.Drawing.Size(17, 17);
            this.txt_kelas.CustomButton.Style = MetroFramework.MetroColorStyle.Blue;
            this.txt_kelas.CustomButton.TabIndex = 1;
            this.txt_kelas.CustomButton.Theme = MetroFramework.MetroThemeStyle.Light;
            this.txt_kelas.CustomButton.UseSelectable = true;
            this.txt_kelas.CustomButton.Visible = false;
            this.txt_kelas.Lines = new string[0];
            this.txt_kelas.Location = new System.Drawing.Point(433, 298);
            this.txt_kelas.MaxLength = 32767;
            this.txt_kelas.Name = "txt_kelas";
            this.txt_kelas.PasswordChar = '\0';
            this.txt_kelas.ScrollBars = System.Windows.Forms.ScrollBars.None;
            this.txt_kelas.SelectedText = "";
            this.txt_kelas.SelectionLength = 0;
            this.txt_kelas.SelectionStart = 0;
            this.txt_kelas.Size = new System.Drawing.Size(70, 22);
            this.txt_kelas.TabIndex = 24;
            this.txt_kelas.UseSelectable = true;
            this.txt_kelas.WaterMark = "Kelas";
            this.txt_kelas.WaterMarkColor = System.Drawing.Color.FromArgb(((int)(((byte)(109)))), ((int)(((byte)(109)))), ((int)(((byte)(109)))));
            this.txt_kelas.WaterMarkFont = new System.Drawing.Font("Segoe UI", 12F, System.Drawing.FontStyle.Italic, System.Drawing.GraphicsUnit.Pixel);
            // 
            // txt_phone
            // 
            this.txt_phone.BackColor = System.Drawing.SystemColors.ActiveCaption;
            // 
            // 
            // 
            this.txt_phone.CustomButton.Image = null;
            this.txt_phone.CustomButton.Location = new System.Drawing.Point(151, 2);
            this.txt_phone.CustomButton.Margin = new System.Windows.Forms.Padding(2, 1, 2, 1);
            this.txt_phone.CustomButton.Name = "";
            this.txt_phone.CustomButton.Size = new System.Drawing.Size(17, 17);
            this.txt_phone.CustomButton.Style = MetroFramework.MetroColorStyle.Blue;
            this.txt_phone.CustomButton.TabIndex = 1;
            this.txt_phone.CustomButton.Theme = MetroFramework.MetroThemeStyle.Light;
            this.txt_phone.CustomButton.UseSelectable = true;
            this.txt_phone.CustomButton.Visible = false;
            this.txt_phone.Lines = new string[0];
            this.txt_phone.Location = new System.Drawing.Point(332, 366);
            this.txt_phone.MaxLength = 32767;
            this.txt_phone.Name = "txt_phone";
            this.txt_phone.PasswordChar = '\0';
            this.txt_phone.ScrollBars = System.Windows.Forms.ScrollBars.None;
            this.txt_phone.SelectedText = "";
            this.txt_phone.SelectionLength = 0;
            this.txt_phone.SelectionStart = 0;
            this.txt_phone.Size = new System.Drawing.Size(171, 22);
            this.txt_phone.TabIndex = 26;
            this.txt_phone.UseSelectable = true;
            this.txt_phone.WaterMark = "Nomor Hp";
            this.txt_phone.WaterMarkColor = System.Drawing.Color.FromArgb(((int)(((byte)(109)))), ((int)(((byte)(109)))), ((int)(((byte)(109)))));
            this.txt_phone.WaterMarkFont = new System.Drawing.Font("Segoe UI", 12F, System.Drawing.FontStyle.Italic, System.Drawing.GraphicsUnit.Pixel);
            // 
            // txt_sex
            // 
            this.txt_sex.FormattingEnabled = true;
            this.txt_sex.ItemHeight = 23;
            this.txt_sex.Items.AddRange(new object[] {
            "Laki-Laki",
            "Perempuan"});
            this.txt_sex.Location = new System.Drawing.Point(330, 189);
            this.txt_sex.Name = "txt_sex";
            this.txt_sex.PromptText = "Jenis Kelamin";
            this.txt_sex.Size = new System.Drawing.Size(171, 29);
            this.txt_sex.TabIndex = 20;
            this.txt_sex.UseSelectable = true;
            // 
            // txt_date
            // 
            this.txt_date.CalendarFont = new System.Drawing.Font("Segoe UI Semilight", 8.25F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.txt_date.Location = new System.Drawing.Point(331, 148);
            this.txt_date.MinimumSize = new System.Drawing.Size(0, 29);
            this.txt_date.Name = "txt_date";
            this.txt_date.Size = new System.Drawing.Size(171, 29);
            this.txt_date.Style = MetroFramework.MetroColorStyle.Purple;
            this.txt_date.TabIndex = 19;
            this.txt_date.Tag = "";
            this.txt_date.Value = new System.DateTime(2015, 12, 1, 23, 53, 28, 0);
            // 
            // txt_alamat
            // 
            this.txt_alamat.BackColor = System.Drawing.SystemColors.ActiveCaption;
            // 
            // 
            // 
            this.txt_alamat.CustomButton.Image = null;
            this.txt_alamat.CustomButton.Location = new System.Drawing.Point(151, 2);
            this.txt_alamat.CustomButton.Margin = new System.Windows.Forms.Padding(2, 1, 2, 1);
            this.txt_alamat.CustomButton.Name = "";
            this.txt_alamat.CustomButton.Size = new System.Drawing.Size(17, 17);
            this.txt_alamat.CustomButton.Style = MetroFramework.MetroColorStyle.Blue;
            this.txt_alamat.CustomButton.TabIndex = 1;
            this.txt_alamat.CustomButton.Theme = MetroFramework.MetroThemeStyle.Light;
            this.txt_alamat.CustomButton.UseSelectable = true;
            this.txt_alamat.CustomButton.Visible = false;
            this.txt_alamat.Lines = new string[0];
            this.txt_alamat.Location = new System.Drawing.Point(331, 330);
            this.txt_alamat.MaxLength = 32767;
            this.txt_alamat.Name = "txt_alamat";
            this.txt_alamat.PasswordChar = '\0';
            this.txt_alamat.ScrollBars = System.Windows.Forms.ScrollBars.None;
            this.txt_alamat.SelectedText = "";
            this.txt_alamat.SelectionLength = 0;
            this.txt_alamat.SelectionStart = 0;
            this.txt_alamat.Size = new System.Drawing.Size(171, 22);
            this.txt_alamat.TabIndex = 25;
            this.txt_alamat.UseSelectable = true;
            this.txt_alamat.WaterMark = "Alamat";
            this.txt_alamat.WaterMarkColor = System.Drawing.Color.FromArgb(((int)(((byte)(109)))), ((int)(((byte)(109)))), ((int)(((byte)(109)))));
            this.txt_alamat.WaterMarkFont = new System.Drawing.Font("Segoe UI", 12F, System.Drawing.FontStyle.Italic, System.Drawing.GraphicsUnit.Pixel);
            // 
            // txt_angkatan
            // 
            this.txt_angkatan.BackColor = System.Drawing.SystemColors.ActiveCaption;
            // 
            // 
            // 
            this.txt_angkatan.CustomButton.Image = null;
            this.txt_angkatan.CustomButton.Location = new System.Drawing.Point(76, 2);
            this.txt_angkatan.CustomButton.Margin = new System.Windows.Forms.Padding(2, 1, 2, 1);
            this.txt_angkatan.CustomButton.Name = "";
            this.txt_angkatan.CustomButton.Size = new System.Drawing.Size(17, 17);
            this.txt_angkatan.CustomButton.Style = MetroFramework.MetroColorStyle.Blue;
            this.txt_angkatan.CustomButton.TabIndex = 1;
            this.txt_angkatan.CustomButton.Theme = MetroFramework.MetroThemeStyle.Light;
            this.txt_angkatan.CustomButton.UseSelectable = true;
            this.txt_angkatan.CustomButton.Visible = false;
            this.txt_angkatan.Lines = new string[0];
            this.txt_angkatan.Location = new System.Drawing.Point(331, 298);
            this.txt_angkatan.MaxLength = 32767;
            this.txt_angkatan.Name = "txt_angkatan";
            this.txt_angkatan.PasswordChar = '\0';
            this.txt_angkatan.ScrollBars = System.Windows.Forms.ScrollBars.None;
            this.txt_angkatan.SelectedText = "";
            this.txt_angkatan.SelectionLength = 0;
            this.txt_angkatan.SelectionStart = 0;
            this.txt_angkatan.Size = new System.Drawing.Size(96, 22);
            this.txt_angkatan.TabIndex = 23;
            this.txt_angkatan.UseSelectable = true;
            this.txt_angkatan.WaterMark = "Angkatan";
            this.txt_angkatan.WaterMarkColor = System.Drawing.Color.FromArgb(((int)(((byte)(109)))), ((int)(((byte)(109)))), ((int)(((byte)(109)))));
            this.txt_angkatan.WaterMarkFont = new System.Drawing.Font("Segoe UI", 12F, System.Drawing.FontStyle.Italic, System.Drawing.GraphicsUnit.Pixel);
            // 
            // txt_jurusan
            // 
            this.txt_jurusan.BackColor = System.Drawing.SystemColors.ActiveCaption;
            // 
            // 
            // 
            this.txt_jurusan.CustomButton.Image = null;
            this.txt_jurusan.CustomButton.Location = new System.Drawing.Point(151, 2);
            this.txt_jurusan.CustomButton.Margin = new System.Windows.Forms.Padding(2, 1, 2, 1);
            this.txt_jurusan.CustomButton.Name = "";
            this.txt_jurusan.CustomButton.Size = new System.Drawing.Size(17, 17);
            this.txt_jurusan.CustomButton.Style = MetroFramework.MetroColorStyle.Blue;
            this.txt_jurusan.CustomButton.TabIndex = 1;
            this.txt_jurusan.CustomButton.Theme = MetroFramework.MetroThemeStyle.Light;
            this.txt_jurusan.CustomButton.UseSelectable = true;
            this.txt_jurusan.CustomButton.Visible = false;
            this.txt_jurusan.Lines = new string[0];
            this.txt_jurusan.Location = new System.Drawing.Point(330, 264);
            this.txt_jurusan.MaxLength = 32767;
            this.txt_jurusan.Name = "txt_jurusan";
            this.txt_jurusan.PasswordChar = '\0';
            this.txt_jurusan.ScrollBars = System.Windows.Forms.ScrollBars.None;
            this.txt_jurusan.SelectedText = "";
            this.txt_jurusan.SelectionLength = 0;
            this.txt_jurusan.SelectionStart = 0;
            this.txt_jurusan.Size = new System.Drawing.Size(171, 22);
            this.txt_jurusan.TabIndex = 22;
            this.txt_jurusan.UseSelectable = true;
            this.txt_jurusan.WaterMark = "Jurusan";
            this.txt_jurusan.WaterMarkColor = System.Drawing.Color.FromArgb(((int)(((byte)(109)))), ((int)(((byte)(109)))), ((int)(((byte)(109)))));
            this.txt_jurusan.WaterMarkFont = new System.Drawing.Font("Segoe UI", 12F, System.Drawing.FontStyle.Italic, System.Drawing.GraphicsUnit.Pixel);
            // 
            // txt_nim
            // 
            this.txt_nim.BackColor = System.Drawing.SystemColors.ActiveCaption;
            // 
            // 
            // 
            this.txt_nim.CustomButton.Image = null;
            this.txt_nim.CustomButton.Location = new System.Drawing.Point(151, 2);
            this.txt_nim.CustomButton.Margin = new System.Windows.Forms.Padding(2, 1, 2, 1);
            this.txt_nim.CustomButton.Name = "";
            this.txt_nim.CustomButton.Size = new System.Drawing.Size(17, 17);
            this.txt_nim.CustomButton.Style = MetroFramework.MetroColorStyle.Blue;
            this.txt_nim.CustomButton.TabIndex = 1;
            this.txt_nim.CustomButton.Theme = MetroFramework.MetroThemeStyle.Light;
            this.txt_nim.CustomButton.UseSelectable = true;
            this.txt_nim.CustomButton.Visible = false;
            this.txt_nim.Lines = new string[0];
            this.txt_nim.Location = new System.Drawing.Point(330, 229);
            this.txt_nim.MaxLength = 32767;
            this.txt_nim.Name = "txt_nim";
            this.txt_nim.PasswordChar = '\0';
            this.txt_nim.ScrollBars = System.Windows.Forms.ScrollBars.None;
            this.txt_nim.SelectedText = "";
            this.txt_nim.SelectionLength = 0;
            this.txt_nim.SelectionStart = 0;
            this.txt_nim.Size = new System.Drawing.Size(171, 22);
            this.txt_nim.TabIndex = 21;
            this.txt_nim.UseSelectable = true;
            this.txt_nim.WaterMark = "NIM";
            this.txt_nim.WaterMarkColor = System.Drawing.Color.FromArgb(((int)(((byte)(109)))), ((int)(((byte)(109)))), ((int)(((byte)(109)))));
            this.txt_nim.WaterMarkFont = new System.Drawing.Font("Segoe UI", 12F, System.Drawing.FontStyle.Italic, System.Drawing.GraphicsUnit.Pixel);
            // 
            // txt_name
            // 
            this.txt_name.BackColor = System.Drawing.SystemColors.ActiveCaption;
            // 
            // 
            // 
            this.txt_name.CustomButton.Image = null;
            this.txt_name.CustomButton.Location = new System.Drawing.Point(151, 2);
            this.txt_name.CustomButton.Margin = new System.Windows.Forms.Padding(2, 1, 2, 1);
            this.txt_name.CustomButton.Name = "";
            this.txt_name.CustomButton.Size = new System.Drawing.Size(17, 17);
            this.txt_name.CustomButton.Style = MetroFramework.MetroColorStyle.Blue;
            this.txt_name.CustomButton.TabIndex = 1;
            this.txt_name.CustomButton.Theme = MetroFramework.MetroThemeStyle.Light;
            this.txt_name.CustomButton.UseSelectable = true;
            this.txt_name.CustomButton.Visible = false;
            this.txt_name.Lines = new string[0];
            this.txt_name.Location = new System.Drawing.Point(332, 112);
            this.txt_name.MaxLength = 32767;
            this.txt_name.Name = "txt_name";
            this.txt_name.PasswordChar = '\0';
            this.txt_name.ScrollBars = System.Windows.Forms.ScrollBars.None;
            this.txt_name.SelectedText = "";
            this.txt_name.SelectionLength = 0;
            this.txt_name.SelectionStart = 0;
            this.txt_name.Size = new System.Drawing.Size(171, 22);
            this.txt_name.TabIndex = 18;
            this.txt_name.UseSelectable = true;
            this.txt_name.WaterMark = "Nama Lengkap";
            this.txt_name.WaterMarkColor = System.Drawing.Color.FromArgb(((int)(((byte)(109)))), ((int)(((byte)(109)))), ((int)(((byte)(109)))));
            this.txt_name.WaterMarkFont = new System.Drawing.Font("Segoe UI", 12F, System.Drawing.FontStyle.Italic, System.Drawing.GraphicsUnit.Pixel);
            // 
            // txt_password
            // 
            this.txt_password.BackColor = System.Drawing.SystemColors.ActiveCaption;
            // 
            // 
            // 
            this.txt_password.CustomButton.Image = null;
            this.txt_password.CustomButton.Location = new System.Drawing.Point(151, 2);
            this.txt_password.CustomButton.Margin = new System.Windows.Forms.Padding(2, 1, 2, 1);
            this.txt_password.CustomButton.Name = "";
            this.txt_password.CustomButton.Size = new System.Drawing.Size(17, 17);
            this.txt_password.CustomButton.Style = MetroFramework.MetroColorStyle.Blue;
            this.txt_password.CustomButton.TabIndex = 1;
            this.txt_password.CustomButton.Theme = MetroFramework.MetroThemeStyle.Light;
            this.txt_password.CustomButton.UseSelectable = true;
            this.txt_password.CustomButton.Visible = false;
            this.txt_password.Lines = new string[0];
            this.txt_password.Location = new System.Drawing.Point(332, 76);
            this.txt_password.MaxLength = 32767;
            this.txt_password.Name = "txt_password";
            this.txt_password.PasswordChar = '●';
            this.txt_password.ScrollBars = System.Windows.Forms.ScrollBars.None;
            this.txt_password.SelectedText = "";
            this.txt_password.SelectionLength = 0;
            this.txt_password.SelectionStart = 0;
            this.txt_password.Size = new System.Drawing.Size(171, 22);
            this.txt_password.TabIndex = 17;
            this.txt_password.UseSelectable = true;
            this.txt_password.UseSystemPasswordChar = true;
            this.txt_password.WaterMark = "Password";
            this.txt_password.WaterMarkColor = System.Drawing.Color.FromArgb(((int)(((byte)(109)))), ((int)(((byte)(109)))), ((int)(((byte)(109)))));
            this.txt_password.WaterMarkFont = new System.Drawing.Font("Segoe UI", 12F, System.Drawing.FontStyle.Italic, System.Drawing.GraphicsUnit.Pixel);
            // 
            // txt_username
            // 
            this.txt_username.BackColor = System.Drawing.SystemColors.ActiveCaption;
            // 
            // 
            // 
            this.txt_username.CustomButton.Image = null;
            this.txt_username.CustomButton.Location = new System.Drawing.Point(151, 2);
            this.txt_username.CustomButton.Margin = new System.Windows.Forms.Padding(2, 1, 2, 1);
            this.txt_username.CustomButton.Name = "";
            this.txt_username.CustomButton.Size = new System.Drawing.Size(17, 17);
            this.txt_username.CustomButton.Style = MetroFramework.MetroColorStyle.Blue;
            this.txt_username.CustomButton.TabIndex = 1;
            this.txt_username.CustomButton.Theme = MetroFramework.MetroThemeStyle.Light;
            this.txt_username.CustomButton.UseSelectable = true;
            this.txt_username.CustomButton.Visible = false;
            this.txt_username.Lines = new string[0];
            this.txt_username.Location = new System.Drawing.Point(332, 42);
            this.txt_username.MaxLength = 32767;
            this.txt_username.Name = "txt_username";
            this.txt_username.PasswordChar = '\0';
            this.txt_username.ScrollBars = System.Windows.Forms.ScrollBars.None;
            this.txt_username.SelectedText = "";
            this.txt_username.SelectionLength = 0;
            this.txt_username.SelectionStart = 0;
            this.txt_username.Size = new System.Drawing.Size(171, 22);
            this.txt_username.TabIndex = 16;
            this.txt_username.UseSelectable = true;
            this.txt_username.WaterMark = "Username";
            this.txt_username.WaterMarkColor = System.Drawing.Color.FromArgb(((int)(((byte)(109)))), ((int)(((byte)(109)))), ((int)(((byte)(109)))));
            this.txt_username.WaterMarkFont = new System.Drawing.Font("Segoe UI", 12F, System.Drawing.FontStyle.Italic, System.Drawing.GraphicsUnit.Pixel);
            // 
            // imageUpload
            // 
            this.imageUpload.Cursor = System.Windows.Forms.Cursors.Hand;
            this.imageUpload.Image = ((System.Drawing.Image)(resources.GetObject("imageUpload.Image")));
            this.imageUpload.Location = new System.Drawing.Point(12, 43);
            this.imageUpload.Name = "imageUpload";
            this.imageUpload.Size = new System.Drawing.Size(268, 243);
            this.imageUpload.SizeMode = System.Windows.Forms.PictureBoxSizeMode.StretchImage;
            this.imageUpload.TabIndex = 29;
            this.imageUpload.TabStop = false;
            this.imageUpload.Click += new System.EventHandler(this.imageUpload_Click);
            // 
            // imagePath
            // 
            this.imagePath.AutoSize = true;
            this.imagePath.ForeColor = System.Drawing.SystemColors.Info;
            this.imagePath.Location = new System.Drawing.Point(9, 298);
            this.imagePath.Name = "imagePath";
            this.imagePath.Size = new System.Drawing.Size(35, 13);
            this.imagePath.TabIndex = 95;
            this.imagePath.Text = "label1";
            // 
            // metroButton1
            // 
            this.metroButton1.BackColor = System.Drawing.SystemColors.ActiveCaption;
            this.metroButton1.ForeColor = System.Drawing.SystemColors.ActiveCaption;
            this.metroButton1.Location = new System.Drawing.Point(97, 332);
            this.metroButton1.Name = "metroButton1";
            this.metroButton1.Size = new System.Drawing.Size(109, 56);
            this.metroButton1.TabIndex = 96;
            this.metroButton1.Text = "Save";
            this.metroButton1.UseSelectable = true;
            this.metroButton1.Click += new System.EventHandler(this.metroButton1_Click);
            // 
            // Form1
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(546, 446);
            this.Controls.Add(this.metroButton1);
            this.Controls.Add(this.imagePath);
            this.Controls.Add(this.imageUpload);
            this.Controls.Add(this.txt_kelas);
            this.Controls.Add(this.txt_phone);
            this.Controls.Add(this.txt_sex);
            this.Controls.Add(this.txt_date);
            this.Controls.Add(this.txt_alamat);
            this.Controls.Add(this.txt_angkatan);
            this.Controls.Add(this.txt_jurusan);
            this.Controls.Add(this.txt_nim);
            this.Controls.Add(this.txt_name);
            this.Controls.Add(this.txt_password);
            this.Controls.Add(this.txt_username);
            this.Name = "Form1";
            this.Text = "Form1";
            ((System.ComponentModel.ISupportInitialize)(this.imageUpload)).EndInit();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private MetroFramework.Controls.MetroTextBox txt_kelas;
        private MetroFramework.Controls.MetroTextBox txt_phone;
        private MetroFramework.Controls.MetroComboBox txt_sex;
        private MetroFramework.Controls.MetroDateTime txt_date;
        private MetroFramework.Controls.MetroTextBox txt_alamat;
        private MetroFramework.Controls.MetroTextBox txt_angkatan;
        private MetroFramework.Controls.MetroTextBox txt_jurusan;
        private MetroFramework.Controls.MetroTextBox txt_nim;
        private MetroFramework.Controls.MetroTextBox txt_name;
        private MetroFramework.Controls.MetroTextBox txt_password;
        private MetroFramework.Controls.MetroTextBox txt_username;
        private System.Windows.Forms.PictureBox imageUpload;
        private System.Windows.Forms.Label imagePath;
        private MetroFramework.Controls.MetroButton metroButton1;
    }
}