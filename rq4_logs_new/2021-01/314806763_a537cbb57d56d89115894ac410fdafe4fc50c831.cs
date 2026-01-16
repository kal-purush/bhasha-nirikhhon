namespace wypozyczalniaSamochodow
{
    partial class RegistrationPanel
    {
        /// <summary> 
        /// Wymagana zmienna projektanta.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary> 
        /// Wyczyść wszystkie używane zasoby.
        /// </summary>
        /// <param name="disposing">prawda, jeżeli zarządzane zasoby powinny zostać zlikwidowane; Fałsz w przeciwnym wypadku.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Kod wygenerowany przez Projektanta składników

        /// <summary> 
        /// Metoda wymagana do obsługi projektanta — nie należy modyfikować 
        /// jej zawartości w edytorze kodu.
        /// </summary>
        private void InitializeComponent()
        {
            this.firstNameLabel = new System.Windows.Forms.Label();
            this.firstNameTextBox = new System.Windows.Forms.TextBox();
            this.lastNameLabel = new System.Windows.Forms.Label();
            this.lastNameTextBox = new System.Windows.Forms.TextBox();
            this.cityLabel = new System.Windows.Forms.Label();
            this.cityTextBox = new System.Windows.Forms.TextBox();
            this.adressLabel = new System.Windows.Forms.Label();
            this.addressTextBox = new System.Windows.Forms.TextBox();
            this.houseNumberLabel = new System.Windows.Forms.Label();
            this.houseNumberTextBox = new System.Windows.Forms.TextBox();
            this.apartmentNumberLabel = new System.Windows.Forms.Label();
            this.apartmentNumberTextBox = new System.Windows.Forms.TextBox();
            this.emailLabel = new System.Windows.Forms.Label();
            this.emailTextBox = new System.Windows.Forms.TextBox();
            this.passwordLabel = new System.Windows.Forms.Label();
            this.passwordTextBox = new System.Windows.Forms.TextBox();
            this.RegistrationButton = new System.Windows.Forms.Button();
            this.passwordLabel2 = new System.Windows.Forms.Label();
            this.passwordTextBox2 = new System.Windows.Forms.TextBox();
            this.BackToLoginButton = new System.Windows.Forms.Button();
            this.SuspendLayout();
            // 
            // firstNameLabel
            // 
            this.firstNameLabel.AutoSize = true;
            this.firstNameLabel.Location = new System.Drawing.Point(40, 48);
            this.firstNameLabel.Name = "firstNameLabel";
            this.firstNameLabel.Size = new System.Drawing.Size(78, 17);
            this.firstNameLabel.TabIndex = 0;
            this.firstNameLabel.Text = "First name:";
            // 
            // firstNameTextBox
            // 
            this.firstNameTextBox.Location = new System.Drawing.Point(175, 48);
            this.firstNameTextBox.Name = "firstNameTextBox";
            this.firstNameTextBox.Size = new System.Drawing.Size(158, 22);
            this.firstNameTextBox.TabIndex = 1;
            this.firstNameTextBox.Click += new System.EventHandler(this.firstNameTextBox_Click);
            // 
            // lastNameLabel
            // 
            this.lastNameLabel.AutoSize = true;
            this.lastNameLabel.Location = new System.Drawing.Point(40, 90);
            this.lastNameLabel.Name = "lastNameLabel";
            this.lastNameLabel.Size = new System.Drawing.Size(78, 17);
            this.lastNameLabel.TabIndex = 2;
            this.lastNameLabel.Text = "Last name:";
            // 
            // lastNameTextBox
            // 
            this.lastNameTextBox.Location = new System.Drawing.Point(175, 90);
            this.lastNameTextBox.Name = "lastNameTextBox";
            this.lastNameTextBox.Size = new System.Drawing.Size(158, 22);
            this.lastNameTextBox.TabIndex = 3;
            this.lastNameTextBox.Click += new System.EventHandler(this.lastNameTextBox_Click);
            // 
            // cityLabel
            // 
            this.cityLabel.AutoSize = true;
            this.cityLabel.Location = new System.Drawing.Point(40, 136);
            this.cityLabel.Name = "cityLabel";
            this.cityLabel.Size = new System.Drawing.Size(35, 17);
            this.cityLabel.TabIndex = 4;
            this.cityLabel.Text = "City:";
            // 
            // cityTextBox
            // 
            this.cityTextBox.Location = new System.Drawing.Point(175, 136);
            this.cityTextBox.Name = "cityTextBox";
            this.cityTextBox.Size = new System.Drawing.Size(158, 22);
            this.cityTextBox.TabIndex = 5;
            this.cityTextBox.Click += new System.EventHandler(this.cityTextBox_Click);
            // 
            // adressLabel
            // 
            this.adressLabel.AutoSize = true;
            this.adressLabel.Location = new System.Drawing.Point(40, 188);
            this.adressLabel.Name = "adressLabel";
            this.adressLabel.Size = new System.Drawing.Size(56, 17);
            this.adressLabel.TabIndex = 6;
            this.adressLabel.Text = "Adress:";
            // 
            // addressTextBox
            // 
            this.addressTextBox.Location = new System.Drawing.Point(175, 188);
            this.addressTextBox.Name = "addressTextBox";
            this.addressTextBox.Size = new System.Drawing.Size(158, 22);
            this.addressTextBox.TabIndex = 7;
            this.addressTextBox.Click += new System.EventHandler(this.addressTextBox_Click);
            // 
            // houseNumberLabel
            // 
            this.houseNumberLabel.AutoSize = true;
            this.houseNumberLabel.Location = new System.Drawing.Point(40, 237);
            this.houseNumberLabel.Name = "houseNumberLabel";
            this.houseNumberLabel.Size = new System.Drawing.Size(105, 17);
            this.houseNumberLabel.TabIndex = 8;
            this.houseNumberLabel.Text = "House number:";
            // 
            // houseNumberTextBox
            // 
            this.houseNumberTextBox.Location = new System.Drawing.Point(175, 237);
            this.houseNumberTextBox.Name = "houseNumberTextBox";
            this.houseNumberTextBox.Size = new System.Drawing.Size(158, 22);
            this.houseNumberTextBox.TabIndex = 9;
            this.houseNumberTextBox.Click += new System.EventHandler(this.houseNumberTextBox_Click);
            // 
            // apartmentNumberLabel
            // 
            this.apartmentNumberLabel.AutoSize = true;
            this.apartmentNumberLabel.Location = new System.Drawing.Point(40, 285);
            this.apartmentNumberLabel.Name = "apartmentNumberLabel";
            this.apartmentNumberLabel.Size = new System.Drawing.Size(129, 17);
            this.apartmentNumberLabel.TabIndex = 10;
            this.apartmentNumberLabel.Text = "Apartment number:";
            // 
            // apartmentNumberTextBox
            // 
            this.apartmentNumberTextBox.Location = new System.Drawing.Point(175, 285);
            this.apartmentNumberTextBox.Name = "apartmentNumberTextBox";
            this.apartmentNumberTextBox.Size = new System.Drawing.Size(158, 22);
            this.apartmentNumberTextBox.TabIndex = 11;
            this.apartmentNumberTextBox.Click += new System.EventHandler(this.apartmentNumberTextBox_Click);
            // 
            // emailLabel
            // 
            this.emailLabel.AutoSize = true;
            this.emailLabel.Location = new System.Drawing.Point(40, 369);
            this.emailLabel.Name = "emailLabel";
            this.emailLabel.Size = new System.Drawing.Size(46, 17);
            this.emailLabel.TabIndex = 12;
            this.emailLabel.Text = "Email:";
            // 
            // emailTextBox
            // 
            this.emailTextBox.Location = new System.Drawing.Point(175, 364);
            this.emailTextBox.Name = "emailTextBox";
            this.emailTextBox.Size = new System.Drawing.Size(158, 22);
            this.emailTextBox.TabIndex = 13;
            this.emailTextBox.Click += new System.EventHandler(this.emailTextBox_Click);
            // 
            // passwordLabel
            // 
            this.passwordLabel.AutoSize = true;
            this.passwordLabel.Location = new System.Drawing.Point(40, 410);
            this.passwordLabel.Name = "passwordLabel";
            this.passwordLabel.Size = new System.Drawing.Size(73, 17);
            this.passwordLabel.TabIndex = 14;
            this.passwordLabel.Text = "Password:";
            // 
            // passwordTextBox
            // 
            this.passwordTextBox.BackColor = System.Drawing.SystemColors.Window;
            this.passwordTextBox.Location = new System.Drawing.Point(175, 410);
            this.passwordTextBox.Name = "passwordTextBox";
            this.passwordTextBox.PasswordChar = '●';
            this.passwordTextBox.Size = new System.Drawing.Size(158, 22);
            this.passwordTextBox.TabIndex = 15;
            this.passwordTextBox.Click += new System.EventHandler(this.passwordTextBox_Click);
            // 
            // RegistrationButton
            // 
            this.RegistrationButton.Location = new System.Drawing.Point(43, 515);
            this.RegistrationButton.Name = "RegistrationButton";
            this.RegistrationButton.Size = new System.Drawing.Size(146, 37);
            this.RegistrationButton.TabIndex = 16;
            this.RegistrationButton.Text = "Register";
            this.RegistrationButton.UseVisualStyleBackColor = true;
            this.RegistrationButton.Click += new System.EventHandler(this.RegistrationButton_Click);
            // 
            // passwordLabel2
            // 
            this.passwordLabel2.AutoSize = true;
            this.passwordLabel2.Location = new System.Drawing.Point(40, 456);
            this.passwordLabel2.Name = "passwordLabel2";
            this.passwordLabel2.Size = new System.Drawing.Size(115, 17);
            this.passwordLabel2.TabIndex = 17;
            this.passwordLabel2.Text = "Check password:";
            // 
            // passwordTextBox2
            // 
            this.passwordTextBox2.Location = new System.Drawing.Point(175, 456);
            this.passwordTextBox2.Name = "passwordTextBox2";
            this.passwordTextBox2.PasswordChar = '●';
            this.passwordTextBox2.Size = new System.Drawing.Size(158, 22);
            this.passwordTextBox2.TabIndex = 18;
            this.passwordTextBox2.Click += new System.EventHandler(this.passwordTextBox2_Click);
            // 
            // BackToLoginButton
            // 
            this.BackToLoginButton.Location = new System.Drawing.Point(226, 516);
            this.BackToLoginButton.Name = "BackToLoginButton";
            this.BackToLoginButton.Size = new System.Drawing.Size(107, 36);
            this.BackToLoginButton.TabIndex = 19;
            this.BackToLoginButton.Text = "Powrót";
            this.BackToLoginButton.UseVisualStyleBackColor = true;
            this.BackToLoginButton.Click += new System.EventHandler(this.backToLoginButton_Click);
            // 
            // RegistrationPanel
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(8F, 16F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.Controls.Add(this.BackToLoginButton);
            this.Controls.Add(this.passwordTextBox2);
            this.Controls.Add(this.passwordLabel2);
            this.Controls.Add(this.RegistrationButton);
            this.Controls.Add(this.passwordTextBox);
            this.Controls.Add(this.passwordLabel);
            this.Controls.Add(this.emailTextBox);
            this.Controls.Add(this.emailLabel);
            this.Controls.Add(this.apartmentNumberTextBox);
            this.Controls.Add(this.apartmentNumberLabel);
            this.Controls.Add(this.houseNumberTextBox);
            this.Controls.Add(this.houseNumberLabel);
            this.Controls.Add(this.addressTextBox);
            this.Controls.Add(this.adressLabel);
            this.Controls.Add(this.cityTextBox);
            this.Controls.Add(this.cityLabel);
            this.Controls.Add(this.lastNameTextBox);
            this.Controls.Add(this.lastNameLabel);
            this.Controls.Add(this.firstNameTextBox);
            this.Controls.Add(this.firstNameLabel);
            this.Name = "RegistrationPanel";
            this.Size = new System.Drawing.Size(703, 633);
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Label firstNameLabel;
        private System.Windows.Forms.TextBox firstNameTextBox;
        private System.Windows.Forms.Label lastNameLabel;
        private System.Windows.Forms.TextBox lastNameTextBox;
        private System.Windows.Forms.Label cityLabel;
        private System.Windows.Forms.TextBox cityTextBox;
        private System.Windows.Forms.Label adressLabel;
        private System.Windows.Forms.TextBox addressTextBox;
        private System.Windows.Forms.Label houseNumberLabel;
        private System.Windows.Forms.TextBox houseNumberTextBox;
        private System.Windows.Forms.Label apartmentNumberLabel;
        private System.Windows.Forms.TextBox apartmentNumberTextBox;
        private System.Windows.Forms.Label emailLabel;
        private System.Windows.Forms.TextBox emailTextBox;
        private System.Windows.Forms.Label passwordLabel;
        private System.Windows.Forms.TextBox passwordTextBox;
        private System.Windows.Forms.Button RegistrationButton;
        private System.Windows.Forms.Label passwordLabel2;
        private System.Windows.Forms.TextBox passwordTextBox2;
        private System.Windows.Forms.Button BackToLoginButton;
    }
}