namespace LoginForm
{
    partial class ForgetPasswordForm
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
            System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(ForgetPasswordForm));
            pictureBox1 = new PictureBox();
            OTPButton = new Button();
            ResetPassButton = new Button();
            panel5 = new Panel();
            OTPBox = new TextBox();
            pictureBox7 = new PictureBox();
            panel4 = new Panel();
            ReTypePasswordBox = new TextBox();
            pictureBox6 = new PictureBox();
            panel3 = new Panel();
            EmailBox = new TextBox();
            pictureBox5 = new PictureBox();
            StatusText = new Label();
            panel2 = new Panel();
            PasswordBox = new TextBox();
            pictureBox4 = new PictureBox();
            label2 = new Label();
            label1 = new Label();
            pictureBox2 = new PictureBox();
            label4 = new Label();
            SignInLink = new LinkLabel();
            label3 = new Label();
            panel1 = new Panel();
            AccountBox = new TextBox();
            pictureBox3 = new PictureBox();
            ((System.ComponentModel.ISupportInitialize)pictureBox1).BeginInit();
            panel5.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)pictureBox7).BeginInit();
            panel4.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)pictureBox6).BeginInit();
            panel3.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)pictureBox5).BeginInit();
            panel2.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)pictureBox4).BeginInit();
            ((System.ComponentModel.ISupportInitialize)pictureBox2).BeginInit();
            panel1.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)pictureBox3).BeginInit();
            SuspendLayout();
            // 
            // pictureBox1
            // 
            pictureBox1.Image = (Image)resources.GetObject("pictureBox1.Image");
            pictureBox1.Location = new Point(-1, -1);
            pictureBox1.Name = "pictureBox1";
            pictureBox1.Size = new Size(467, 743);
            pictureBox1.SizeMode = PictureBoxSizeMode.StretchImage;
            pictureBox1.TabIndex = 1;
            pictureBox1.TabStop = false;
            // 
            // OTPButton
            // 
            OTPButton.BackgroundImage = (Image)resources.GetObject("OTPButton.BackgroundImage");
            OTPButton.BackgroundImageLayout = ImageLayout.Stretch;
            OTPButton.FlatAppearance.BorderSize = 0;
            OTPButton.FlatStyle = FlatStyle.Flat;
            OTPButton.Font = new Font("Microsoft Sans Serif", 13.8F, FontStyle.Bold, GraphicsUnit.Point, 0);
            OTPButton.Location = new Point(947, 299);
            OTPButton.Name = "OTPButton";
            OTPButton.Size = new Size(154, 62);
            OTPButton.TabIndex = 44;
            OTPButton.Text = "Send OTP";
            OTPButton.UseVisualStyleBackColor = true;
            OTPButton.Click += OTPButton_Click;
            // 
            // ResetPassButton
            // 
            ResetPassButton.BackgroundImage = (Image)resources.GetObject("ResetPassButton.BackgroundImage");
            ResetPassButton.BackgroundImageLayout = ImageLayout.Stretch;
            ResetPassButton.FlatAppearance.BorderSize = 0;
            ResetPassButton.FlatStyle = FlatStyle.Flat;
            ResetPassButton.Font = new Font("Microsoft Sans Serif", 18F, FontStyle.Bold, GraphicsUnit.Point, 0);
            ResetPassButton.Location = new Point(653, 503);
            ResetPassButton.Name = "ResetPassButton";
            ResetPassButton.Size = new Size(275, 78);
            ResetPassButton.TabIndex = 43;
            ResetPassButton.Text = "Reset password";
            ResetPassButton.UseVisualStyleBackColor = true;
            ResetPassButton.Click += ResetPassButton_Click;
            // 
            // panel5
            // 
            panel5.BackColor = Color.Azure;
            panel5.Controls.Add(OTPBox);
            panel5.Controls.Add(pictureBox7);
            panel5.Location = new Point(472, 299);
            panel5.Name = "panel5";
            panel5.Size = new Size(469, 62);
            panel5.TabIndex = 42;
            // 
            // OTPBox
            // 
            OTPBox.BackColor = Color.Azure;
            OTPBox.BorderStyle = BorderStyle.None;
            OTPBox.Font = new Font("Segoe UI", 13.8F, FontStyle.Bold, GraphicsUnit.Point, 0);
            OTPBox.ForeColor = SystemColors.InactiveCaption;
            OTPBox.Location = new Point(131, 17);
            OTPBox.Name = "OTPBox";
            OTPBox.Size = new Size(272, 31);
            OTPBox.TabIndex = 7;
            OTPBox.Text = "Enter OTP";
            OTPBox.Enter += OTPBox_Enter;
            OTPBox.Leave += OTPBox_Leave;
            // 
            // pictureBox7
            // 
            pictureBox7.Image = (Image)resources.GetObject("pictureBox7.Image");
            pictureBox7.Location = new Point(0, 0);
            pictureBox7.Name = "pictureBox7";
            pictureBox7.Size = new Size(125, 62);
            pictureBox7.SizeMode = PictureBoxSizeMode.Zoom;
            pictureBox7.TabIndex = 6;
            pictureBox7.TabStop = false;
            // 
            // panel4
            // 
            panel4.BackColor = Color.Azure;
            panel4.Controls.Add(ReTypePasswordBox);
            panel4.Controls.Add(pictureBox6);
            panel4.Location = new Point(472, 435);
            panel4.Name = "panel4";
            panel4.Size = new Size(629, 62);
            panel4.TabIndex = 41;
            // 
            // ReTypePasswordBox
            // 
            ReTypePasswordBox.BackColor = Color.Azure;
            ReTypePasswordBox.BorderStyle = BorderStyle.None;
            ReTypePasswordBox.Font = new Font("Segoe UI", 13.8F, FontStyle.Bold, GraphicsUnit.Point, 0);
            ReTypePasswordBox.ForeColor = SystemColors.InactiveCaption;
            ReTypePasswordBox.Location = new Point(131, 17);
            ReTypePasswordBox.Name = "ReTypePasswordBox";
            ReTypePasswordBox.Size = new Size(451, 31);
            ReTypePasswordBox.TabIndex = 7;
            ReTypePasswordBox.Text = "Retype password";
            ReTypePasswordBox.Enter += ReTypePasswordBox_Enter;
            ReTypePasswordBox.Leave += ReTypePasswordBox_Leave;
            // 
            // pictureBox6
            // 
            pictureBox6.Image = (Image)resources.GetObject("pictureBox6.Image");
            pictureBox6.Location = new Point(0, 0);
            pictureBox6.Name = "pictureBox6";
            pictureBox6.Size = new Size(125, 62);
            pictureBox6.SizeMode = PictureBoxSizeMode.Zoom;
            pictureBox6.TabIndex = 6;
            pictureBox6.TabStop = false;
            // 
            // panel3
            // 
            panel3.BackColor = Color.Azure;
            panel3.Controls.Add(EmailBox);
            panel3.Controls.Add(pictureBox5);
            panel3.Location = new Point(472, 231);
            panel3.Name = "panel3";
            panel3.Size = new Size(629, 62);
            panel3.TabIndex = 40;
            // 
            // EmailBox
            // 
            EmailBox.BackColor = Color.Azure;
            EmailBox.BorderStyle = BorderStyle.None;
            EmailBox.Font = new Font("Segoe UI", 13.8F, FontStyle.Bold, GraphicsUnit.Point, 0);
            EmailBox.ForeColor = SystemColors.InactiveCaption;
            EmailBox.Location = new Point(131, 15);
            EmailBox.Name = "EmailBox";
            EmailBox.Size = new Size(451, 31);
            EmailBox.TabIndex = 1;
            EmailBox.Text = "Enter email";
            EmailBox.Enter += EmailBox_Enter;
            EmailBox.Leave += EmailBox_Leave;
            // 
            // pictureBox5
            // 
            pictureBox5.Image = (Image)resources.GetObject("pictureBox5.Image");
            pictureBox5.Location = new Point(0, 0);
            pictureBox5.Name = "pictureBox5";
            pictureBox5.Size = new Size(125, 62);
            pictureBox5.SizeMode = PictureBoxSizeMode.Zoom;
            pictureBox5.TabIndex = 0;
            pictureBox5.TabStop = false;
            // 
            // StatusText
            // 
            StatusText.AutoSize = true;
            StatusText.Font = new Font("Microsoft Sans Serif", 18F, FontStyle.Bold, GraphicsUnit.Point, 0);
            StatusText.ForeColor = Color.Red;
            StatusText.Location = new Point(472, 584);
            StatusText.Name = "StatusText";
            StatusText.Size = new Size(440, 36);
            StatusText.TabIndex = 39;
            StatusText.Text = "Current Status: Not Logged In";
            // 
            // panel2
            // 
            panel2.BackColor = Color.Azure;
            panel2.Controls.Add(PasswordBox);
            panel2.Controls.Add(pictureBox4);
            panel2.Location = new Point(472, 367);
            panel2.Name = "panel2";
            panel2.Size = new Size(629, 62);
            panel2.TabIndex = 38;
            // 
            // PasswordBox
            // 
            PasswordBox.BackColor = Color.Azure;
            PasswordBox.BorderStyle = BorderStyle.None;
            PasswordBox.Font = new Font("Segoe UI", 13.8F, FontStyle.Bold, GraphicsUnit.Point, 0);
            PasswordBox.ForeColor = SystemColors.InactiveCaption;
            PasswordBox.Location = new Point(131, 17);
            PasswordBox.Name = "PasswordBox";
            PasswordBox.Size = new Size(451, 31);
            PasswordBox.TabIndex = 7;
            PasswordBox.Text = "Enter password";
            PasswordBox.Enter += PasswordBox_Enter;
            PasswordBox.Leave += PasswordBox_Leave;
            // 
            // pictureBox4
            // 
            pictureBox4.Image = (Image)resources.GetObject("pictureBox4.Image");
            pictureBox4.Location = new Point(0, 0);
            pictureBox4.Name = "pictureBox4";
            pictureBox4.Size = new Size(125, 62);
            pictureBox4.SizeMode = PictureBoxSizeMode.Zoom;
            pictureBox4.TabIndex = 6;
            pictureBox4.TabStop = false;
            // 
            // label2
            // 
            label2.AutoSize = true;
            label2.Font = new Font("Segoe UI", 25.8000011F, FontStyle.Bold | FontStyle.Italic, GraphicsUnit.Point, 0);
            label2.Location = new Point(474, 90);
            label2.Name = "label2";
            label2.Size = new Size(618, 60);
            label2.TabIndex = 36;
            label2.Text = "Reset your password account";
            // 
            // label1
            // 
            label1.AutoSize = true;
            label1.Font = new Font("Segoe UI", 25.8000011F, FontStyle.Bold | FontStyle.Italic, GraphicsUnit.Point, 0);
            label1.Location = new Point(487, -1);
            label1.Name = "label1";
            label1.Size = new Size(268, 60);
            label1.TabIndex = 35;
            label1.Text = "Welcome to";
            // 
            // pictureBox2
            // 
            pictureBox2.BackColor = Color.Transparent;
            pictureBox2.Image = (Image)resources.GetObject("pictureBox2.Image");
            pictureBox2.Location = new Point(761, 12);
            pictureBox2.Name = "pictureBox2";
            pictureBox2.Size = new Size(230, 75);
            pictureBox2.SizeMode = PictureBoxSizeMode.AutoSize;
            pictureBox2.TabIndex = 34;
            pictureBox2.TabStop = false;
            // 
            // label4
            // 
            label4.AutoSize = true;
            label4.Font = new Font("Microsoft Sans Serif", 13.8F, FontStyle.Bold, GraphicsUnit.Point, 0);
            label4.Location = new Point(746, 713);
            label4.Name = "label4";
            label4.Size = new Size(279, 29);
            label4.TabIndex = 47;
            label4.Text = "and begin your journey";
            // 
            // SignInLink
            // 
            SignInLink.AutoSize = true;
            SignInLink.Font = new Font("Microsoft Sans Serif", 13.8F, FontStyle.Bold, GraphicsUnit.Point, 0);
            SignInLink.Location = new Point(784, 673);
            SignInLink.Name = "SignInLink";
            SignInLink.Size = new Size(180, 29);
            SignInLink.TabIndex = 46;
            SignInLink.TabStop = true;
            SignInLink.Text = "Back to SignIn";
            SignInLink.LinkClicked += SignInLink_LinkClicked;
            // 
            // label3
            // 
            label3.AutoSize = true;
            label3.Font = new Font("Microsoft Sans Serif", 13.8F, FontStyle.Bold, GraphicsUnit.Point, 0);
            label3.Location = new Point(477, 673);
            label3.Name = "label3";
            label3.Size = new Size(308, 29);
            label3.TabIndex = 45;
            label3.Text = "Already have an account?";
            // 
            // panel1
            // 
            panel1.BackColor = Color.Azure;
            panel1.Controls.Add(AccountBox);
            panel1.Controls.Add(pictureBox3);
            panel1.Location = new Point(472, 163);
            panel1.Name = "panel1";
            panel1.Size = new Size(629, 62);
            panel1.TabIndex = 5;
            // 
            // AccountBox
            // 
            AccountBox.BackColor = Color.Azure;
            AccountBox.BorderStyle = BorderStyle.None;
            AccountBox.Font = new Font("Segoe UI", 13.8F, FontStyle.Bold, GraphicsUnit.Point, 0);
            AccountBox.ForeColor = SystemColors.ActiveCaption;
            AccountBox.Location = new Point(131, 15);
            AccountBox.Name = "AccountBox";
            AccountBox.Size = new Size(451, 31);
            AccountBox.TabIndex = 1;
            AccountBox.Text = "Enter username";
            AccountBox.Enter += AccountBox_Enter;
            AccountBox.Leave += AccountBox_Leave;
            // 
            // pictureBox3
            // 
            pictureBox3.Image = (Image)resources.GetObject("pictureBox3.Image");
            pictureBox3.Location = new Point(0, 0);
            pictureBox3.Name = "pictureBox3";
            pictureBox3.Size = new Size(125, 62);
            pictureBox3.SizeMode = PictureBoxSizeMode.Zoom;
            pictureBox3.TabIndex = 0;
            pictureBox3.TabStop = false;
            // 
            // ForgetPasswordForm
            // 
            AutoScaleDimensions = new SizeF(8F, 20F);
            AutoScaleMode = AutoScaleMode.Font;
            BackColor = Color.Aquamarine;
            ClientSize = new Size(1097, 744);
            Controls.Add(panel1);
            Controls.Add(label4);
            Controls.Add(SignInLink);
            Controls.Add(label3);
            Controls.Add(OTPButton);
            Controls.Add(ResetPassButton);
            Controls.Add(panel5);
            Controls.Add(panel4);
            Controls.Add(panel3);
            Controls.Add(StatusText);
            Controls.Add(panel2);
            Controls.Add(label2);
            Controls.Add(label1);
            Controls.Add(pictureBox2);
            Controls.Add(pictureBox1);
            Name = "ForgetPasswordForm";
            Text = "ForgetPasswordForm";
            Load += ForgetPasswordForm_Load;
            ((System.ComponentModel.ISupportInitialize)pictureBox1).EndInit();
            panel5.ResumeLayout(false);
            panel5.PerformLayout();
            ((System.ComponentModel.ISupportInitialize)pictureBox7).EndInit();
            panel4.ResumeLayout(false);
            panel4.PerformLayout();
            ((System.ComponentModel.ISupportInitialize)pictureBox6).EndInit();
            panel3.ResumeLayout(false);
            panel3.PerformLayout();
            ((System.ComponentModel.ISupportInitialize)pictureBox5).EndInit();
            panel2.ResumeLayout(false);
            panel2.PerformLayout();
            ((System.ComponentModel.ISupportInitialize)pictureBox4).EndInit();
            ((System.ComponentModel.ISupportInitialize)pictureBox2).EndInit();
            panel1.ResumeLayout(false);
            panel1.PerformLayout();
            ((System.ComponentModel.ISupportInitialize)pictureBox3).EndInit();
            ResumeLayout(false);
            PerformLayout();
        }

        #endregion

        private PictureBox pictureBox1;
        private Button OTPButton;
        private Button ResetPassButton;
        private Panel panel5;
        private TextBox OTPBox;
        private PictureBox pictureBox7;
        private Panel panel4;
        private TextBox ReTypePasswordBox;
        private PictureBox pictureBox6;
        private Panel panel3;
        private TextBox EmailBox;
        private PictureBox pictureBox5;
        private Label StatusText;
        private Panel panel2;
        private TextBox PasswordBox;
        private PictureBox pictureBox4;
        private Label label2;
        private Label label1;
        private PictureBox pictureBox2;
        private Label label4;
        private LinkLabel SignInLink;
        private Label label3;
        private Panel panel1;
        private TextBox AccountBox;
        private PictureBox pictureBox3;
    }
}