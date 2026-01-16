namespace RD_Client
{
    partial class Form1
    {
        /// <summary>
        ///  Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        ///  Clean up any resources being used.
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
        ///  Required method for Designer support - do not modify
        ///  the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            label1 = new Label();
            tbIP = new TextBox();
            tbPassword = new TextBox();
            label2 = new Label();
            btConnect = new Button();
            SuspendLayout();
            // 
            // label1
            // 
            label1.AutoSize = true;
            label1.Location = new Point(25, 25);
            label1.Name = "label1";
            label1.Size = new Size(100, 19);
            label1.TabIndex = 0;
            label1.Text = "IP remote host";
            // 
            // tbIP
            // 
            tbIP.Location = new Point(25, 47);
            tbIP.Name = "tbIP";
            tbIP.Size = new Size(219, 25);
            tbIP.TabIndex = 1;
            // 
            // tbPassword
            // 
            tbPassword.Location = new Point(25, 121);
            tbPassword.Name = "tbPassword";
            tbPassword.Size = new Size(219, 25);
            tbPassword.TabIndex = 3;
            // 
            // label2
            // 
            label2.AutoSize = true;
            label2.Location = new Point(25, 99);
            label2.Name = "label2";
            label2.Size = new Size(67, 19);
            label2.TabIndex = 2;
            label2.Text = "Password";
            // 
            // btConnect
            // 
            btConnect.Location = new Point(323, 25);
            btConnect.Name = "btConnect";
            btConnect.Size = new Size(144, 149);
            btConnect.TabIndex = 4;
            btConnect.Text = "Connect";
            btConnect.UseVisualStyleBackColor = true;
            btConnect.Click += btConnect_Click;
            // 
            // Form1
            // 
            AutoScaleDimensions = new SizeF(7F, 17F);
            AutoScaleMode = AutoScaleMode.Font;
            ClientSize = new Size(502, 195);
            Controls.Add(btConnect);
            Controls.Add(tbPassword);
            Controls.Add(label2);
            Controls.Add(tbIP);
            Controls.Add(label1);
            Font = new Font("Segoe UI", 10F, FontStyle.Regular, GraphicsUnit.Point);
            FormBorderStyle = FormBorderStyle.FixedSingle;
            MaximizeBox = false;
            Name = "Form1";
            Text = "Client";
            FormClosing += Form1_FormClosing;
            ResumeLayout(false);
            PerformLayout();
        }

        #endregion

        private Label label1;
        private TextBox tbIP;
        private TextBox tbPassword;
        private Label label2;
        private Button btConnect;
    }
}