namespace tutorial
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
            this.submit_button = new System.Windows.Forms.Button();
            this.main_text = new System.Windows.Forms.Label();
            this.label1 = new System.Windows.Forms.Label();
            this.label2 = new System.Windows.Forms.Label();
            this.username_box = new System.Windows.Forms.TextBox();
            this.password_box = new System.Windows.Forms.TextBox();
            this.DebugButton = new System.Windows.Forms.Button();
            this.SuspendLayout();
            // 
            // submit_button
            // 
            this.submit_button.Location = new System.Drawing.Point(105, 99);
            this.submit_button.Name = "submit_button";
            this.submit_button.Size = new System.Drawing.Size(75, 23);
            this.submit_button.TabIndex = 0;
            this.submit_button.Text = "Submit";
            this.submit_button.UseVisualStyleBackColor = true;
            this.submit_button.Click += new System.EventHandler(this.submit_button_Click);
            // 
            // main_text
            // 
            this.main_text.AutoSize = true;
            this.main_text.Font = new System.Drawing.Font("Microsoft Sans Serif", 8.25F);
            this.main_text.Location = new System.Drawing.Point(71, 9);
            this.main_text.Name = "main_text";
            this.main_text.Size = new System.Drawing.Size(158, 13);
            this.main_text.TabIndex = 1;
            this.main_text.Text = "Please type your details to login.";
            this.main_text.TextAlign = System.Drawing.ContentAlignment.MiddleCenter;
            this.main_text.Click += new System.EventHandler(this.main_text_Click);
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Location = new System.Drawing.Point(12, 37);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(55, 13);
            this.label1.TabIndex = 2;
            this.label1.Text = "Username";
            // 
            // label2
            // 
            this.label2.AutoSize = true;
            this.label2.Location = new System.Drawing.Point(12, 68);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(53, 13);
            this.label2.TabIndex = 3;
            this.label2.Text = "Password";
            // 
            // username_box
            // 
            this.username_box.Location = new System.Drawing.Point(74, 37);
            this.username_box.Name = "username_box";
            this.username_box.Size = new System.Drawing.Size(198, 20);
            this.username_box.TabIndex = 4;
            this.username_box.Tag = "YouTube";
            this.username_box.Text = "YouTube";
            // 
            // password_box
            // 
            this.password_box.Location = new System.Drawing.Point(74, 68);
            this.password_box.Name = "password_box";
            this.password_box.PasswordChar = '*';
            this.password_box.Size = new System.Drawing.Size(198, 20);
            this.password_box.TabIndex = 5;
            this.password_box.Tag = "Videos_2_@";
            this.password_box.Text = "Videos_2_@";
            // 
            // DebugButton
            // 
            this.DebugButton.Location = new System.Drawing.Point(197, 99);
            this.DebugButton.Name = "DebugButton";
            this.DebugButton.Size = new System.Drawing.Size(75, 23);
            this.DebugButton.TabIndex = 6;
            this.DebugButton.Text = "Debug ";
            this.DebugButton.UseVisualStyleBackColor = true;
            this.DebugButton.Click += new System.EventHandler(this.DebugButton_Click);
            // 
            // Form1
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(284, 134);
            this.Controls.Add(this.DebugButton);
            this.Controls.Add(this.password_box);
            this.Controls.Add(this.username_box);
            this.Controls.Add(this.label2);
            this.Controls.Add(this.label1);
            this.Controls.Add(this.main_text);
            this.Controls.Add(this.submit_button);
            this.Name = "Form1";
            this.Text = "Login Menu";
            this.Load += new System.EventHandler(this.Form1_Load);
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Button submit_button;
        private System.Windows.Forms.Label main_text;
        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.Label label2;
        private System.Windows.Forms.TextBox username_box;
        private System.Windows.Forms.TextBox password_box;
        private System.Windows.Forms.Button DebugButton;
    }
}
