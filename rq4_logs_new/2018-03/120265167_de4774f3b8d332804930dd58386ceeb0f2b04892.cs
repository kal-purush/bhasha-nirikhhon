namespace GolfApp
{
    partial class frmForgotPassword
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
            this.label1 = new System.Windows.Forms.Label();
            this.lblNewPsswrd = new System.Windows.Forms.Label();
            this.lblVerifyPsswrd = new System.Windows.Forms.Label();
            this.textBox1 = new System.Windows.Forms.TextBox();
            this.textBox2 = new System.Windows.Forms.TextBox();
            this.btnSubmit = new System.Windows.Forms.Button();
            this.SuspendLayout();
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Location = new System.Drawing.Point(172, 9);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(166, 20);
            this.label1.TabIndex = 0;
            this.label1.Text = "Password Reset Form";
            // 
            // lblNewPsswrd
            // 
            this.lblNewPsswrd.AutoSize = true;
            this.lblNewPsswrd.Location = new System.Drawing.Point(75, 97);
            this.lblNewPsswrd.Name = "lblNewPsswrd";
            this.lblNewPsswrd.Size = new System.Drawing.Size(113, 20);
            this.lblNewPsswrd.TabIndex = 1;
            this.lblNewPsswrd.Text = "New Password";
            // 
            // lblVerifyPsswrd
            // 
            this.lblVerifyPsswrd.AutoSize = true;
            this.lblVerifyPsswrd.Location = new System.Drawing.Point(75, 183);
            this.lblVerifyPsswrd.Name = "lblVerifyPsswrd";
            this.lblVerifyPsswrd.Size = new System.Drawing.Size(154, 20);
            this.lblVerifyPsswrd.TabIndex = 2;
            this.lblVerifyPsswrd.Text = "Verify new password";
            // 
            // textBox1
            // 
            this.textBox1.Location = new System.Drawing.Point(316, 97);
            this.textBox1.Name = "textBox1";
            this.textBox1.Size = new System.Drawing.Size(100, 26);
            this.textBox1.TabIndex = 3;
            // 
            // textBox2
            // 
            this.textBox2.Location = new System.Drawing.Point(316, 177);
            this.textBox2.Name = "textBox2";
            this.textBox2.Size = new System.Drawing.Size(100, 26);
            this.textBox2.TabIndex = 4;
            // 
            // btnSubmit
            // 
            this.btnSubmit.Location = new System.Drawing.Point(217, 289);
            this.btnSubmit.Name = "btnSubmit";
            this.btnSubmit.Size = new System.Drawing.Size(104, 56);
            this.btnSubmit.TabIndex = 5;
            this.btnSubmit.Text = "Submit";
            this.btnSubmit.UseVisualStyleBackColor = true;
            // 
            // ForgotPassword
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(9F, 20F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(504, 398);
            this.Controls.Add(this.btnSubmit);
            this.Controls.Add(this.textBox2);
            this.Controls.Add(this.textBox1);
            this.Controls.Add(this.lblVerifyPsswrd);
            this.Controls.Add(this.lblNewPsswrd);
            this.Controls.Add(this.label1);
            this.Name = "ForgotPassword";
            this.Text = "ForgotPassword";
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.Label lblNewPsswrd;
        private System.Windows.Forms.Label lblVerifyPsswrd;
        private System.Windows.Forms.TextBox textBox1;
        private System.Windows.Forms.TextBox textBox2;
        private System.Windows.Forms.Button btnSubmit;
    }
}