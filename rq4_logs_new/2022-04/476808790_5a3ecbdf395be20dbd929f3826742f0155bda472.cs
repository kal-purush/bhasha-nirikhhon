namespace NotSteam
{
    partial class AddFunds
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
            this.label18 = new System.Windows.Forms.Label();
            this.cbMoney = new System.Windows.Forms.ComboBox();
            this.label23 = new System.Windows.Forms.Label();
            this.textBox1 = new System.Windows.Forms.TextBox();
            this.label24 = new System.Windows.Forms.Label();
            this.textBox2 = new System.Windows.Forms.TextBox();
            this.cbTerms = new System.Windows.Forms.CheckBox();
            this.button1 = new System.Windows.Forms.Button();
            this.SuspendLayout();
            // 
            // label18
            // 
            this.label18.AutoSize = true;
            this.label18.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label18.Location = new System.Drawing.Point(31, 52);
            this.label18.Name = "label18";
            this.label18.Size = new System.Drawing.Size(255, 20);
            this.label18.TabIndex = 2;
            this.label18.Text = "Select the amount you want to add";
            // 
            // cbMoney
            // 
            this.cbMoney.FormattingEnabled = true;
            this.cbMoney.Items.AddRange(new object[] {
            "5",
            "10",
            "25",
            "50",
            "100"});
            this.cbMoney.Location = new System.Drawing.Point(81, 75);
            this.cbMoney.Name = "cbMoney";
            this.cbMoney.Size = new System.Drawing.Size(121, 21);
            this.cbMoney.TabIndex = 3;
            // 
            // label23
            // 
            this.label23.AutoSize = true;
            this.label23.Font = new System.Drawing.Font("Microsoft Sans Serif", 9.75F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label23.Location = new System.Drawing.Point(158, 188);
            this.label23.Name = "label23";
            this.label23.Size = new System.Drawing.Size(87, 16);
            this.label23.TabIndex = 4;
            this.label23.Text = "Card Number";
            // 
            // textBox1
            // 
            this.textBox1.Location = new System.Drawing.Point(81, 207);
            this.textBox1.Name = "textBox1";
            this.textBox1.Size = new System.Drawing.Size(251, 20);
            this.textBox1.TabIndex = 5;
            // 
            // label24
            // 
            this.label24.AutoSize = true;
            this.label24.Font = new System.Drawing.Font("Microsoft Sans Serif", 9.75F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label24.Location = new System.Drawing.Point(507, 188);
            this.label24.Name = "label24";
            this.label24.Size = new System.Drawing.Size(91, 16);
            this.label24.TabIndex = 6;
            this.label24.Text = "Security Code";
            // 
            // textBox2
            // 
            this.textBox2.Location = new System.Drawing.Point(498, 207);
            this.textBox2.Name = "textBox2";
            this.textBox2.Size = new System.Drawing.Size(100, 20);
            this.textBox2.TabIndex = 7;
            // 
            // cbTerms
            // 
            this.cbTerms.AutoSize = true;
            this.cbTerms.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.cbTerms.Location = new System.Drawing.Point(147, 296);
            this.cbTerms.Name = "cbTerms";
            this.cbTerms.Size = new System.Drawing.Size(424, 24);
            this.cbTerms.TabIndex = 8;
            this.cbTerms.Text = "I agree to the terms of NotSteam Subscriber Agreement";
            this.cbTerms.UseVisualStyleBackColor = true;
            // 
            // button1
            // 
            this.button1.Location = new System.Drawing.Point(334, 346);
            this.button1.Name = "button1";
            this.button1.Size = new System.Drawing.Size(75, 23);
            this.button1.TabIndex = 9;
            this.button1.Text = "Add Funds";
            this.button1.UseVisualStyleBackColor = true;
            this.button1.Click += new System.EventHandler(this.button1_Click);
            // 
            // AddFunds
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(800, 450);
            this.Controls.Add(this.button1);
            this.Controls.Add(this.cbTerms);
            this.Controls.Add(this.textBox2);
            this.Controls.Add(this.label24);
            this.Controls.Add(this.textBox1);
            this.Controls.Add(this.label23);
            this.Controls.Add(this.cbMoney);
            this.Controls.Add(this.label18);
            this.Name = "AddFunds";
            this.Text = "AddFunds";
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Label label18;
        private System.Windows.Forms.ComboBox cbMoney;
        private System.Windows.Forms.Label label23;
        private System.Windows.Forms.TextBox textBox1;
        private System.Windows.Forms.Label label24;
        private System.Windows.Forms.TextBox textBox2;
        private System.Windows.Forms.CheckBox cbTerms;
        private System.Windows.Forms.Button button1;
    }
}