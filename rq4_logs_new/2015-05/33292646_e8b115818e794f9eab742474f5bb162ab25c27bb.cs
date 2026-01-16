namespace WindowsFormsApplication1
{
    partial class Form_Add
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
            this.add_button = new System.Windows.Forms.Button();
            this.add_sex_textBox = new System.Windows.Forms.TextBox();
            this.label4 = new System.Windows.Forms.Label();
            this.add_job_textBox = new System.Windows.Forms.TextBox();
            this.add_age_textBox = new System.Windows.Forms.TextBox();
            this.add_FIO_textBox = new System.Windows.Forms.TextBox();
            this.label3 = new System.Windows.Forms.Label();
            this.label2 = new System.Windows.Forms.Label();
            this.label1 = new System.Windows.Forms.Label();
            this.add_aq_textBox = new System.Windows.Forms.TextBox();
            this.label5 = new System.Windows.Forms.Label();
            this.add_children_textBox = new System.Windows.Forms.TextBox();
            this.label6 = new System.Windows.Forms.Label();
            this.SuspendLayout();
            // 
            // add_button
            // 
            this.add_button.Location = new System.Drawing.Point(86, 208);
            this.add_button.Name = "add_button";
            this.add_button.Size = new System.Drawing.Size(123, 53);
            this.add_button.TabIndex = 0;
            this.add_button.Text = "Добавить";
            this.add_button.UseVisualStyleBackColor = true;
            this.add_button.Click += new System.EventHandler(this.button1_Click);
            // 
            // add_sex_textBox
            // 
            this.add_sex_textBox.Location = new System.Drawing.Point(86, 107);
            this.add_sex_textBox.Name = "add_sex_textBox";
            this.add_sex_textBox.Size = new System.Drawing.Size(193, 20);
            this.add_sex_textBox.TabIndex = 16;
            // 
            // label4
            // 
            this.label4.AutoSize = true;
            this.label4.Location = new System.Drawing.Point(12, 107);
            this.label4.Name = "label4";
            this.label4.Size = new System.Drawing.Size(27, 13);
            this.label4.TabIndex = 15;
            this.label4.Text = "Пол";
            // 
            // add_job_textBox
            // 
            this.add_job_textBox.Location = new System.Drawing.Point(86, 76);
            this.add_job_textBox.Name = "add_job_textBox";
            this.add_job_textBox.Size = new System.Drawing.Size(193, 20);
            this.add_job_textBox.TabIndex = 14;
            // 
            // add_age_textBox
            // 
            this.add_age_textBox.Location = new System.Drawing.Point(86, 46);
            this.add_age_textBox.Name = "add_age_textBox";
            this.add_age_textBox.Size = new System.Drawing.Size(193, 20);
            this.add_age_textBox.TabIndex = 13;
            // 
            // add_FIO_textBox
            // 
            this.add_FIO_textBox.Location = new System.Drawing.Point(86, 15);
            this.add_FIO_textBox.Name = "add_FIO_textBox";
            this.add_FIO_textBox.Size = new System.Drawing.Size(193, 20);
            this.add_FIO_textBox.TabIndex = 12;
            // 
            // label3
            // 
            this.label3.AutoSize = true;
            this.label3.Location = new System.Drawing.Point(12, 76);
            this.label3.Name = "label3";
            this.label3.Size = new System.Drawing.Size(38, 13);
            this.label3.TabIndex = 11;
            this.label3.Text = "Отдел";
            // 
            // label2
            // 
            this.label2.AutoSize = true;
            this.label2.Location = new System.Drawing.Point(12, 46);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(49, 13);
            this.label2.TabIndex = 10;
            this.label2.Text = "Возраст";
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Location = new System.Drawing.Point(12, 22);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(34, 13);
            this.label1.TabIndex = 9;
            this.label1.Text = "ФИО";
            // 
            // add_aq_textBox
            // 
            this.add_aq_textBox.Location = new System.Drawing.Point(86, 166);
            this.add_aq_textBox.Name = "add_aq_textBox";
            this.add_aq_textBox.Size = new System.Drawing.Size(193, 20);
            this.add_aq_textBox.TabIndex = 20;
            // 
            // label5
            // 
            this.label5.AutoSize = true;
            this.label5.Location = new System.Drawing.Point(12, 166);
            this.label5.Name = "label5";
            this.label5.Size = new System.Drawing.Size(69, 13);
            this.label5.TabIndex = 19;
            this.label5.Text = "Знакомства";
            // 
            // add_children_textBox
            // 
            this.add_children_textBox.Location = new System.Drawing.Point(86, 135);
            this.add_children_textBox.Name = "add_children_textBox";
            this.add_children_textBox.Size = new System.Drawing.Size(193, 20);
            this.add_children_textBox.TabIndex = 18;
            // 
            // label6
            // 
            this.label6.AutoSize = true;
            this.label6.Location = new System.Drawing.Point(12, 135);
            this.label6.Name = "label6";
            this.label6.Size = new System.Drawing.Size(33, 13);
            this.label6.TabIndex = 17;
            this.label6.Text = "Дети";
            // 
            // Form_Add
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(298, 273);
            this.Controls.Add(this.add_aq_textBox);
            this.Controls.Add(this.label5);
            this.Controls.Add(this.add_children_textBox);
            this.Controls.Add(this.label6);
            this.Controls.Add(this.add_sex_textBox);
            this.Controls.Add(this.label4);
            this.Controls.Add(this.add_job_textBox);
            this.Controls.Add(this.add_age_textBox);
            this.Controls.Add(this.add_FIO_textBox);
            this.Controls.Add(this.label3);
            this.Controls.Add(this.label2);
            this.Controls.Add(this.label1);
            this.Controls.Add(this.add_button);
            this.Name = "Form_Add";
            this.StartPosition = System.Windows.Forms.FormStartPosition.CenterScreen;
            this.Text = "Добавление";
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Button add_button;
        private System.Windows.Forms.TextBox add_sex_textBox;
        private System.Windows.Forms.Label label4;
        private System.Windows.Forms.TextBox add_job_textBox;
        private System.Windows.Forms.TextBox add_age_textBox;
        private System.Windows.Forms.TextBox add_FIO_textBox;
        private System.Windows.Forms.Label label3;
        private System.Windows.Forms.Label label2;
        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.TextBox add_aq_textBox;
        private System.Windows.Forms.Label label5;
        private System.Windows.Forms.TextBox add_children_textBox;
        private System.Windows.Forms.Label label6;
    }
}