namespace Clock2
{
    partial class ConfigWindow
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
            this.label2 = new System.Windows.Forms.Label();
            this.label3 = new System.Windows.Forms.Label();
            this.timeBox = new System.Windows.Forms.TextBox();
            this.locationBox = new System.Windows.Forms.TextBox();
            this.codeBox = new System.Windows.Forms.TextBox();
            this.submitBtn = new System.Windows.Forms.Button();
            this.label4 = new System.Windows.Forms.Label();
            this.label5 = new System.Windows.Forms.Label();
            this.holNameBox = new System.Windows.Forms.TextBox();
            this.holDateBox = new System.Windows.Forms.TextBox();
            this.dataBtn = new System.Windows.Forms.Button();
            this.SuspendLayout();
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Location = new System.Drawing.Point(24, 30);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(162, 26);
            this.label1.TabIndex = 0;
            this.label1.Text = "End of day time in 24 hour format\r\nExample 13:00";
            // 
            // label2
            // 
            this.label2.AutoSize = true;
            this.label2.Location = new System.Drawing.Point(24, 70);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(48, 13);
            this.label2.TabIndex = 1;
            this.label2.Text = "Location";
            // 
            // label3
            // 
            this.label3.AutoSize = true;
            this.label3.Location = new System.Drawing.Point(24, 110);
            this.label3.Name = "label3";
            this.label3.Size = new System.Drawing.Size(124, 13);
            this.label3.TabIndex = 2;
            this.label3.Text = "Open Weather API code";
            // 
            // timeBox
            // 
            this.timeBox.Location = new System.Drawing.Point(190, 27);
            this.timeBox.Name = "timeBox";
            this.timeBox.Size = new System.Drawing.Size(100, 20);
            this.timeBox.TabIndex = 3;
            // 
            // locationBox
            // 
            this.locationBox.Location = new System.Drawing.Point(190, 67);
            this.locationBox.Name = "locationBox";
            this.locationBox.Size = new System.Drawing.Size(100, 20);
            this.locationBox.TabIndex = 4;
            // 
            // codeBox
            // 
            this.codeBox.Location = new System.Drawing.Point(190, 107);
            this.codeBox.Name = "codeBox";
            this.codeBox.Size = new System.Drawing.Size(100, 20);
            this.codeBox.TabIndex = 5;
            // 
            // submitBtn
            // 
            this.submitBtn.Location = new System.Drawing.Point(117, 154);
            this.submitBtn.Name = "submitBtn";
            this.submitBtn.Size = new System.Drawing.Size(75, 23);
            this.submitBtn.TabIndex = 6;
            this.submitBtn.Text = "Submit";
            this.submitBtn.UseVisualStyleBackColor = true;
            this.submitBtn.Click += new System.EventHandler(this.submitBtn_Click);
            // 
            // label4
            // 
            this.label4.AutoSize = true;
            this.label4.Location = new System.Drawing.Point(24, 200);
            this.label4.Name = "label4";
            this.label4.Size = new System.Drawing.Size(73, 13);
            this.label4.TabIndex = 7;
            this.label4.Text = "Holiday Name";
            // 
            // label5
            // 
            this.label5.AutoSize = true;
            this.label5.Location = new System.Drawing.Point(24, 240);
            this.label5.Name = "label5";
            this.label5.Size = new System.Drawing.Size(118, 26);
            this.label5.TabIndex = 8;
            this.label5.Text = "Holiday Date\r\nExample YYYY-MM-DD";
            // 
            // holNameBox
            // 
            this.holNameBox.Location = new System.Drawing.Point(190, 197);
            this.holNameBox.Name = "holNameBox";
            this.holNameBox.Size = new System.Drawing.Size(100, 20);
            this.holNameBox.TabIndex = 9;
            // 
            // holDateBox
            // 
            this.holDateBox.Location = new System.Drawing.Point(190, 237);
            this.holDateBox.Name = "holDateBox";
            this.holDateBox.Size = new System.Drawing.Size(100, 20);
            this.holDateBox.TabIndex = 10;
            // 
            // dataBtn
            // 
            this.dataBtn.Location = new System.Drawing.Point(117, 278);
            this.dataBtn.Name = "dataBtn";
            this.dataBtn.Size = new System.Drawing.Size(75, 23);
            this.dataBtn.TabIndex = 11;
            this.dataBtn.Text = "Submit";
            this.dataBtn.UseVisualStyleBackColor = true;
            this.dataBtn.Click += new System.EventHandler(this.dataBtn_Click);
            // 
            // ConfigWindow
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(302, 323);
            this.Controls.Add(this.dataBtn);
            this.Controls.Add(this.holDateBox);
            this.Controls.Add(this.holNameBox);
            this.Controls.Add(this.label5);
            this.Controls.Add(this.label4);
            this.Controls.Add(this.submitBtn);
            this.Controls.Add(this.codeBox);
            this.Controls.Add(this.locationBox);
            this.Controls.Add(this.timeBox);
            this.Controls.Add(this.label3);
            this.Controls.Add(this.label2);
            this.Controls.Add(this.label1);
            this.Name = "ConfigWindow";
            this.Text = "ConfigWindow";
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.Label label2;
        private System.Windows.Forms.Label label3;
        private System.Windows.Forms.TextBox timeBox;
        private System.Windows.Forms.TextBox locationBox;
        private System.Windows.Forms.TextBox codeBox;
        private System.Windows.Forms.Button submitBtn;
        private System.Windows.Forms.Label label4;
        private System.Windows.Forms.Label label5;
        private System.Windows.Forms.TextBox holNameBox;
        private System.Windows.Forms.TextBox holDateBox;
        private System.Windows.Forms.Button dataBtn;
    }
}