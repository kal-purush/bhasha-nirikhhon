namespace TwoNumbersAppWeek2
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
            this.txtFirstNum = new System.Windows.Forms.TextBox();
            this.txtSecondNum = new System.Windows.Forms.TextBox();
            this.lblSecondNum = new System.Windows.Forms.Label();
            this.lblFirstNum = new System.Windows.Forms.Label();
            this.btnAddNumbers = new System.Windows.Forms.Button();
            this.SuspendLayout();
            // 
            // txtFirstNum
            // 
            this.txtFirstNum.Location = new System.Drawing.Point(164, 74);
            this.txtFirstNum.Name = "txtFirstNum";
            this.txtFirstNum.Size = new System.Drawing.Size(100, 22);
            this.txtFirstNum.TabIndex = 0;
            // 
            // txtSecondNum
            // 
            this.txtSecondNum.Location = new System.Drawing.Point(164, 125);
            this.txtSecondNum.Name = "txtSecondNum";
            this.txtSecondNum.Size = new System.Drawing.Size(100, 22);
            this.txtSecondNum.TabIndex = 1;
            // 
            // lblSecondNum
            // 
            this.lblSecondNum.AutoSize = true;
            this.lblSecondNum.Location = new System.Drawing.Point(164, 106);
            this.lblSecondNum.Name = "lblSecondNum";
            this.lblSecondNum.Size = new System.Drawing.Size(108, 16);
            this.lblSecondNum.TabIndex = 2;
            this.lblSecondNum.Text = "Second Number:";
            // 
            // lblFirstNum
            // 
            this.lblFirstNum.AutoSize = true;
            this.lblFirstNum.Location = new System.Drawing.Point(164, 55);
            this.lblFirstNum.Name = "lblFirstNum";
            this.lblFirstNum.Size = new System.Drawing.Size(83, 16);
            this.lblFirstNum.TabIndex = 3;
            this.lblFirstNum.Text = "First Number";
            // 
            // btnAddNumbers
            // 
            this.btnAddNumbers.Location = new System.Drawing.Point(171, 172);
            this.btnAddNumbers.Name = "btnAddNumbers";
            this.btnAddNumbers.Size = new System.Drawing.Size(75, 23);
            this.btnAddNumbers.TabIndex = 4;
            this.btnAddNumbers.Text = "Add Numbers";
            this.btnAddNumbers.UseVisualStyleBackColor = true;
            this.btnAddNumbers.Click += new System.EventHandler(this.btnAddNumbers_Click);
            // 
            // Form1
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(8F, 16F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(800, 450);
            this.Controls.Add(this.btnAddNumbers);
            this.Controls.Add(this.lblFirstNum);
            this.Controls.Add(this.lblSecondNum);
            this.Controls.Add(this.txtSecondNum);
            this.Controls.Add(this.txtFirstNum);
            this.Name = "Form1";
            this.Text = "Form1";
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.TextBox txtFirstNum;
        private System.Windows.Forms.TextBox txtSecondNum;
        private System.Windows.Forms.Label lblSecondNum;
        private System.Windows.Forms.Label lblFirstNum;
        private System.Windows.Forms.Button btnAddNumbers;
    }
}
