namespace FoxLab3
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
            this.picHelp = new System.Windows.Forms.PictureBox();
            this.grpDataRequired = new System.Windows.Forms.GroupBox();
            this.btnExit = new System.Windows.Forms.Button();
            this.btnReset = new System.Windows.Forms.Button();
            this.btnCalculate = new System.Windows.Forms.Button();
            this.txtYears = new System.Windows.Forms.TextBox();
            this.txtFutureValue = new System.Windows.Forms.TextBox();
            this.lblRatePerYear = new System.Windows.Forms.Label();
            this.label3 = new System.Windows.Forms.Label();
            this.label2 = new System.Windows.Forms.Label();
            this.label1 = new System.Windows.Forms.Label();
            this.grpSolution = new System.Windows.Forms.GroupBox();
            this.lblMonthPay = new System.Windows.Forms.Label();
            this.label4 = new System.Windows.Forms.Label();
            this.label6 = new System.Windows.Forms.Label();
            ((System.ComponentModel.ISupportInitialize)(this.picHelp)).BeginInit();
            this.grpDataRequired.SuspendLayout();
            this.grpSolution.SuspendLayout();
            this.SuspendLayout();
            // 
            // picHelp
            // 
            this.picHelp.BorderStyle = System.Windows.Forms.BorderStyle.FixedSingle;
            this.picHelp.Image = global::FoxLab3.Properties.Resources.Help;
            this.picHelp.Location = new System.Drawing.Point(233, 0);
            this.picHelp.Name = "picHelp";
            this.picHelp.Size = new System.Drawing.Size(91, 68);
            this.picHelp.SizeMode = System.Windows.Forms.PictureBoxSizeMode.Zoom;
            this.picHelp.TabIndex = 0;
            this.picHelp.TabStop = false;
            this.picHelp.Click += new System.EventHandler(this.picHelp_Click);
            // 
            // grpDataRequired
            // 
            this.grpDataRequired.BackColor = System.Drawing.Color.FromArgb(((int)(((byte)(255)))), ((int)(((byte)(224)))), ((int)(((byte)(192)))));
            this.grpDataRequired.Controls.Add(this.btnExit);
            this.grpDataRequired.Controls.Add(this.btnReset);
            this.grpDataRequired.Controls.Add(this.btnCalculate);
            this.grpDataRequired.Controls.Add(this.txtYears);
            this.grpDataRequired.Controls.Add(this.txtFutureValue);
            this.grpDataRequired.Controls.Add(this.lblRatePerYear);
            this.grpDataRequired.Controls.Add(this.label3);
            this.grpDataRequired.Controls.Add(this.label2);
            this.grpDataRequired.Controls.Add(this.label1);
            this.grpDataRequired.Controls.Add(this.picHelp);
            this.grpDataRequired.Location = new System.Drawing.Point(13, 12);
            this.grpDataRequired.Name = "grpDataRequired";
            this.grpDataRequired.Size = new System.Drawing.Size(325, 177);
            this.grpDataRequired.TabIndex = 1;
            this.grpDataRequired.TabStop = false;
            this.grpDataRequired.Text = "Data Required";
            // 
            // btnExit
            // 
            this.btnExit.Location = new System.Drawing.Point(218, 142);
            this.btnExit.Name = "btnExit";
            this.btnExit.Size = new System.Drawing.Size(90, 29);
            this.btnExit.TabIndex = 4;
            this.btnExit.Text = "E&xit";
            this.btnExit.UseVisualStyleBackColor = true;
            this.btnExit.Click += new System.EventHandler(this.btnExit_Click);
            // 
            // btnReset
            // 
            this.btnReset.DialogResult = System.Windows.Forms.DialogResult.Cancel;
            this.btnReset.Location = new System.Drawing.Point(117, 142);
            this.btnReset.Name = "btnReset";
            this.btnReset.Size = new System.Drawing.Size(90, 29);
            this.btnReset.TabIndex = 3;
            this.btnReset.Text = "&Reset";
            this.btnReset.UseVisualStyleBackColor = true;
            this.btnReset.Click += new System.EventHandler(this.btnReset_Click);
            // 
            // btnCalculate
            // 
            this.btnCalculate.Location = new System.Drawing.Point(16, 142);
            this.btnCalculate.Name = "btnCalculate";
            this.btnCalculate.Size = new System.Drawing.Size(90, 29);
            this.btnCalculate.TabIndex = 2;
            this.btnCalculate.Text = "&Calculate";
            this.btnCalculate.UseVisualStyleBackColor = true;
            this.btnCalculate.Click += new System.EventHandler(this.btnCalculate_Click);
            // 
            // txtYears
            // 
            this.txtYears.Location = new System.Drawing.Point(116, 99);
            this.txtYears.Name = "txtYears";
            this.txtYears.Size = new System.Drawing.Size(100, 33);
            this.txtYears.TabIndex = 1;
            this.txtYears.TextAlign = System.Windows.Forms.HorizontalAlignment.Center;
            // 
            // txtFutureValue
            // 
            this.txtFutureValue.Location = new System.Drawing.Point(116, 27);
            this.txtFutureValue.Name = "txtFutureValue";
            this.txtFutureValue.Size = new System.Drawing.Size(100, 33);
            this.txtFutureValue.TabIndex = 0;
            this.txtFutureValue.TextAlign = System.Windows.Forms.HorizontalAlignment.Center;
            // 
            // lblRatePerYear
            // 
            this.lblRatePerYear.BackColor = System.Drawing.Color.FromArgb(((int)(((byte)(255)))), ((int)(((byte)(192)))), ((int)(((byte)(192)))));
            this.lblRatePerYear.BorderStyle = System.Windows.Forms.BorderStyle.FixedSingle;
            this.lblRatePerYear.Font = new System.Drawing.Font("Arial", 9.75F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.lblRatePerYear.Location = new System.Drawing.Point(116, 63);
            this.lblRatePerYear.Name = "lblRatePerYear";
            this.lblRatePerYear.Size = new System.Drawing.Size(100, 24);
            this.lblRatePerYear.TabIndex = 4;
            this.lblRatePerYear.Text = "3.28";
            this.lblRatePerYear.TextAlign = System.Drawing.ContentAlignment.MiddleCenter;
            // 
            // label3
            // 
            this.label3.AutoSize = true;
            this.label3.Location = new System.Drawing.Point(7, 106);
            this.label3.Name = "label3";
            this.label3.Size = new System.Drawing.Size(72, 25);
            this.label3.TabIndex = 3;
            this.label3.Text = "Years:";
            // 
            // label2
            // 
            this.label2.AutoSize = true;
            this.label2.Location = new System.Drawing.Point(7, 70);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(150, 25);
            this.label2.TabIndex = 2;
            this.label2.Text = "Rate per Year:";
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Location = new System.Drawing.Point(7, 34);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(136, 25);
            this.label1.TabIndex = 1;
            this.label1.Text = "Future Value:";
            // 
            // grpSolution
            // 
            this.grpSolution.BackColor = System.Drawing.Color.LemonChiffon;
            this.grpSolution.Controls.Add(this.lblMonthPay);
            this.grpSolution.Controls.Add(this.label4);
            this.grpSolution.Controls.Add(this.label6);
            this.grpSolution.Location = new System.Drawing.Point(345, 13);
            this.grpSolution.Name = "grpSolution";
            this.grpSolution.Size = new System.Drawing.Size(225, 174);
            this.grpSolution.TabIndex = 2;
            this.grpSolution.TabStop = false;
            this.grpSolution.Text = "Solution";
            this.grpSolution.Visible = false;
            // 
            // lblMonthPay
            // 
            this.lblMonthPay.BackColor = System.Drawing.Color.PaleTurquoise;
            this.lblMonthPay.BorderStyle = System.Windows.Forms.BorderStyle.FixedSingle;
            this.lblMonthPay.Font = new System.Drawing.Font("Arial", 9.75F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.lblMonthPay.Location = new System.Drawing.Point(6, 55);
            this.lblMonthPay.Name = "lblMonthPay";
            this.lblMonthPay.Size = new System.Drawing.Size(213, 24);
            this.lblMonthPay.TabIndex = 5;
            this.lblMonthPay.TextAlign = System.Drawing.ContentAlignment.MiddleCenter;
            // 
            // label4
            // 
            this.label4.Location = new System.Drawing.Point(6, 29);
            this.label4.Name = "label4";
            this.label4.Size = new System.Drawing.Size(202, 33);
            this.label4.TabIndex = 0;
            this.label4.Text = "Monthly Payments;";
            // 
            // label6
            // 
            this.label6.BackColor = System.Drawing.Color.FromArgb(((int)(((byte)(255)))), ((int)(((byte)(192)))), ((int)(((byte)(192)))));
            this.label6.BorderStyle = System.Windows.Forms.BorderStyle.FixedSingle;
            this.label6.Font = new System.Drawing.Font("Arial", 9.75F, System.Drawing.FontStyle.Bold);
            this.label6.Location = new System.Drawing.Point(0, 116);
            this.label6.Name = "label6";
            this.label6.Size = new System.Drawing.Size(225, 52);
            this.label6.TabIndex = 2;
            this.label6.Text = "Payments are totalled.\r\nReset to start over.";
            this.label6.TextAlign = System.Drawing.ContentAlignment.MiddleCenter;
            // 
            // Form1
            // 
            this.AcceptButton = this.btnCalculate;
            this.AutoScaleDimensions = new System.Drawing.SizeF(13F, 25F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.CancelButton = this.btnReset;
            this.ClientSize = new System.Drawing.Size(593, 199);
            this.Controls.Add(this.grpSolution);
            this.Controls.Add(this.grpDataRequired);
            this.Font = new System.Drawing.Font("Arial", 11F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.Margin = new System.Windows.Forms.Padding(4);
            this.Name = "Form1";
            this.StartPosition = System.Windows.Forms.FormStartPosition.CenterScreen;
            this.Text = "Annuity Payment by William Fox";
            ((System.ComponentModel.ISupportInitialize)(this.picHelp)).EndInit();
            this.grpDataRequired.ResumeLayout(false);
            this.grpDataRequired.PerformLayout();
            this.grpSolution.ResumeLayout(false);
            this.ResumeLayout(false);

        }

        #endregion

        private System.Windows.Forms.PictureBox picHelp;
        private System.Windows.Forms.GroupBox grpDataRequired;
        private System.Windows.Forms.Label lblRatePerYear;
        private System.Windows.Forms.Label label3;
        private System.Windows.Forms.Label label2;
        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.TextBox txtYears;
        private System.Windows.Forms.TextBox txtFutureValue;
        private System.Windows.Forms.Button btnExit;
        private System.Windows.Forms.Button btnReset;
        private System.Windows.Forms.Button btnCalculate;
        private System.Windows.Forms.GroupBox grpSolution;
        private System.Windows.Forms.Label label6;
        private System.Windows.Forms.Label label4;
        private System.Windows.Forms.Label lblMonthPay;
    }
}
