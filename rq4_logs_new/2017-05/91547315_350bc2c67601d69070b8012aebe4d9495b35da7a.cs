namespace SimpleGuiCalculator
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
			this.label1 = new System.Windows.Forms.Label();
			this.label2 = new System.Windows.Forms.Label();
			this.label3 = new System.Windows.Forms.Label();
			this.txtNum1 = new System.Windows.Forms.TextBox();
			this.txtNum2 = new System.Windows.Forms.TextBox();
			this.btnAdd = new System.Windows.Forms.Button();
			this.btnSub = new System.Windows.Forms.Button();
			this.btnMult = new System.Windows.Forms.Button();
			this.btnDiv = new System.Windows.Forms.Button();
			this.label4 = new System.Windows.Forms.Label();
			this.lblAnswer = new System.Windows.Forms.Label();
			this.btnClear = new System.Windows.Forms.Button();
			this.SuspendLayout();
			// 
			// label1
			// 
			this.label1.AutoSize = true;
			this.label1.Font = new System.Drawing.Font("Lucida Fax", 18F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
			this.label1.Location = new System.Drawing.Point(99, 9);
			this.label1.Name = "label1";
			this.label1.Size = new System.Drawing.Size(233, 27);
			this.label1.TabIndex = 0;
			this.label1.Text = "Simple Calculator!";
			// 
			// label2
			// 
			this.label2.AutoSize = true;
			this.label2.Location = new System.Drawing.Point(17, 73);
			this.label2.Name = "label2";
			this.label2.Size = new System.Drawing.Size(77, 13);
			this.label2.TabIndex = 1;
			this.label2.Text = "Enter first num:";
			// 
			// label3
			// 
			this.label3.AutoSize = true;
			this.label3.Location = new System.Drawing.Point(17, 115);
			this.label3.Name = "label3";
			this.label3.Size = new System.Drawing.Size(96, 13);
			this.label3.TabIndex = 2;
			this.label3.Text = "Enter second num:";
			// 
			// txtNum1
			// 
			this.txtNum1.Location = new System.Drawing.Point(119, 70);
			this.txtNum1.Name = "txtNum1";
			this.txtNum1.Size = new System.Drawing.Size(100, 20);
			this.txtNum1.TabIndex = 3;
			// 
			// txtNum2
			// 
			this.txtNum2.Location = new System.Drawing.Point(119, 112);
			this.txtNum2.Name = "txtNum2";
			this.txtNum2.Size = new System.Drawing.Size(100, 20);
			this.txtNum2.TabIndex = 4;
			// 
			// btnAdd
			// 
			this.btnAdd.Location = new System.Drawing.Point(255, 59);
			this.btnAdd.Name = "btnAdd";
			this.btnAdd.Size = new System.Drawing.Size(68, 41);
			this.btnAdd.TabIndex = 5;
			this.btnAdd.Text = "+";
			this.btnAdd.UseVisualStyleBackColor = true;
			this.btnAdd.Click += new System.EventHandler(this.btnAdd_Click);
			// 
			// btnSub
			// 
			this.btnSub.Location = new System.Drawing.Point(357, 59);
			this.btnSub.Name = "btnSub";
			this.btnSub.Size = new System.Drawing.Size(68, 41);
			this.btnSub.TabIndex = 6;
			this.btnSub.Text = "-";
			this.btnSub.UseVisualStyleBackColor = true;
			this.btnSub.Click += new System.EventHandler(this.btnSub_Click);
			// 
			// btnMult
			// 
			this.btnMult.Location = new System.Drawing.Point(255, 125);
			this.btnMult.Name = "btnMult";
			this.btnMult.Size = new System.Drawing.Size(68, 41);
			this.btnMult.TabIndex = 7;
			this.btnMult.Text = "*";
			this.btnMult.UseVisualStyleBackColor = true;
			this.btnMult.Click += new System.EventHandler(this.btnMult_Click);
			// 
			// btnDiv
			// 
			this.btnDiv.Location = new System.Drawing.Point(357, 125);
			this.btnDiv.Name = "btnDiv";
			this.btnDiv.Size = new System.Drawing.Size(68, 41);
			this.btnDiv.TabIndex = 8;
			this.btnDiv.Text = "/";
			this.btnDiv.UseVisualStyleBackColor = true;
			this.btnDiv.Click += new System.EventHandler(this.btnDiv_Click);
			// 
			// label4
			// 
			this.label4.AutoSize = true;
			this.label4.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
			this.label4.Location = new System.Drawing.Point(37, 276);
			this.label4.Name = "label4";
			this.label4.Size = new System.Drawing.Size(110, 20);
			this.label4.TabIndex = 9;
			this.label4.Text = "The answer is:";
			// 
			// lblAnswer
			// 
			this.lblAnswer.BorderStyle = System.Windows.Forms.BorderStyle.Fixed3D;
			this.lblAnswer.Location = new System.Drawing.Point(153, 276);
			this.lblAnswer.Name = "lblAnswer";
			this.lblAnswer.Size = new System.Drawing.Size(100, 23);
			this.lblAnswer.TabIndex = 12;
			// 
			// btnClear
			// 
			this.btnClear.Location = new System.Drawing.Point(304, 172);
			this.btnClear.Name = "btnClear";
			this.btnClear.Size = new System.Drawing.Size(68, 41);
			this.btnClear.TabIndex = 13;
			this.btnClear.Text = "Clear";
			this.btnClear.UseVisualStyleBackColor = true;
			this.btnClear.Click += new System.EventHandler(this.btnClear_Click);
			// 
			// Form1
			// 
			this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
			this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
			this.BackColor = System.Drawing.SystemColors.ActiveBorder;
			this.ClientSize = new System.Drawing.Size(457, 373);
			this.Controls.Add(this.btnClear);
			this.Controls.Add(this.lblAnswer);
			this.Controls.Add(this.label4);
			this.Controls.Add(this.btnDiv);
			this.Controls.Add(this.btnMult);
			this.Controls.Add(this.btnSub);
			this.Controls.Add(this.btnAdd);
			this.Controls.Add(this.txtNum2);
			this.Controls.Add(this.txtNum1);
			this.Controls.Add(this.label3);
			this.Controls.Add(this.label2);
			this.Controls.Add(this.label1);
			this.Name = "Form1";
			this.Text = "Simple Gui Calculator";
			this.ResumeLayout(false);
			this.PerformLayout();

		}

		#endregion

		private System.Windows.Forms.Label label1;
		private System.Windows.Forms.Label label2;
		private System.Windows.Forms.Label label3;
		private System.Windows.Forms.TextBox txtNum1;
		private System.Windows.Forms.TextBox txtNum2;
		private System.Windows.Forms.Button btnAdd;
		private System.Windows.Forms.Button btnSub;
		private System.Windows.Forms.Button btnMult;
		private System.Windows.Forms.Button btnDiv;
		private System.Windows.Forms.Label label4;
		private System.Windows.Forms.Label lblAnswer;
		private System.Windows.Forms.Button btnClear;
	}
}
