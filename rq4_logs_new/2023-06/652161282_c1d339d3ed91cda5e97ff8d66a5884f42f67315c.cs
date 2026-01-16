namespace Recipe_Organizer_PRN211.Feedback
{
	partial class FeedbackForm
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
			label1 = new Label();
			label2 = new Label();
			label3 = new Label();
			label5 = new Label();
			label4 = new Label();
			txtTitle = new TextBox();
			groupBox1 = new GroupBox();
			rbReport = new RadioButton();
			rbComment = new RadioButton();
			rbVUnsatisfied = new RadioButton();
			rbUnsatisfied = new RadioButton();
			rbNeutral = new RadioButton();
			rbSatisfied = new RadioButton();
			rbVSatisfied = new RadioButton();
			groupBox2 = new GroupBox();
			txtDescription = new TextBox();
			btnClear = new Button();
			btnDelete = new Button();
			btnShowAll = new Button();
			btnSubmit = new Button();
			btnExit = new Button();
			groupBox1.SuspendLayout();
			groupBox2.SuspendLayout();
			SuspendLayout();
			// 
			// label1
			// 
			label1.AutoSize = true;
			label1.Font = new Font("Montserrat", 10.2F, FontStyle.Regular, GraphicsUnit.Point);
			label1.Location = new Point(70, 32);
			label1.Name = "label1";
			label1.Size = new Size(55, 24);
			label1.TabIndex = 0;
			label1.Text = "Title: ";
			// 
			// label2
			// 
			label2.AutoSize = true;
			label2.Font = new Font("Montserrat", 10.2F, FontStyle.Regular, GraphicsUnit.Point);
			label2.Location = new Point(70, 88);
			label2.Name = "label2";
			label2.Size = new Size(143, 24);
			label2.TabIndex = 1;
			label2.Text = "Feedback Type:";
			// 
			// label3
			// 
			label3.AutoSize = true;
			label3.Location = new Point(70, 147);
			label3.Name = "label3";
			label3.Size = new Size(0, 20);
			label3.TabIndex = 2;
			// 
			// label5
			// 
			label5.AutoSize = true;
			label5.Font = new Font("Montserrat", 10.2F, FontStyle.Regular, GraphicsUnit.Point);
			label5.Location = new Point(70, 143);
			label5.Name = "label5";
			label5.Size = new Size(335, 24);
			label5.TabIndex = 4;
			label5.Text = "How satisfied are you with our recipe?";
			// 
			// label4
			// 
			label4.AutoSize = true;
			label4.Font = new Font("Montserrat", 10.2F, FontStyle.Regular, GraphicsUnit.Point);
			label4.Location = new Point(70, 227);
			label4.Name = "label4";
			label4.Size = new Size(205, 24);
			label4.TabIndex = 5;
			label4.Text = "How can we improve ?";
			// 
			// txtTitle
			// 
			txtTitle.Location = new Point(150, 29);
			txtTitle.Name = "txtTitle";
			txtTitle.Size = new Size(255, 27);
			txtTitle.TabIndex = 6;
			// 
			// groupBox1
			// 
			groupBox1.Controls.Add(rbReport);
			groupBox1.Controls.Add(rbComment);
			groupBox1.Location = new Point(230, 62);
			groupBox1.Name = "groupBox1";
			groupBox1.Size = new Size(313, 78);
			groupBox1.TabIndex = 7;
			groupBox1.TabStop = false;
			// 
			// rbReport
			// 
			rbReport.AutoSize = true;
			rbReport.Font = new Font("Montserrat", 9F, FontStyle.Regular, GraphicsUnit.Point);
			rbReport.Location = new Point(174, 31);
			rbReport.Name = "rbReport";
			rbReport.Size = new Size(82, 25);
			rbReport.TabIndex = 1;
			rbReport.TabStop = true;
			rbReport.Text = "Report";
			rbReport.UseVisualStyleBackColor = true;
			rbReport.Click += FeedbackType_Click;
			// 
			// rbComment
			// 
			rbComment.AutoSize = true;
			rbComment.Font = new Font("Montserrat", 9F, FontStyle.Regular, GraphicsUnit.Point);
			rbComment.Location = new Point(14, 31);
			rbComment.Name = "rbComment";
			rbComment.Size = new Size(108, 25);
			rbComment.TabIndex = 0;
			rbComment.TabStop = true;
			rbComment.Text = "Comment";
			rbComment.UseVisualStyleBackColor = true;
			rbComment.Click += FeedbackType_Click;
			// 
			// rbVUnsatisfied
			// 
			rbVUnsatisfied.AutoSize = true;
			rbVUnsatisfied.Font = new Font("Montserrat", 9F, FontStyle.Regular, GraphicsUnit.Point);
			rbVUnsatisfied.Location = new Point(517, 19);
			rbVUnsatisfied.Name = "rbVUnsatisfied";
			rbVUnsatisfied.Size = new Size(149, 25);
			rbVUnsatisfied.TabIndex = 2;
			rbVUnsatisfied.TabStop = true;
			rbVUnsatisfied.Text = "Very unsatisfied";
			rbVUnsatisfied.UseVisualStyleBackColor = true;
			rbVUnsatisfied.Click += FeedbackLevel_Click;
			// 
			// rbUnsatisfied
			// 
			rbUnsatisfied.AutoSize = true;
			rbUnsatisfied.Font = new Font("Montserrat", 9F, FontStyle.Regular, GraphicsUnit.Point);
			rbUnsatisfied.Location = new Point(385, 19);
			rbUnsatisfied.Name = "rbUnsatisfied";
			rbUnsatisfied.Size = new Size(115, 25);
			rbUnsatisfied.TabIndex = 3;
			rbUnsatisfied.TabStop = true;
			rbUnsatisfied.Text = "Unsatisfied";
			rbUnsatisfied.UseVisualStyleBackColor = true;
			rbUnsatisfied.Click += FeedbackLevel_Click;
			// 
			// rbNeutral
			// 
			rbNeutral.AutoSize = true;
			rbNeutral.Font = new Font("Montserrat", 9F, FontStyle.Regular, GraphicsUnit.Point);
			rbNeutral.Location = new Point(280, 19);
			rbNeutral.Name = "rbNeutral";
			rbNeutral.Size = new Size(87, 25);
			rbNeutral.TabIndex = 4;
			rbNeutral.TabStop = true;
			rbNeutral.Text = "Neutral";
			rbNeutral.UseVisualStyleBackColor = true;
			rbNeutral.Click += FeedbackLevel_Click;
			// 
			// rbSatisfied
			// 
			rbSatisfied.AutoSize = true;
			rbSatisfied.Font = new Font("Montserrat", 9F, FontStyle.Regular, GraphicsUnit.Point);
			rbSatisfied.Location = new Point(168, 19);
			rbSatisfied.Name = "rbSatisfied";
			rbSatisfied.Size = new Size(95, 25);
			rbSatisfied.TabIndex = 5;
			rbSatisfied.TabStop = true;
			rbSatisfied.Text = "Satisfied";
			rbSatisfied.UseVisualStyleBackColor = true;
			rbSatisfied.Click += FeedbackLevel_Click;
			// 
			// rbVSatisfied
			// 
			rbVSatisfied.AutoSize = true;
			rbVSatisfied.Font = new Font("Montserrat", 9F, FontStyle.Regular, GraphicsUnit.Point);
			rbVSatisfied.Location = new Point(19, 19);
			rbVSatisfied.Name = "rbVSatisfied";
			rbVSatisfied.Size = new Size(133, 25);
			rbVSatisfied.TabIndex = 6;
			rbVSatisfied.TabStop = true;
			rbVSatisfied.Text = " Very satisfied";
			rbVSatisfied.UseVisualStyleBackColor = true;
			rbVSatisfied.Click += FeedbackLevel_Click;
			// 
			// groupBox2
			// 
			groupBox2.Controls.Add(rbVUnsatisfied);
			groupBox2.Controls.Add(rbVSatisfied);
			groupBox2.Controls.Add(rbUnsatisfied);
			groupBox2.Controls.Add(rbSatisfied);
			groupBox2.Controls.Add(rbNeutral);
			groupBox2.Location = new Point(70, 165);
			groupBox2.Name = "groupBox2";
			groupBox2.Size = new Size(687, 59);
			groupBox2.TabIndex = 8;
			groupBox2.TabStop = false;
			// 
			// txtDescription
			// 
			txtDescription.Font = new Font("Montserrat", 9F, FontStyle.Regular, GraphicsUnit.Point);
			txtDescription.ForeColor = Color.Silver;
			txtDescription.Location = new Point(70, 263);
			txtDescription.Multiline = true;
			txtDescription.Name = "txtDescription";
			txtDescription.Size = new Size(687, 140);
			txtDescription.TabIndex = 9;
			txtDescription.Text = "Enter your Feedback...\r\n";
			// 
			// btnClear
			// 
			btnClear.Font = new Font("Montserrat", 9F, FontStyle.Regular, GraphicsUnit.Point);
			btnClear.Location = new Point(679, 263);
			btnClear.Name = "btnClear";
			btnClear.Size = new Size(78, 29);
			btnClear.TabIndex = 10;
			btnClear.Text = "Clear";
			btnClear.UseVisualStyleBackColor = true;
			btnClear.Click += btnClear_Click;
			// 
			// btnDelete
			// 
			btnDelete.Font = new Font("Montserrat", 9F, FontStyle.Regular, GraphicsUnit.Point);
			btnDelete.Location = new Point(595, 263);
			btnDelete.Name = "btnDelete";
			btnDelete.Size = new Size(78, 29);
			btnDelete.TabIndex = 11;
			btnDelete.Text = "Delete";
			btnDelete.UseVisualStyleBackColor = true;
			btnDelete.Click += btnDelete_Click;
			// 
			// btnShowAll
			// 
			btnShowAll.Font = new Font("Montserrat", 9F, FontStyle.Regular, GraphicsUnit.Point);
			btnShowAll.Location = new Point(515, 409);
			btnShowAll.Name = "btnShowAll";
			btnShowAll.Size = new Size(88, 29);
			btnShowAll.TabIndex = 12;
			btnShowAll.Text = "Show All";
			btnShowAll.UseVisualStyleBackColor = true;
			btnShowAll.Click += btnShowAll_Click;
			// 
			// btnSubmit
			// 
			btnSubmit.Font = new Font("Montserrat", 9F, FontStyle.Regular, GraphicsUnit.Point);
			btnSubmit.Location = new Point(609, 409);
			btnSubmit.Name = "btnSubmit";
			btnSubmit.Size = new Size(78, 29);
			btnSubmit.TabIndex = 13;
			btnSubmit.Text = "Submit";
			btnSubmit.UseVisualStyleBackColor = true;
			btnSubmit.Click += btnSubmit_Click;
			// 
			// btnExit
			// 
			btnExit.Font = new Font("Montserrat", 9F, FontStyle.Regular, GraphicsUnit.Point);
			btnExit.Location = new Point(693, 409);
			btnExit.Name = "btnExit";
			btnExit.Size = new Size(78, 29);
			btnExit.TabIndex = 14;
			btnExit.Text = "Exit";
			btnExit.UseVisualStyleBackColor = true;
			btnExit.Click += btnExit_Click;
			// 
			// FeedbackForm
			// 
			AutoScaleDimensions = new SizeF(8F, 20F);
			AutoScaleMode = AutoScaleMode.Font;
			ClientSize = new Size(800, 450);
			Controls.Add(btnExit);
			Controls.Add(btnSubmit);
			Controls.Add(btnShowAll);
			Controls.Add(btnDelete);
			Controls.Add(btnClear);
			Controls.Add(txtDescription);
			Controls.Add(groupBox1);
			Controls.Add(txtTitle);
			Controls.Add(label4);
			Controls.Add(label5);
			Controls.Add(label3);
			Controls.Add(label2);
			Controls.Add(label1);
			Controls.Add(groupBox2);
			Name = "FeedbackForm";
			Text = "FeedbackForm";
			Click += FeedbackType_Click;
			groupBox1.ResumeLayout(false);
			groupBox1.PerformLayout();
			groupBox2.ResumeLayout(false);
			groupBox2.PerformLayout();
			ResumeLayout(false);
			PerformLayout();
		}

		#endregion

		private Label label1;
		private Label label2;
		private Label label3;
		private Label label5;
		private Label label4;
		private TextBox txtTitle;
		private GroupBox groupBox1;
		private RadioButton rbReport;
		private RadioButton rbComment;
		private RadioButton rbVUnsatisfied;
		private RadioButton rbUnsatisfied;
		private RadioButton rbNeutral;
		private RadioButton rbSatisfied;
		private RadioButton rbVSatisfied;
		private GroupBox groupBox2;
		private TextBox txtDescription;
		private Button btnClear;
		private Button btnDelete;
		private Button btnShowAll;
		private Button btnSubmit;
		private Button btnExit;
	}
}