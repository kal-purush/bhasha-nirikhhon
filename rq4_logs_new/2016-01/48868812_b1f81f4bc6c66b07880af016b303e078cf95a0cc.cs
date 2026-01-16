namespace RC210_DataAssistant_V2
{
	partial class BetaForm
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
			this.components = new System.ComponentModel.Container();
			this.label1 = new System.Windows.Forms.Label();
			this.buttonOK = new System.Windows.Forms.Button();
			this.buttonSupportGroup = new System.Windows.Forms.Button();
			this.label2 = new System.Windows.Forms.Label();
			this.label3 = new System.Windows.Forms.Label();
			this.timer1 = new System.Windows.Forms.Timer(this.components);
			this.buttonAbout = new System.Windows.Forms.Button();
			this.label4 = new System.Windows.Forms.Label();
			this.SuspendLayout();
			// 
			// label1
			// 
			this.label1.Font = new System.Drawing.Font("Microsoft Sans Serif", 14.25F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
			this.label1.Location = new System.Drawing.Point(12, 9);
			this.label1.Name = "label1";
			this.label1.Size = new System.Drawing.Size(448, 23);
			this.label1.TabIndex = 0;
			this.label1.Text = "RC210 Data Assistant V2";
			this.label1.TextAlign = System.Drawing.ContentAlignment.TopCenter;
			// 
			// buttonOK
			// 
			this.buttonOK.DialogResult = System.Windows.Forms.DialogResult.OK;
			this.buttonOK.Enabled = false;
			this.buttonOK.Location = new System.Drawing.Point(385, 171);
			this.buttonOK.Name = "buttonOK";
			this.buttonOK.Size = new System.Drawing.Size(75, 23);
			this.buttonOK.TabIndex = 1;
			this.buttonOK.Text = "OK";
			this.buttonOK.UseVisualStyleBackColor = true;
			// 
			// buttonSupportGroup
			// 
			this.buttonSupportGroup.Location = new System.Drawing.Point(12, 171);
			this.buttonSupportGroup.Name = "buttonSupportGroup";
			this.buttonSupportGroup.Size = new System.Drawing.Size(109, 23);
			this.buttonSupportGroup.TabIndex = 2;
			this.buttonSupportGroup.Text = "Support Group";
			this.buttonSupportGroup.UseVisualStyleBackColor = true;
			this.buttonSupportGroup.Click += new System.EventHandler(this.buttonSupportGroup_Click);
			// 
			// label2
			// 
			this.label2.AutoSize = true;
			this.label2.Location = new System.Drawing.Point(9, 52);
			this.label2.Name = "label2";
			this.label2.Size = new System.Drawing.Size(445, 13);
			this.label2.TabIndex = 3;
			this.label2.Text = "This software is a beta version only, please report problems or questions to the " +
    "support group.";
			// 
			// label3
			// 
			this.label3.AutoSize = true;
			this.label3.Location = new System.Drawing.Point(12, 94);
			this.label3.Name = "label3";
			this.label3.Size = new System.Drawing.Size(338, 13);
			this.label3.TabIndex = 4;
			this.label3.Text = "The final version of this software will not include this annoying window.";
			// 
			// timer1
			// 
			this.timer1.Interval = 1000;
			this.timer1.Tick += new System.EventHandler(this.timer1_Tick);
			// 
			// buttonAbout
			// 
			this.buttonAbout.Location = new System.Drawing.Point(197, 171);
			this.buttonAbout.Name = "buttonAbout";
			this.buttonAbout.Size = new System.Drawing.Size(75, 23);
			this.buttonAbout.TabIndex = 5;
			this.buttonAbout.Text = "About";
			this.buttonAbout.UseVisualStyleBackColor = true;
			this.buttonAbout.Click += new System.EventHandler(this.buttonAbout_Click);
			// 
			// label4
			// 
			this.label4.Location = new System.Drawing.Point(13, 117);
			this.label4.Name = "label4";
			this.label4.Size = new System.Drawing.Size(295, 40);
			this.label4.TabIndex = 6;
			this.label4.Text = "After 30 seconds you will be able to click OK and continue. This is to discourage" +
    " use of this beta version after its intended test cycle.";
			this.label4.DoubleClick += new System.EventHandler(this.label4_DoubleClick);
			// 
			// BetaForm
			// 
			this.AcceptButton = this.buttonOK;
			this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
			this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
			this.ClientSize = new System.Drawing.Size(472, 206);
			this.ControlBox = false;
			this.Controls.Add(this.label4);
			this.Controls.Add(this.buttonAbout);
			this.Controls.Add(this.label3);
			this.Controls.Add(this.label2);
			this.Controls.Add(this.buttonSupportGroup);
			this.Controls.Add(this.buttonOK);
			this.Controls.Add(this.label1);
			this.FormBorderStyle = System.Windows.Forms.FormBorderStyle.None;
			this.Name = "BetaForm";
			this.StartPosition = System.Windows.Forms.FormStartPosition.CenterScreen;
			this.Text = "BetaForm";
			this.Load += new System.EventHandler(this.BetaForm_Load);
			this.ResumeLayout(false);
			this.PerformLayout();

		}

		#endregion

		private System.Windows.Forms.Label label1;
		private System.Windows.Forms.Button buttonOK;
		private System.Windows.Forms.Button buttonSupportGroup;
		private System.Windows.Forms.Label label2;
		private System.Windows.Forms.Label label3;
		private System.Windows.Forms.Timer timer1;
		private System.Windows.Forms.Button buttonAbout;
		private System.Windows.Forms.Label label4;
	}
}