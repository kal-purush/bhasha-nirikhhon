namespace QLSV {
	partial class SetCourseRegistrationPeriod {
		/// <summary>
		/// Required designer variable.
		/// </summary>
		private System.ComponentModel.IContainer components = null;

		/// <summary>
		/// Clean up any resources being used.
		/// </summary>
		/// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
		protected override void Dispose(bool disposing) {
			if (disposing && (components != null)) {
				components.Dispose();
			}
			base.Dispose(disposing);
		}

		#region Windows Form Designer generated code

		/// <summary>
		/// Required method for Designer support - do not modify
		/// the contents of this method with the code editor.
		/// </summary>
		private void InitializeComponent() {
			System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(SetCourseRegistrationPeriod));
			this.dtp_startDate = new System.Windows.Forms.DateTimePicker();
			this.label1 = new System.Windows.Forms.Label();
			this.label2 = new System.Windows.Forms.Label();
			this.dtp_endDate = new System.Windows.Forms.DateTimePicker();
			this.btn_setPeriod = new System.Windows.Forms.Button();
			this.SuspendLayout();
			// 
			// dtp_startDate
			// 
			this.dtp_startDate.CustomFormat = "dd/MM/yyyy";
			this.dtp_startDate.Font = new System.Drawing.Font("Cambria", 16.2F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
			this.dtp_startDate.Format = System.Windows.Forms.DateTimePickerFormat.Custom;
			this.dtp_startDate.Location = new System.Drawing.Point(349, 54);
			this.dtp_startDate.Name = "dtp_startDate";
			this.dtp_startDate.Size = new System.Drawing.Size(200, 39);
			this.dtp_startDate.TabIndex = 0;
			// 
			// label1
			// 
			this.label1.AutoSize = true;
			this.label1.Font = new System.Drawing.Font("Cambria", 16.2F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
			this.label1.Location = new System.Drawing.Point(54, 54);
			this.label1.Name = "label1";
			this.label1.Size = new System.Drawing.Size(232, 33);
			this.label1.TabIndex = 1;
			this.label1.Text = "Thời gian bắt đầu:";
			// 
			// label2
			// 
			this.label2.AutoSize = true;
			this.label2.Font = new System.Drawing.Font("Cambria", 16.2F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
			this.label2.Location = new System.Drawing.Point(54, 135);
			this.label2.Name = "label2";
			this.label2.Size = new System.Drawing.Size(238, 33);
			this.label2.TabIndex = 3;
			this.label2.Text = "Thời gian kết thúc:";
			// 
			// dtp_endDate
			// 
			this.dtp_endDate.CustomFormat = "dd/MM/yyyy";
			this.dtp_endDate.Font = new System.Drawing.Font("Cambria", 16.2F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
			this.dtp_endDate.Format = System.Windows.Forms.DateTimePickerFormat.Custom;
			this.dtp_endDate.Location = new System.Drawing.Point(349, 130);
			this.dtp_endDate.Name = "dtp_endDate";
			this.dtp_endDate.Size = new System.Drawing.Size(200, 39);
			this.dtp_endDate.TabIndex = 2;
			// 
			// btn_setPeriod
			// 
			this.btn_setPeriod.Font = new System.Drawing.Font("Cambria", 16.2F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
			this.btn_setPeriod.Location = new System.Drawing.Point(229, 215);
			this.btn_setPeriod.Name = "btn_setPeriod";
			this.btn_setPeriod.Size = new System.Drawing.Size(183, 59);
			this.btn_setPeriod.TabIndex = 4;
			this.btn_setPeriod.Text = "Thiết lập";
			this.btn_setPeriod.UseVisualStyleBackColor = true;
			this.btn_setPeriod.Click += new System.EventHandler(this.btn_setPeriod_Click);
			// 
			// SetCourseRegistrationPeriod
			// 
			this.AutoScaleDimensions = new System.Drawing.SizeF(8F, 16F);
			this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
			this.ClientSize = new System.Drawing.Size(640, 311);
			this.Controls.Add(this.btn_setPeriod);
			this.Controls.Add(this.label2);
			this.Controls.Add(this.dtp_endDate);
			this.Controls.Add(this.label1);
			this.Controls.Add(this.dtp_startDate);
			this.FormBorderStyle = System.Windows.Forms.FormBorderStyle.FixedSingle;
			this.Icon = ((System.Drawing.Icon)(resources.GetObject("$this.Icon")));
			this.MaximizeBox = false;
			this.Name = "SetCourseRegistrationPeriod";
			this.StartPosition = System.Windows.Forms.FormStartPosition.CenterScreen;
			this.Text = "Thiết lập thời gian ĐKHP";
			this.ResumeLayout(false);
			this.PerformLayout();

		}

		#endregion

		private System.Windows.Forms.DateTimePicker dtp_startDate;
		private System.Windows.Forms.Label label1;
		private System.Windows.Forms.Label label2;
		private System.Windows.Forms.DateTimePicker dtp_endDate;
		private System.Windows.Forms.Button btn_setPeriod;
	}
}