namespace QLSV {
	partial class RegistrationResult {
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
			this.lb_title = new System.Windows.Forms.Label();
			this.lb_successNum = new System.Windows.Forms.Label();
			this.lb_errorNum = new System.Windows.Forms.Label();
			this.lb_successList = new System.Windows.Forms.Label();
			this.lb_errorList = new System.Windows.Forms.Label();
			this.pb_close = new System.Windows.Forms.PictureBox();
			this.pb_success = new System.Windows.Forms.PictureBox();
			this.pb_error = new System.Windows.Forms.PictureBox();
			((System.ComponentModel.ISupportInitialize)(this.pb_close)).BeginInit();
			((System.ComponentModel.ISupportInitialize)(this.pb_success)).BeginInit();
			((System.ComponentModel.ISupportInitialize)(this.pb_error)).BeginInit();
			this.SuspendLayout();
			// 
			// lb_title
			// 
			this.lb_title.AutoSize = true;
			this.lb_title.Font = new System.Drawing.Font("Cambria", 19.8F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
			this.lb_title.Location = new System.Drawing.Point(131, 25);
			this.lb_title.Name = "lb_title";
			this.lb_title.Size = new System.Drawing.Size(304, 40);
			this.lb_title.TabIndex = 2;
			this.lb_title.Text = "KẾT QUẢ ĐĂNG KÝ";
			// 
			// lb_successNum
			// 
			this.lb_successNum.AutoSize = true;
			this.lb_successNum.Font = new System.Drawing.Font("Cambria", 12F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
			this.lb_successNum.Location = new System.Drawing.Point(97, 118);
			this.lb_successNum.Name = "lb_successNum";
			this.lb_successNum.Size = new System.Drawing.Size(159, 23);
			this.lb_successNum.TabIndex = 3;
			this.lb_successNum.Text = "Lớp Thành Công";
			// 
			// lb_errorNum
			// 
			this.lb_errorNum.AutoSize = true;
			this.lb_errorNum.Font = new System.Drawing.Font("Cambria", 12F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
			this.lb_errorNum.Location = new System.Drawing.Point(91, 241);
			this.lb_errorNum.Name = "lb_errorNum";
			this.lb_errorNum.Size = new System.Drawing.Size(102, 23);
			this.lb_errorNum.TabIndex = 4;
			this.lb_errorNum.Text = "Lớp Bị Lỗi";
			// 
			// lb_successList
			// 
			this.lb_successList.AutoSize = true;
			this.lb_successList.Font = new System.Drawing.Font("Cambria", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
			this.lb_successList.Location = new System.Drawing.Point(43, 158);
			this.lb_successList.Name = "lb_successList";
			this.lb_successList.Size = new System.Drawing.Size(150, 23);
			this.lb_successList.TabIndex = 5;
			this.lb_successList.Text = "Lớp Thành Công";
			// 
			// lb_errorList
			// 
			this.lb_errorList.AutoSize = true;
			this.lb_errorList.Font = new System.Drawing.Font("Cambria", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
			this.lb_errorList.Location = new System.Drawing.Point(43, 291);
			this.lb_errorList.Name = "lb_errorList";
			this.lb_errorList.Size = new System.Drawing.Size(150, 23);
			this.lb_errorList.TabIndex = 6;
			this.lb_errorList.Text = "Lớp Thành Công";
			// 
			// pb_close
			// 
			this.pb_close.Cursor = System.Windows.Forms.Cursors.Hand;
			this.pb_close.Image = global::QLSV.Properties.Resources.close;
			this.pb_close.Location = new System.Drawing.Point(538, 37);
			this.pb_close.Name = "pb_close";
			this.pb_close.Size = new System.Drawing.Size(16, 16);
			this.pb_close.SizeMode = System.Windows.Forms.PictureBoxSizeMode.AutoSize;
			this.pb_close.TabIndex = 7;
			this.pb_close.TabStop = false;
			this.pb_close.Click += new System.EventHandler(this.pb_close_Click);
			// 
			// pb_success
			// 
			this.pb_success.Image = global::QLSV.Properties.Resources.checkmark;
			this.pb_success.Location = new System.Drawing.Point(43, 114);
			this.pb_success.Name = "pb_success";
			this.pb_success.Size = new System.Drawing.Size(32, 32);
			this.pb_success.SizeMode = System.Windows.Forms.PictureBoxSizeMode.AutoSize;
			this.pb_success.TabIndex = 1;
			this.pb_success.TabStop = false;
			// 
			// pb_error
			// 
			this.pb_error.Image = global::QLSV.Properties.Resources.error2;
			this.pb_error.Location = new System.Drawing.Point(42, 237);
			this.pb_error.Name = "pb_error";
			this.pb_error.Size = new System.Drawing.Size(32, 32);
			this.pb_error.SizeMode = System.Windows.Forms.PictureBoxSizeMode.AutoSize;
			this.pb_error.TabIndex = 0;
			this.pb_error.TabStop = false;
			// 
			// RegistrationResult
			// 
			this.AutoScaleDimensions = new System.Drawing.SizeF(8F, 16F);
			this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
			this.AutoSize = true;
			this.AutoSizeMode = System.Windows.Forms.AutoSizeMode.GrowAndShrink;
			this.BackColor = System.Drawing.Color.White;
			this.ClientSize = new System.Drawing.Size(800, 450);
			this.ControlBox = false;
			this.Controls.Add(this.pb_close);
			this.Controls.Add(this.lb_errorList);
			this.Controls.Add(this.lb_successList);
			this.Controls.Add(this.lb_errorNum);
			this.Controls.Add(this.lb_successNum);
			this.Controls.Add(this.lb_title);
			this.Controls.Add(this.pb_success);
			this.Controls.Add(this.pb_error);
			this.FormBorderStyle = System.Windows.Forms.FormBorderStyle.FixedSingle;
			this.Name = "RegistrationResult";
			this.Padding = new System.Windows.Forms.Padding(40);
			this.StartPosition = System.Windows.Forms.FormStartPosition.CenterScreen;
			((System.ComponentModel.ISupportInitialize)(this.pb_close)).EndInit();
			((System.ComponentModel.ISupportInitialize)(this.pb_success)).EndInit();
			((System.ComponentModel.ISupportInitialize)(this.pb_error)).EndInit();
			this.ResumeLayout(false);
			this.PerformLayout();

		}

		#endregion

		private System.Windows.Forms.PictureBox pb_error;
		private System.Windows.Forms.PictureBox pb_success;
		private System.Windows.Forms.Label lb_title;
		private System.Windows.Forms.Label lb_successNum;
		private System.Windows.Forms.Label lb_errorNum;
		private System.Windows.Forms.Label lb_successList;
		private System.Windows.Forms.Label lb_errorList;
		private System.Windows.Forms.PictureBox pb_close;
	}
}