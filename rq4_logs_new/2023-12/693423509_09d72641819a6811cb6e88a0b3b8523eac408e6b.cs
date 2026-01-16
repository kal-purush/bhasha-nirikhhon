namespace QLSV {
	partial class ChangePassword {
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
			this.label1 = new System.Windows.Forms.Label();
			this.tb_oldPass = new System.Windows.Forms.TextBox();
			this.tb_newPass = new System.Windows.Forms.TextBox();
			this.label2 = new System.Windows.Forms.Label();
			this.tb_confirmPass = new System.Windows.Forms.TextBox();
			this.label3 = new System.Windows.Forms.Label();
			this.btn_changePass = new System.Windows.Forms.Button();
			this.btn_cancel = new System.Windows.Forms.Button();
			this.label4 = new System.Windows.Forms.Label();
			this.label5 = new System.Windows.Forms.Label();
			this.label6 = new System.Windows.Forms.Label();
			this.SuspendLayout();
			// 
			// label1
			// 
			this.label1.AutoSize = true;
			this.label1.Font = new System.Drawing.Font("Cambria", 16.2F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
			this.label1.Location = new System.Drawing.Point(37, 39);
			this.label1.Name = "label1";
			this.label1.Size = new System.Drawing.Size(236, 33);
			this.label1.TabIndex = 0;
			this.label1.Text = "Nhập mật khẩu cũ:";
			// 
			// tb_oldPass
			// 
			this.tb_oldPass.BorderStyle = System.Windows.Forms.BorderStyle.FixedSingle;
			this.tb_oldPass.Font = new System.Drawing.Font("Cambria", 13.8F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
			this.tb_oldPass.Location = new System.Drawing.Point(346, 38);
			this.tb_oldPass.Name = "tb_oldPass";
			this.tb_oldPass.Size = new System.Drawing.Size(203, 34);
			this.tb_oldPass.TabIndex = 1;
			// 
			// tb_newPass
			// 
			this.tb_newPass.BorderStyle = System.Windows.Forms.BorderStyle.FixedSingle;
			this.tb_newPass.Font = new System.Drawing.Font("Cambria", 13.8F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
			this.tb_newPass.Location = new System.Drawing.Point(346, 115);
			this.tb_newPass.Name = "tb_newPass";
			this.tb_newPass.Size = new System.Drawing.Size(203, 34);
			this.tb_newPass.TabIndex = 3;
			// 
			// label2
			// 
			this.label2.AutoSize = true;
			this.label2.Font = new System.Drawing.Font("Cambria", 16.2F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
			this.label2.Location = new System.Drawing.Point(37, 116);
			this.label2.Name = "label2";
			this.label2.Size = new System.Drawing.Size(257, 33);
			this.label2.TabIndex = 2;
			this.label2.Text = "Nhập mật khẩu mới:";
			// 
			// tb_confirmPass
			// 
			this.tb_confirmPass.BorderStyle = System.Windows.Forms.BorderStyle.FixedSingle;
			this.tb_confirmPass.Font = new System.Drawing.Font("Cambria", 13.8F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
			this.tb_confirmPass.Location = new System.Drawing.Point(346, 190);
			this.tb_confirmPass.Name = "tb_confirmPass";
			this.tb_confirmPass.Size = new System.Drawing.Size(203, 34);
			this.tb_confirmPass.TabIndex = 5;
			// 
			// label3
			// 
			this.label3.AutoSize = true;
			this.label3.Font = new System.Drawing.Font("Cambria", 16.2F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
			this.label3.Location = new System.Drawing.Point(37, 191);
			this.label3.Name = "label3";
			this.label3.Size = new System.Drawing.Size(248, 33);
			this.label3.TabIndex = 4;
			this.label3.Text = "Xác nhận mật khẩu:";
			// 
			// btn_changePass
			// 
			this.btn_changePass.Font = new System.Drawing.Font("Cambria", 13.8F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
			this.btn_changePass.Location = new System.Drawing.Point(60, 274);
			this.btn_changePass.Name = "btn_changePass";
			this.btn_changePass.Size = new System.Drawing.Size(213, 50);
			this.btn_changePass.TabIndex = 6;
			this.btn_changePass.Text = "Đổi mật khẩu";
			this.btn_changePass.UseVisualStyleBackColor = true;
			this.btn_changePass.Click += new System.EventHandler(this.btn_changePass_Click);
			// 
			// btn_cancel
			// 
			this.btn_cancel.Font = new System.Drawing.Font("Cambria", 13.8F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
			this.btn_cancel.Location = new System.Drawing.Point(339, 274);
			this.btn_cancel.Name = "btn_cancel";
			this.btn_cancel.Size = new System.Drawing.Size(213, 50);
			this.btn_cancel.TabIndex = 7;
			this.btn_cancel.Text = "Hủy";
			this.btn_cancel.UseVisualStyleBackColor = true;
			this.btn_cancel.Click += new System.EventHandler(this.btn_cancel_Click);
			// 
			// label4
			// 
			this.label4.AutoSize = true;
			this.label4.Font = new System.Drawing.Font("Cambria", 13.8F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
			this.label4.ForeColor = System.Drawing.Color.Red;
			this.label4.Location = new System.Drawing.Point(279, 43);
			this.label4.Name = "label4";
			this.label4.Size = new System.Drawing.Size(22, 27);
			this.label4.TabIndex = 8;
			this.label4.Text = "*";
			// 
			// label5
			// 
			this.label5.AutoSize = true;
			this.label5.Font = new System.Drawing.Font("Cambria", 13.8F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
			this.label5.ForeColor = System.Drawing.Color.Red;
			this.label5.Location = new System.Drawing.Point(300, 120);
			this.label5.Name = "label5";
			this.label5.Size = new System.Drawing.Size(22, 27);
			this.label5.TabIndex = 9;
			this.label5.Text = "*";
			// 
			// label6
			// 
			this.label6.AutoSize = true;
			this.label6.Font = new System.Drawing.Font("Cambria", 13.8F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
			this.label6.ForeColor = System.Drawing.Color.Red;
			this.label6.Location = new System.Drawing.Point(291, 195);
			this.label6.Name = "label6";
			this.label6.Size = new System.Drawing.Size(22, 27);
			this.label6.TabIndex = 10;
			this.label6.Text = "*";
			// 
			// ChangePassword
			// 
			this.AutoScaleDimensions = new System.Drawing.SizeF(8F, 16F);
			this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
			this.ClientSize = new System.Drawing.Size(623, 367);
			this.ControlBox = false;
			this.Controls.Add(this.label6);
			this.Controls.Add(this.label5);
			this.Controls.Add(this.label4);
			this.Controls.Add(this.btn_cancel);
			this.Controls.Add(this.btn_changePass);
			this.Controls.Add(this.tb_confirmPass);
			this.Controls.Add(this.label3);
			this.Controls.Add(this.tb_newPass);
			this.Controls.Add(this.label2);
			this.Controls.Add(this.tb_oldPass);
			this.Controls.Add(this.label1);
			this.Name = "ChangePassword";
			this.StartPosition = System.Windows.Forms.FormStartPosition.CenterScreen;
			this.ResumeLayout(false);
			this.PerformLayout();

		}

		#endregion

		private System.Windows.Forms.Label label1;
		private System.Windows.Forms.TextBox tb_oldPass;
		private System.Windows.Forms.TextBox tb_newPass;
		private System.Windows.Forms.Label label2;
		private System.Windows.Forms.TextBox tb_confirmPass;
		private System.Windows.Forms.Label label3;
		private System.Windows.Forms.Button btn_changePass;
		private System.Windows.Forms.Button btn_cancel;
		private System.Windows.Forms.Label label4;
		private System.Windows.Forms.Label label5;
		private System.Windows.Forms.Label label6;
	}
}