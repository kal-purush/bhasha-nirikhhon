namespace TextyClient {
	partial class MainForm {
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
			this.components = new System.ComponentModel.Container();
			this.label1 = new System.Windows.Forms.Label();
			this.advancedConsole = new TextyClient.AdvancedConsole();
			this.connectionTimer = new System.Windows.Forms.Timer(this.components);
			this.SuspendLayout();
			// 
			// label1
			// 
			this.label1.AutoSize = true;
			this.label1.Dock = System.Windows.Forms.DockStyle.Top;
			this.label1.Font = new System.Drawing.Font("宋体", 9F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(134)));
			this.label1.Location = new System.Drawing.Point(0, 0);
			this.label1.Name = "label1";
			this.label1.Size = new System.Drawing.Size(197, 12);
			this.label1.TabIndex = 0;
			this.label1.Text = "不要关闭这个窗口，否则会结束程序";
			// 
			// advancedConsole
			// 
			this.advancedConsole.Dock = System.Windows.Forms.DockStyle.Fill;
			this.advancedConsole.Location = new System.Drawing.Point(0, 12);
			this.advancedConsole.Name = "advancedConsole";
			this.advancedConsole.Size = new System.Drawing.Size(800, 547);
			this.advancedConsole.TabIndex = 1;
			// 
			// connectionTimer
			// 
			this.connectionTimer.Tick += new System.EventHandler(this.connectionTimer_Tick);
			// 
			// MainForm
			// 
			this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 12F);
			this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
			this.ClientSize = new System.Drawing.Size(800, 559);
			this.Controls.Add(this.advancedConsole);
			this.Controls.Add(this.label1);
			this.Name = "MainForm";
			this.Text = "信息";
			this.Load += new System.EventHandler(this.MainForm_Load);
			this.ResumeLayout(false);
			this.PerformLayout();

		}

		#endregion

		private System.Windows.Forms.Label label1;
		private AdvancedConsole advancedConsole;
		private System.Windows.Forms.Timer connectionTimer;
	}
}