using System;
using System.IO;
using System.Windows.Forms;

namespace BeGraph {
	public partial class MainWnd : Form {
		public MainWnd() {
			InitializeComponent();
		}

		private void Form1_Load(object sender, EventArgs e) {
			this.WindowState = FormWindowState.Maximized;
			this.FormBorderStyle = FormBorderStyle.Sizable;
		}

		private void saveToolbarItem_Click(object sender, EventArgs e) {
			//TODO: implement
		}

		private void newToolbarItem_Click(object sender, EventArgs e) {
			//TODO: implement
		}

		private void openToolbarItem_Click(object sender, EventArgs e) {
			//TODO: implement
		}

		private void exitToolbarItem_Click(object sender, EventArgs e) {
			//TODO: implement
		}
	}
}