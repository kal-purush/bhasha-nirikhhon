using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace ImageProcessing
{
    public partial class Form1 : Form
    {
        OpenFileDialog oDlg;
        private Bitmap currentBitmap;

        public Form1()
        {
            InitializeComponent();
            oDlg = new OpenFileDialog(); // Open Dialog Initialization
            oDlg.RestoreDirectory = true;
            oDlg.InitialDirectory = "C:\\";
            oDlg.FilterIndex = 1;
            oDlg.Filter = "jpg Files (*.jpg)|*.jpg|gif Files (*.gif)|*.gif|png Files (*.png)|*.png |bmp Files (*.bmp)|*.bmp";

        }

        private void openToolStripMenuItem_Click(object sender, EventArgs e)
        {
            if (DialogResult.OK == oDlg.ShowDialog())
            {
                currentBitmap = (Bitmap)Bitmap.FromFile(oDlg.FileName);
                string BitmapPath = oDlg.FileName;
                this.AutoScroll = true;
                this.AutoScrollMinSize = new Size(Convert.ToInt32(currentBitmap.Width), Convert.ToInt32(currentBitmap.Height));
                this.pictureBox1.Width = currentBitmap.Width;
                this.pictureBox1.Height = currentBitmap.Height;
                this.Width = currentBitmap.Width+18;
                this.Height = currentBitmap.Height + this.menuStrip1.Height+48;
                this.Invalidate();
                currentBitmap = ImageHandler.ProcessImage(currentBitmap);
                this.pictureBox1.Image = currentBitmap;
            }
        }
    }
}