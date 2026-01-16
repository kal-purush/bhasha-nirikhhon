using System;
using System.Windows.Forms;
using ApplicationLogger;

namespace ApplicationLogAnalyzer
{
    public partial class Form1 : Form
    {
        public Form1()
        {
            InitializeComponent();


            openLogFileDialog.FileOk += openLogFileDialog_FileOk;
        }

        private void button1_Click(object sender, EventArgs e)
        {
            openLogFileDialog.ShowDialog();

        }

        void openLogFileDialog_FileOk(object sender, System.ComponentModel.CancelEventArgs e)
        {
            Logger.Initialize(openLogFileDialog.SafeFileName);
        }

        private void openLogFileDialog_FileOk_1(object sender, System.ComponentModel.CancelEventArgs e)
        {

        }
    }
}