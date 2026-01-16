using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace PTClient
{
    public partial class Login1 : Form
    {
        public Login1()
        {
            InitializeComponent();
        }

        private void button1_Click(object sender, EventArgs e)
        {
           //if (Controller.Instance.Login(textBox1.Text, textBox2.Text).Captain == true)
           // {
           //     Map.Overview cap = new Map.Overview();
           //     cap.ShowDialog();
           // }
           //else if (Controller.Instance.Login(textBox1.Text, textBox2.Text).Captain == false)
           // {
           //     Worker wor = new Worker();
           //     wor.ShowDialog();
           // }
        }

        private void Login_Load(object sender, EventArgs e)
        {

        }
    }
}