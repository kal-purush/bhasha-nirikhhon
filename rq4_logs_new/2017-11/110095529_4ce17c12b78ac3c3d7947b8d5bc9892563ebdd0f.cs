using PTClient.Logic.LogicController;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace PTClient.GUI
{
    public partial class LoginScreen : Form
    {
        IController control = new Controller();
        public LoginScreen()
        {
            InitializeComponent();
        }

        private void button1_Click(object sender, EventArgs e)
        {
            String username = textBoxUsername.Text;
            String password = textBoxPassword.Text;
            control.Login(username, password);
        }

        private void Login_Load(object sender, EventArgs e)
        {

        }
    }
}