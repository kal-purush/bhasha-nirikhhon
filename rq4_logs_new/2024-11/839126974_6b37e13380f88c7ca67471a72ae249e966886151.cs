using System;
using System.Text.RegularExpressions;
using System.Web.UI;

namespace GUI.Controls
{
    public partial class ValidarEmail : UserControl
    {
        protected void Page_Load(object sender, EventArgs e)
        {
            
        }
        public string Email
        {
            get { return txtEmail.Text; }
            set { txtEmail.Text = value; }
        }

        public bool EsEmailValido()
        {
            string pattern = @"^[^@\s]+@[^@\s]+\.[^@\s]+$";
            return Regex.IsMatch(Email, pattern);
        }
    }
}