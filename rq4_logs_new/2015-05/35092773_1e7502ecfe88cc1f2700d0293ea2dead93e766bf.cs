using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace legendary_crafter
{
    internal class input
    {
        public int i_value
        {
            get;
            set;
        }

        public TextBox textBox
        {
            get;
            set;
        }

        public input setInput(TextBox textBox)
        {
            Int32.TryParse(textBox.Text, out i_value);
            this.i_value = i_value;
        }
    }
}