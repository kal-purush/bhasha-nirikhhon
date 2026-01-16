using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace WDFBanKinhMat.Classes.CBBox
{
    internal class CBBox
    {
        public void FillComboBox(ComboBox comboName, DataTable dataTable, string displayMember, string Value)
        {
            comboName.DataSource = dataTable;
            comboName.DisplayMember = displayMember;
            comboName.ValueMember = Value;

        }
    }
}