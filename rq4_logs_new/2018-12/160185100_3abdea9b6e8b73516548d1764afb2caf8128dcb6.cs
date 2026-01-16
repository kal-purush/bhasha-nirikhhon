using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace Extensions
{
    public static class Extensions
    {
        public static void Refresh(this ComboBox cb)
        {
            if(cb.DataSource != null)
            {
                var db_ = cb.DataSource;

                cb.DataSource = null;
                cb.DataSource = db_;
            }
        }
    }
}