using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.UI.WebControls;

namespace App.UI.WebForm.Common
{
    public class Helpers
    {
        public static void ConfigurarCombo(DropDownList combo,
                string textfield, string valuefield, object data
                )
        {
            combo.DataTextField = textfield; //Texto
            combo.DataValueField = valuefield; //Código
            combo.DataSource = data;
            combo.DataBind();
        }
    }
}