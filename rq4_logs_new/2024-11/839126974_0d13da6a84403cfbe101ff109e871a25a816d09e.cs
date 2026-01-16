using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;

namespace GUI.Controls
{
    public partial class ValidarContraseña : System.Web.UI.UserControl
    {
        protected void Page_Load(object sender, EventArgs e)
        {
            cvPassword.ErrorMessage = "";
        }

        protected void ValidatePassword(object source, ServerValidateEventArgs args)
        {
            string password = args.Value;

            var regex = new Regex(@"^(?=.*[A-Z])(?=.*\d)(?=.*[@$!%*?&])[A-Za-z\d@$!%*?&]{8,}$");
            
            if (regex.IsMatch(password))
            {
                args.IsValid = true;
                cvPassword.ErrorMessage = "";
            }
            else
            {
                args.IsValid = false;
                cvPassword.ErrorMessage = "La contraseña debe tener al menos 8 caracteres, una mayúscula, un número y un carácter especial.";
            }
        }

        public string Password
        {
            get { return txtPassword.Text; }
            set { txtPassword.Text = value; }
        }
    }
}
﻿//------------------------------------------------------------------------------
// <generado automáticamente>
//     Este código fue generado por una herramienta.
//
//     Los cambios en este archivo podrían causar un comportamiento incorrecto y se perderán si
//     se vuelve a generar el código. 
// </generado automáticamente>
//------------------------------------------------------------------------------

namespace GUI.Controls
{


    public partial class ValidarContraseña
    {

        /// <summary>
        /// Control lblPassword.
        /// </summary>
        /// <remarks>
        /// Campo generado automáticamente.
        /// Para modificarlo, mueva la declaración del campo del archivo del diseñador al archivo de código subyacente.
        /// </remarks>
        protected global::System.Web.UI.WebControls.Label lblPassword;

        /// <summary>
        /// Control txtPassword.
        /// </summary>
        /// <remarks>
        /// Campo generado automáticamente.
        /// Para modificarlo, mueva la declaración del campo del archivo del diseñador al archivo de código subyacente.
        /// </remarks>
        protected global::System.Web.UI.WebControls.TextBox txtPassword;

        /// <summary>
        /// Control cvPassword.
        /// </summary>
        /// <remarks>
        /// Campo generado automáticamente.
        /// Para modificarlo, mueva la declaración del campo del archivo del diseñador al archivo de código subyacente.
        /// </remarks>
        protected global::System.Web.UI.WebControls.CustomValidator cvPassword;
    }
}