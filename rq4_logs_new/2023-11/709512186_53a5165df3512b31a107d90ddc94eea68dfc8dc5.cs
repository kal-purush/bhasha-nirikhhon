using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;
using TCC_euquero.Logica;

namespace TCC_euquero
{
    public partial class validarEmail : System.Web.UI.Page
    {
        private string email = "";
        protected void Page_Load(object sender, EventArgs e)
        {
            if (String.IsNullOrEmpty(Request["email"].ToString()))
                Response.Redirect("index.aspx");

            email = Request["email"].ToString();

            GerenciarCadastroUsuario gerenciarCadastroUsuario = new GerenciarCadastroUsuario();

            if (gerenciarCadastroUsuario.VerificarValidacao(email))
                Response.Redirect("index.aspx");

            litEmailConfirmacao.Text = $"{email}.\n";

            gerenciarCadastroUsuario.EnviarCodigoEmail(email);
        }

        protected void btnValidar_Click(object sender, EventArgs e)
        {
            int codigoDigitado = 0;
            try
            {
                codigoDigitado = int.Parse(Codigo.Value);
            }
            catch
            {
                litRespostaSistema.Text = "Código inválido.";
                return;
            }

            GerenciarCadastroUsuario gerenciarCadastroUsuario = new GerenciarCadastroUsuario();
            if (gerenciarCadastroUsuario.ValidarConta(email, codigoDigitado))
            {
                Session["email"] = email;
                Response.Redirect("index.aspx");
            }
            else
            {
                litRespostaSistema.Text = "Código inválido.";
            }
        }
    }
}