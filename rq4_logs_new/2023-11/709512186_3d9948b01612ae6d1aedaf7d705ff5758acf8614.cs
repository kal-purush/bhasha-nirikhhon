using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;
using TCC_euquero.Modelo;

namespace TCC_euquero
{
    public partial class criaçaoAnuncio : System.Web.UI.Page
    {
        protected void Page_Load(object sender, EventArgs e)
        {

        }

        protected void btnCriarAnuncio_Click(object sender, EventArgs e)
        {
            Anuncio anuncio = new Anuncio();

            anuncio.CriarAnuncio(txtEmail.Value, txtDataEncerramento.Value, txtNomeProduto.Value, descProduto.Value, txtValorMinimo.Value, txtValorMaximo.Value);

            Response.Redirect("perfil.aspx");
        }
    }
}