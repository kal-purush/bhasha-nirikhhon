using System.Collections.Generic;
using System.Data;
using System.Web.Services;
using System.Web.UI.WebControls;
using System.Windows.Forms;

namespace GUI.WebService
{
    /// <summary>
    /// Descripción breve de CalcularCarrito
    /// </summary>
    [WebService(Namespace = "http://tempuri.org/")]
    [WebServiceBinding(ConformsTo = WsiProfiles.BasicProfile1_1)]
    [System.ComponentModel.ToolboxItem(false)]
    public class CalcularCarrito : System.Web.Services.WebService
    {
        [WebMethod]
        public decimal CalcularTotalCarrito(System.Web.UI.WebControls.Label lblSubtotal)
        {
            decimal totalCarrito = 0;
            if (lblSubtotal != null)
            {
                totalCarrito += decimal.Parse(lblSubtotal.Text, System.Globalization.NumberStyles.Currency);
            }
            return totalCarrito;
        }

        [WebMethod]
        public decimal CalcularTotal(List<Models.Carrito> listadoItems)
        {
            decimal totalCarrito = 0;
            foreach (var item in listadoItems)
            {
                totalCarrito += item.Subtotal;
            }

            return totalCarrito;
        }
    }
}