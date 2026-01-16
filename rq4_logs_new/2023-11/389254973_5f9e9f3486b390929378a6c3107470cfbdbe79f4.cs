using MySql.Data.MySqlClient;
using Org.BouncyCastle.Asn1.X509;
using System.Windows.Forms;

namespace papeSraErika.Ventas
{
    class addDataTable
    {
        public void Table(string data)
        {
            papeShopView pape = new papeShopView();
            int bandera = 0;
            string sql = "select * from productos where CODIGO_BARRAS = '" + data + "'";
            string _list = null;
            string cantidadText = "1";

            MySqlDataReader res = systemQuerys.dataTable(sql);
            while (res.Read())
            {
                if ((res.GetString(5).Equals("0")))
                {
                    MessageBox.Show("Este producto ya no tiene stock");
                    bandera = 1;

                }
                else
                {
                    _list += res.GetString(4) + " " + res.GetString(1) + " $ " + res.GetString(6);
                    pape.setTotalLabel(res.GetString(6));
                    pape.addListBox(_list);
                    bandera = 2;
                    cantidadText = "1";
                }

            }
            if (bandera == 0)
            {
                MessageBox.Show("Producto no encontrado");
            }
        }
    }
}