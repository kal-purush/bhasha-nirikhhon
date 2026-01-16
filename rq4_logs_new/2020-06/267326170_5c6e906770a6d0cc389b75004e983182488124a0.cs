using ProtoMine.DAO;
using ProtoMine.Modelo;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ProtoMine.Controle
{
    class VendaController
    {
        VendaDAO vendaDAO = new VendaDAO();

        public void AdicionarVenda(Venda venda)
        {
            vendaDAO.AdicionarVenda(venda);
        }
    }
}