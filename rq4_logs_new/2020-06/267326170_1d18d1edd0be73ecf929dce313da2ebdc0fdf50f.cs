using MySql.Data.MySqlClient;
using ProtoMine.Cache;
using ProtoMine.DAO;
using ProtoMine.Modelo;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ProtoMine.Controle
{
    class ItemController
    {
        public void CarregarItens()
        {
            UtilidadesTelas util = new UtilidadesTelas();
            ItemBasicoDAO itemDAO = new ItemBasicoDAO();
            try
            {
                itemDAO.carregarItensSimples(UserCache.UsuarioLogado.Id);
            }
            catch (MySqlException exce)
            {
                util.MensagemDeTeste("Erro no load dos itens, falha na conexão ao banco de dados", "Erro!");
                throw exce;
            }
            catch (Exception ex)
            {
                util.MensagemDeTeste("Erro não esperado no load dos itens:  " + ex.Message, "Erro!");
                throw ex;
            }
        }
    }
}