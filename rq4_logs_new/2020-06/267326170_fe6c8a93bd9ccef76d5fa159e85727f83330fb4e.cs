using MySql.Data.MySqlClient;
using ProtoMine.Controle;
using ProtoMine.Modelo;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ProtoMine.DAO
{
    class VendaDAO : Conexao
    {
        MySqlCommand comando = null;
        UtilidadesTelas util = new UtilidadesTelas();

        public List<Venda> BuscarVendas()
        {
            try
            {
                abrirConexao();

                comando = new MySqlCommand("select * from venda ", conexao);

                MySqlDataReader reader = comando.ExecuteReader();

                List<Venda> listaDeVendas = new List<Venda>();

                while (reader.Read())
                {
                    Venda venda = new Venda();
                    venda.Id = reader.GetInt32(0);
                    venda.Valor = reader.GetDouble(1);
                    venda.Comprador.Id = reader.GetInt32(2);
                    venda.Vendedor.Id = reader.GetInt32(3);
                    venda.Item.Id = reader.GetInt32(4);
                    venda.ValorUnitario = reader.GetDouble(5);
                    venda.Pedido.Id = reader.GetInt32(6);
                    listaDeVendas.Add(venda);
                }
                return listaDeVendas;
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
            finally
            {
                fecharConexao(); // Fecha a conexão
            }
        }
    }
}