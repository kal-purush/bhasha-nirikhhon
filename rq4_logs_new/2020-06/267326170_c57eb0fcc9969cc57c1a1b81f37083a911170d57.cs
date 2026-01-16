using Microsoft.Win32;
using MySql.Data.MySqlClient;
using ProtoMine.Cache;
using ProtoMine.Controle;
using ProtoMine.Modelo;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ProtoMine.DAO
{
    class ItemEspecialDAO : Conexao
    {
        MySqlCommand comando = null;
        UtilidadesTelas util = new UtilidadesTelas();
        public void carregarItensEspeciais()
        {
            try
            {
                abrirConexao();

                comando = new MySqlCommand("select item.* from item "
                    + "join usuarioitem on item.id = usuarioitem.id_item "
                    + "join usuario on usuario.id = usuarioitem.id_usuario "
                    + "where usuario.id = @ID and item.tipo != @TIPO", conexao);

                comando.Parameters.AddWithValue("@ID", UserCache.UsuarioLogado.Id);
                comando.Parameters.AddWithValue("@TIPO", "comum");

                MySqlDataReader reader = comando.ExecuteReader();

                while (reader.Read())
                {
                    ItemEspecial itemTemp = new ItemEspecial();
                    itemTemp.Id = reader.GetInt32(0);
                    itemTemp.Nome = reader.GetString(1);
                    itemTemp.Peso = reader.GetDouble(2);
                    itemTemp.UrlImg = reader.GetString(3);
                    itemTemp.Tipo = reader.GetString(4);
                    if (itemTemp.Tipo == "pic")
                        UserCache.Picareta = itemTemp;
                    else
                        UserCache.Mochila = itemTemp;
                }
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