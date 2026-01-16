using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using FrameWork.Entidades;

namespace FrameWork.Dados
{
    public class DAL_Clientes : DAL_Data
    {
        //Seleciona Clientes
        public List<Clientes> SelecionarClientes(Int32 pIdCliente)
        {
            List<Clientes> vol_ListaClientes = new List<Clientes>();
            try
            {
                //Limpa os parâmetros
                Command.Parameters.Clear();
                SqlCommand.Append("SP_LOJA_S_CLIENTES");
                Command.Parameters.AddWithValue("@PIDCLIENTE", pIdCliente);
                using (SqlDataReader vol_DataReader = GetDataReaderProc())
                {
                    while (vol_DataReader.Read())
                    {
                        Clientes vol_Clientes = new Clientes
                        {
                            IdCliente = Convert.ToInt32(vol_DataReader["IDCLIENTE"]),
                            Descricao = Convert.ToString(vol_DataReader["DESCRICAO"])
                        };
                        //Adiciona item a lista
                        vol_ListaClientes.Add(vol_Clientes);
                    }
                    return vol_ListaClientes;
                }
            }
            catch
            {
                return null;
            }
        }

        //Gravar 
        public bool Gravar(Int32 pIdCliente, string pDescricao)
        {
            try
            {
                //Limpa os parâmetros
                Command.Parameters.Clear();
                SqlCommand.Append("SP_LOJA_I_CLIENTES");
                Command.Parameters.AddWithValue("@PIDCLIENTE", pIdCliente);
                Command.Parameters.AddWithValue("@PDESCRICAO", pDescricao);
                return ExecuteProc();
            }
            catch
            {
                return false;
            }
        }

        //Altera 
        public bool Alterar(Int32 pIdCliente, string pDescricao)
        {
            try
            {
                //Limpa os parâmetros
                Command.Parameters.Clear();
                SqlCommand.Append("SP_LOJA_U_CLIENTES");
                Command.Parameters.AddWithValue("@PIDCLIENTE", pIdCliente);
                Command.Parameters.AddWithValue("@PDESCRICAO", pDescricao);
                return ExecuteProc();
            }
            catch
            {
                return false;
            }
        }

        //Excluir
        public bool Excluir(Int32 pIdCliente)
        {
            try
            {
                //Exclui os parâmetros
                Command.Parameters.Clear();
                SqlCommand.Append("SP_LOJA_D_CLIENTES");
                Command.Parameters.AddWithValue("@PIDCLIENTE", pIdCliente);
                return ExecuteProc();
            }
            catch
            {
                return false;
            }
        }

        public Int32 NextId()
        {
            //Variavel de controle
            Int32 vil_NextId = 1;

            try
            {
                //Limpa os parâmetros
                Command.Parameters.Clear();
                SqlCommand.Append("SP_LOJA_N_CLIENTES");
                using (SqlDataReader vol_DataReader = GetDataReaderProc())
                {
                    while (vol_DataReader.Read())
                    {
                        vil_NextId = Convert.ToInt32(vol_DataReader[0]) + 1;
                    }

                }
                return vil_NextId;
            }
            catch
            {
                return vil_NextId;
            }
        }
    }
}
