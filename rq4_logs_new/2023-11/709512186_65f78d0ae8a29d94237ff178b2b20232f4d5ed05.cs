using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using TCC_euquero.Modelo;
using MySql.Data.MySqlClient;

namespace TCC_euquero.Logica
{
    public class GerenciarCategorias : Banco
    {
        public Categoria BuscarCategoria(int pCdCategoria)
        {
            List<Parametro> listaParametro = new List<Parametro>();

            listaParametro.Add(new Parametro("pCdCategoria", pCdCategoria.ToString()));

            Conectar();

            MySqlDataReader dados = ConsultarProcedure("BuscarCategoria", listaParametro);
            Categoria categoria = new Categoria();
            if(dados.Read())
                categoria = new Categoria(dados.GetInt32(0), dados.GetString(1));

            Desconectar();

            return categoria;
        }
    }
}