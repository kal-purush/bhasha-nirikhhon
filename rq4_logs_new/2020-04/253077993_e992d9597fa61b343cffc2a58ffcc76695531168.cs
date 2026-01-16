using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using System.Data;
using System.Data.SqlClient;



using ApiExcelReader.Conexion;
using ApiExcelReader.Modelo;

namespace ApiExcelReader.Controllers
{
    [ApiController]
    [Route("[controller]")]

    public class LoginController : Controller
    {
        ConexionExcelFiltros conexionExcelFiltros = new ConexionExcelFiltros();
        EntidadLogin entidadLogin = new EntidadLogin();
        string strdescripcionError = string.Empty;

        [HttpGet("{strUsuario}&{strPassword}", Name = "ObtenerCliente")]
        public ActionResult<EntidadLogin> ObtenerCliente(string strUsuario, string strPassword)
        {

            try
            {
                DataTable RespuestaConsultaCliente = new DataTable();

                RespuestaConsultaCliente = conexionExcelFiltros.Consulta(string.Format("select * from [Login$] where Usuario = '{0}'", strUsuario), 2);

                if (RespuestaConsultaCliente.Rows.Count != 0)
                {
                    entidadLogin.Id = RespuestaConsultaCliente.Rows[0]["Id"].ToString();
                    entidadLogin.Usuario = RespuestaConsultaCliente.Rows[0]["Usuario"].ToString();
                    entidadLogin.Password = RespuestaConsultaCliente.Rows[0]["Password"].ToString();
                    entidadLogin.Perfil = RespuestaConsultaCliente.Rows[0]["Perfil"].ToString();
                    
                }                

                else
                {
                    entidadLogin.Error = "2";
                    entidadLogin.DescripcionError = "no se encontro el Usuario.";
                }

                if (entidadLogin.Usuario.Equals(strPassword) && entidadLogin.Password.Equals(strPassword))
                {
                    entidadLogin.ValidarUsuario = true;
                    entidadLogin.Error = "0";
                    entidadLogin.DescripcionError = "";
                }

                return entidadLogin;
            }

            catch (Exception ex)
            {

                entidadLogin.Error = "1";
                strdescripcionError = ex.Message.ToString();
                entidadLogin.DescripcionError = strdescripcionError;
                return entidadLogin;
            }

        }
    }
}